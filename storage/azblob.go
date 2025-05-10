package storage

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"io"
	"log"
	"net/url"
)

type AzBlobStorage struct {
	containerURL azblob.ContainerURL
	accountName  string // Store for potential use/logging
	debug        bool   // Debug flag
}

// debugf logs debug messages if debug is enabled
func (a *AzBlobStorage) debugf(format string, args ...interface{}) {
	if a.debug {
		log.Printf("[azblob:debug] "+format, args...)
	}
}

// NewAzBlobStorage creates a new Azure Blob Storage client.
func NewAzBlobStorage(accountName, accountKey, containerName, endpoint string, debug bool) (*AzBlobStorage, error) {
	if accountName == "" || accountKey == "" || containerName == "" {
		return nil, fmt.Errorf("azure storage account name, key, and container name cannot be empty")
	}
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create azure shared key credential: %w", err)
	}

	// Use default pipeline options
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// Construct the container URL
	// For Azurite (local testing), use the custom endpoint if provided
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	if endpoint != "" {
		serviceURL = endpoint
	}

	u, err := url.Parse(fmt.Sprintf("%s/%s", serviceURL, containerName))
	if err != nil {
		return nil, fmt.Errorf("failed to parse azure container URL: %w", err)
	}
	containerURL := azblob.NewContainerURL(*u, p)

	// Verify container exists, create if not exists
	ctx := context.Background()
	_, err = containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		// Check if error is container not found
		if stgErr, ok := err.(azblob.StorageError); ok && stgErr.ServiceCode() == azblob.ServiceCodeContainerNotFound {
			// Container doesn't exist, create it
			if debug {
				log.Printf("[azblob:debug] Container %s not found, creating it", containerName)
			}
			_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
			if err != nil {
				return nil, fmt.Errorf("failed to create container %s: %w", containerName, err)
			}
			if debug {
				log.Printf("[azblob:debug] Container %s created successfully", containerName)
			}
		} else {
			return nil, fmt.Errorf("failed to access container %s: %w", containerName, err)
		}
	}

	storage := &AzBlobStorage{
		containerURL: containerURL,
		accountName:  accountName,
		debug:        debug,
	}

	if debug {
		log.Printf("[azblob:debug] Successfully initialized Azure Blob Storage client for account %s, container %s", accountName, containerName)
	}

	return storage, nil
}

// Upload compresses and uploads data to the specified blob name (filename + compression extension).
func (a *AzBlobStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	ctx := context.Background()
	compressedReader, ext := compressStream(reader, format, level)
	blobName := filename + ext
	blobURL := a.containerURL.NewBlockBlobURL(blobName)

	a.debugf("Uploading blob: %s (compression: %s level %d)", blobName, format, level)

	// Use UploadStreamToBlockBlob for efficient streaming upload
	_, err := azblob.UploadStreamToBlockBlob(ctx, compressedReader, blobURL, azblob.UploadStreamToBlockBlobOptions{
		// Can configure parallelism, buffer size, metadata, tags etc. here if needed
	})
	if err != nil {
		a.debugf("Failed to upload blob %s: %v", blobName, err)
		return fmt.Errorf("failed to upload %s to azure container %s: %w", blobName, a.containerURL.String(), err)
	}
	a.debugf("Successfully uploaded blob: %s", blobName)
	return nil
}

// UploadWithExtension uploads pre-compressed data to Azure Blob Storage with the appropriate extension
func (a *AzBlobStorage) UploadWithExtension(filename string, reader io.Reader, contentEncoding string) error {
	ctx := context.Background()
	
	var ext string
	switch strings.ToLower(contentEncoding) {
	case "gzip":
		ext = ".gz"
	case "zstd":
		ext = ".zstd"
	}
	
	blobName := filename + ext
	blobURL := a.containerURL.NewBlockBlobURL(blobName)

	a.debugf("Uploading pre-compressed blob: %s (encoding: %s)", blobName, contentEncoding)

	// Create HTTP headers for the blob
	headers := azblob.BlobHTTPHeaders{}
	if contentEncoding != "" {
		headers.ContentEncoding = contentEncoding
	}

	// Use UploadStreamToBlockBlob for efficient streaming upload
	_, err := azblob.UploadStreamToBlockBlob(ctx, reader, blobURL, azblob.UploadStreamToBlockBlobOptions{
		BlobHTTPHeaders: headers,
	})
	if err != nil {
		a.debugf("Failed to upload pre-compressed blob %s: %v", blobName, err)
		return fmt.Errorf("failed to upload pre-compressed %s to azure container %s: %w", blobName, a.containerURL.String(), err)
	}
	a.debugf("Successfully uploaded pre-compressed blob: %s", blobName)
	return nil
}

// Download retrieves a blob from Azure Blob Storage and returns a reader for its decompressed content.
func (a *AzBlobStorage) Download(filename string) (io.ReadCloser, error) {
	ctx := context.Background()
	blobURL := a.containerURL.NewBlockBlobURL(filename)

	a.debugf("Attempting to download blob: %s", filename)

	// Directly attempt download
	response, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		a.debugf("Failed to download blob %s: %v", filename, err)
		return nil, fmt.Errorf("failed to download %s from azure container %s: %w", filename, a.containerURL.String(), err)
	}

	// Success! Return the response body wrapped in our decompressor
	// The response body needs to be closed by the caller.
	a.debugf("Successfully downloaded blob: %s", filename)
	bodyStream := response.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3}) // Use retry reader
	return decompressStream(bodyStream, filename), nil
}

// List returns a list of blob names in the Azure container matching the prefix.
func (a *AzBlobStorage) List(prefix string, recursive bool) ([]string, error) {
	ctx := context.Background()
	var blobNames []string

	a.debugf("Listing blobs with prefix: %s (recursive: %v)", prefix, recursive)

	marker := azblob.Marker{}
	delimiter := "/"
	if recursive {
		delimiter = "" // Empty delimiter for recursive listing
	}

	for marker.NotDone() {
		listBlob, err := a.containerURL.ListBlobsHierarchySegment(ctx, marker, delimiter, azblob.ListBlobsSegmentOptions{
			Prefix: prefix,
		})
		if err != nil {
			a.debugf("Failed to list blobs with prefix %s: %v", prefix, err)
			return nil, fmt.Errorf("failed to list blobs in azure container %s with prefix %s: %w", a.containerURL.String(), prefix, err)
		}

		// Add blobs
		for _, blobInfo := range listBlob.Segment.BlobItems {
			blobNames = append(blobNames, blobInfo.Name)
		}

		// For non-recursive, add prefixes (subdirectories)
		if !recursive {
			for _, prefix := range listBlob.Segment.BlobPrefixes {
				blobNames = append(blobNames, prefix.Name)
			}
		}

		marker = listBlob.NextMarker
	}

	a.debugf("Found %d blobs matching prefix: %s", len(blobNames), prefix)
	return blobNames, nil
}

// Close closes the Azure Blob Storage connection.
func (a *AzBlobStorage) Close() error {
	// Azure SDK doesn't require explicit closing of connections
	a.debugf("Closing Azure Blob Storage connection (no-op)")
	return nil
}
