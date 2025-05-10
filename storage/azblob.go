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

// Download retrieves a blob from Azure Blob Storage and returns a reader for its decompressed content.
// It tries common compression extensions (.gz, .zstd) if the base filename doesn't exist.
func (a *AzBlobStorage) Download(filename string) (io.ReadCloser, error) {
	ctx := context.Background()
	extensionsToTry := []string{".gz", ".zstd", ""} // Try compressed first, then raw

	a.debugf("Attempting to download blob: %s (will try extensions: %v)", filename, extensionsToTry)

	var lastErr error
	for _, ext := range extensionsToTry {
		blobName := filename + ext
		blobURL := a.containerURL.NewBlockBlobURL(blobName)

		a.debugf("Trying to download blob: %s", blobName)

		// Attempt to download the blob properties first to check existence with less overhead
		// _, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
		// if err == nil { // Blob exists

		// Or directly attempt download
		response, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})

		if err == nil {
			// Success! Return the response body wrapped in our decompressor
			// The response body needs to be closed by the caller.
			a.debugf("Successfully downloaded blob: %s", blobName)
			bodyStream := response.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3}) // Use retry reader
			decompressedStream := decompressStream(bodyStream, blobName)                // Handles decompression based on blobName extension
			return decompressedStream, nil
		}

		// Handle error
		a.debugf("Failed to download blob %s: %v", blobName, err)
		lastErr = fmt.Errorf("failed attempt to download %s from azure container %s: %w", blobName, a.containerURL.String(), err)

		// Check if the error is a 404 Not Found
		if stgErr, ok := err.(azblob.StorageError); ok {
			if stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
				// Blob not found, continue to try the next extension
				a.debugf("Blob %s not found (404), trying next extension", blobName)
				continue
			}
		}
		// If it's not a BlobNotFound error, return it immediately
		return nil, lastErr
	}

	// If we tried all extensions and none worked, return the last error encountered
	a.debugf("All download attempts failed for %s with extensions %v", filename, extensionsToTry)
	return nil, fmt.Errorf("file %s not found in azure container %s with extensions %v: %w", filename, a.containerURL.String(), extensionsToTry, lastErr)
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
