package storage

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"io"
	"net/url"
)

type AzBlobStorage struct {
	containerURL azblob.ContainerURL
	accountName  string // Store for potential use/logging
}

// NewAzBlobStorage creates a new Azure Blob Storage client.
func NewAzBlobStorage(accountName, accountKey, containerName, endpoint string) (*AzBlobStorage, error) {
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

	// Verify container exists
	_, err = containerURL.GetProperties(context.Background(), azblob.LeaseAccessConditions{})
	if err != nil {
		return nil, fmt.Errorf("failed to access container %s: %w", containerName, err)
	}

	// Optional: Verify container exists? Could add a check here.

	return &AzBlobStorage{
		containerURL: containerURL,
		accountName:  accountName,
	}, nil
}

// Upload compresses and uploads data to the specified blob name (filename + compression extension).
func (a *AzBlobStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	ctx := context.Background()
	compressedReader, ext := compressStream(reader, format, level)
	blobName := filename + ext
	blobURL := a.containerURL.NewBlockBlobURL(blobName)

	// Use UploadStreamToBlockBlob for efficient streaming upload
	_, err := azblob.UploadStreamToBlockBlob(ctx, compressedReader, blobURL, azblob.UploadStreamToBlockBlobOptions{
		// Can configure parallelism, buffer size, metadata, tags etc. here if needed
	})
	if err != nil {
		return fmt.Errorf("failed to upload %s to azure container %s: %w", blobName, a.containerURL.String(), err)
	}
	return nil
}

// Download retrieves a blob from Azure Blob Storage and returns a reader for its decompressed content.
// It tries common compression extensions (.gz, .zstd) if the base filename doesn't exist.
func (a *AzBlobStorage) Download(filename string) (io.ReadCloser, error) {
	ctx := context.Background()
	extensionsToTry := []string{".gz", ".zstd", ""} // Try compressed first, then raw

	var lastErr error
	for _, ext := range extensionsToTry {
		blobName := filename + ext
		blobURL := a.containerURL.NewBlockBlobURL(blobName)

		// Attempt to download the blob properties first to check existence with less overhead
		// _, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
		// if err == nil { // Blob exists

		// Or directly attempt download
		response, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})

		if err == nil {
			// Success! Return the response body wrapped in our decompressor
			// The response body needs to be closed by the caller.
			bodyStream := response.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3}) // Use retry reader
			decompressedStream := decompressStream(bodyStream, blobName)                // Handles decompression based on blobName extension
			return decompressedStream, nil
		}

		// Handle error
		lastErr = fmt.Errorf("failed attempt to download %s from azure container %s: %w", blobName, a.containerURL.String(), err)

		// Check if the error is a 404 Not Found
		if stgErr, ok := err.(azblob.StorageError); ok {
			if stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
				// Blob not found, continue to try the next extension
				continue
			}
		}
		// If it's not a BlobNotFound error, return it immediately
		return nil, lastErr
	}

	// If we tried all extensions and none worked, return the last error encountered
	return nil, fmt.Errorf("file %s not found in azure container %s with extensions %v: %w", filename, a.containerURL.String(), extensionsToTry, lastErr)
}

// List returns a list of blob names in the Azure container matching the prefix.
func (a *AzBlobStorage) List(prefix string, recursive bool) ([]string, error) {
	ctx := context.Background()
	var blobNames []string

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

	return blobNames, nil
}

// Close closes the Azure Blob Storage connection.
func (a *AzBlobStorage) Close() error {
	// Azure SDK doesn't require explicit closing of connections
	return nil
}
