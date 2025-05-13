package storage

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"io"
	"log"
	"net/url"
	"strings"
)

type AzBlobStorage struct {
	containerURL azblob.ContainerURL
	debug        bool // Debug flag
	// Store for potential use/logging
	accountName   string
	containerName string
}

// debugf logs debug messages if debug is enabled
func (a *AzBlobStorage) debugf(format string, args ...interface{}) {
	if a.debug {
		log.Printf("[azblob:debug] "+format, args...)
	}
}

func (a *AzBlobStorage) azblobDebugLog(level pipeline.LogLevel, msg string) {
	a.debugf("[azblob:%v] %s", level, msg)
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

	storage := &AzBlobStorage{
		accountName: accountName,
		debug:       debug,
	}
	options := azblob.PipelineOptions{}
	if debug {
		options.Log = pipeline.LogOptions{
			Log: storage.azblobDebugLog,
			ShouldLog: func(level pipeline.LogLevel) bool {
				return true
			},
		}
	}

	// Use default pipeline options
	p := azblob.NewPipeline(credential, options)

	// Construct the container URL
	// For Azurite (local testing), use the custom endpoint if provided
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	if endpoint != "" {
		serviceURL = endpoint
	}

	parsedURL, err := url.Parse(fmt.Sprintf("%s/%s", serviceURL, containerName))
	if err != nil {
		return nil, fmt.Errorf("failed to parse azure container URL: %w", err)
	}
	containerURL := azblob.NewContainerURL(*parsedURL, p)

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

	storage.containerURL = containerURL

	if debug {
		log.Printf("[azblob:debug] Successfully initialized Azure Blob Storage client for account %s, container %s", accountName, containerName)
	}

	return storage, nil
}

// Upload uploads data to Azure Blob Storage.
// If contentEncoding is provided, it's assumed data is pre-compressed, and BlobHTTPHeaders.ContentEncoding is set.
// Otherwise, compressFormat and compressLevel are used for client-side compression.
func (a *AzBlobStorage) Upload(filename string, reader io.Reader, compressFormat string, compressLevel int, contentEncoding string) error {
	ctx := context.Background()
	blobName := filename
	var finalReader = reader
	httpHeaders := azblob.BlobHTTPHeaders{}

	if contentEncoding != "" {
		a.debugf("AzBlob Upload: pre-compressed data with contentEncoding: %s for blob %s", contentEncoding, blobName)
		actualEncoding := strings.ToLower(contentEncoding)
		switch actualEncoding {
		case "gzip":
			blobName += ".gz"
			httpHeaders.ContentEncoding = "gzip"
		case "zstd":
			blobName += ".zstd"
			httpHeaders.ContentEncoding = "zstd" // Azure supports this
		default:
			a.debugf("AzBlob Upload: unknown contentEncoding '%s' for blob %s, uploading as is", contentEncoding, blobName)
			// blobName remains unchanged
		}
	} else if compressFormat != "" && compressFormat != "none" {
		a.debugf("AzBlob Upload: compressing data with format: %s, level: %d for blob %s", compressFormat, compressLevel, blobName)
		var ext string
		finalReader, ext = compressStream(reader, compressFormat, compressLevel)
		blobName += ext
		switch strings.ToLower(compressFormat) {
		case "gzip":
			httpHeaders.ContentEncoding = "gzip"
		case "zstd":
			httpHeaders.ContentEncoding = "zstd"
		}
	} else {
		a.debugf("AzBlob Upload: uploading data uncompressed for blob %s", blobName)
	}

	a.debugf("AzBlob Upload: final blob name: %s", blobName)
	blobURL := a.containerURL.NewBlockBlobURL(blobName)

	uploadOptions := azblob.UploadStreamToBlockBlobOptions{}
	if httpHeaders.ContentEncoding != "" {
		uploadOptions.BlobHTTPHeaders = httpHeaders
		a.debugf("AzBlob Upload: setting ContentEncoding HTTP header to '%s' for blob %s", httpHeaders.ContentEncoding, blobName)
	}

	_, err := azblob.UploadStreamToBlockBlob(ctx, finalReader, blobURL, uploadOptions)
	if err != nil {
		a.debugf("Failed to upload blob %s: %v", blobName, err)
		return fmt.Errorf("failed to upload %s to azure container %s: %w", blobName, a.containerName, err)
	}
	a.debugf("Successfully uploaded blob: %s", blobName)
	return nil
}

// Download retrieves a blob from Azure Blob Storage.
// If noClientDecompression is true, the raw blob stream is returned.
// Otherwise, decompressStream is used based on the filename's extension.
func (a *AzBlobStorage) Download(filename string, noClientDecompression bool) (io.ReadCloser, error) {
	ctx := context.Background()
	a.debugf("AzBlob Download: attempting to download blob: %s (noClientDecompression: %t)", filename, noClientDecompression)
	blobURL := a.containerURL.NewBlockBlobURL(filename)

	// Directly attempt download
	response, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		a.debugf("Failed to download blob %s: %v", filename, err)
		return nil, fmt.Errorf("failed to download %s from azure container %s: %w", filename, a.containerName, err)
	}

	bodyStream := response.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3})

	if noClientDecompression {
		a.debugf("AzBlob Download: client-side decompression disabled for blob %s", filename)
		return bodyStream, nil
	}

	// Attempt to decompress. `decompressStream` uses the filename for extension detection.
	// Azure SDK's Download method with `azblob.BlobAccessConditions{IfMatch: response.ETag()}` and checking `response.ContentEncoding()`
	// could be used for more advanced handling if Azure performs server-side decompression based on Accept-Encoding (which it typically doesn't for raw blob download).
	a.debugf("AzBlob Download: attempting client-side decompression for blob %s", filename)
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
