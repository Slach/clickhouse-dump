package storage

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GCSStorage struct {
	bucket     *storage.BucketHandle
	bucketName string          // Store bucket name for logging
	client     *storage.Client // Store client to close it later
	endpoint   string          // Custom endpoint URL
}

// NewGCSStorage creates a new Google Cloud Storage client.
func NewGCSStorage(bucketName, endpoint string) (*GCSStorage, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("gcs bucket name cannot be empty")
	}
	ctx := context.Background()
	
	// Create client options
	opts := []option.ClientOption{}
	if endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
	}
	
	// Creates a client with default credentials found in the environment.
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gcs client: %w", err)
	}

	return &GCSStorage{
		bucket:     client.Bucket(bucketName),
		bucketName: bucketName,
		client:     client,
	}, nil
}

// Upload compresses and uploads data to the specified GCS object name (filename + compression extension).
func (g *GCSStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	ctx := context.Background()
	compressedReader, ext := compressStream(reader, format, level)
	objectName := filename + ext
	obj := g.bucket.Object(objectName)
	writer := obj.NewWriter(ctx)
	// Can set metadata, content type etc. on writer here if needed
	// writer.ContentType = "application/octet-stream"

	_, err := io.Copy(writer, compressedReader)
	if err != nil {
		// It's important to close the writer even on error to clean up resources.
		_ = writer.Close() // Ignore close error if copy failed
		return fmt.Errorf("failed to copy data to gcs object %s in bucket %s: %w", objectName, g.bucketName, err)
	}

	// Close the writer to finalize the upload
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close gcs writer for object %s in bucket %s: %w", objectName, g.bucketName, err)
	}
	return nil
}

// Download retrieves an object from GCS and returns a reader for its decompressed content.
// It tries common compression extensions (.gz, .zstd) if the base filename doesn't exist.
func (g *GCSStorage) Download(filename string) (io.ReadCloser, error) {
	ctx := context.Background()
	extensionsToTry := []string{".gz", ".zstd", ""} // Try compressed first, then raw

	var lastErr error
	for _, ext := range extensionsToTry {
		objectName := filename + ext
		obj := g.bucket.Object(objectName)

		// Attempt to create a reader for the object
		reader, err := obj.NewReader(ctx)

		if err == nil {
			// Success! Return the reader wrapped in our decompressor.
			// The caller must close the returned reader.
			decompressedStream := decompressStream(reader, objectName) // Handles decompression based on objectName extension
			return decompressedStream, nil
		}

		// Handle error
		lastErr = fmt.Errorf("failed attempt to download %s from gcs bucket %s: %w", objectName, g.bucketName, err)

		// Check if the error is storage.ErrObjectNotExist
		if err == storage.ErrObjectNotExist {
			// Object not found, continue to try the next extension
			continue
		}

		// If it's not an ErrObjectNotExist error, return it immediately
		return nil, lastErr
	}

	// If we tried all extensions and none worked, return the last error encountered
	return nil, fmt.Errorf("file %s not found in gcs bucket %s with extensions %v: %w", filename, g.bucketName, extensionsToTry, lastErr)
}

// List returns a list of object names in the GCS bucket matching the prefix.
func (g *GCSStorage) List(prefix string) ([]string, error) {
	ctx := context.Background()
	var objectNames []string

	// Create query to list objects with the given prefix
	query := &storage.Query{Prefix: prefix}
	it := g.bucket.Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			// Finished iterating
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in gcs bucket %s with prefix %s: %w", g.bucketName, prefix, err)
		}
		objectNames = append(objectNames, attrs.Name)
	}

	return objectNames, nil
}

// Close closes the underlying GCS client.
func (g *GCSStorage) Close() error {
	if g.client != nil {
		return g.client.Close()
	}
	return nil
}
