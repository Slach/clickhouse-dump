package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	googleHTTPTransport "google.golang.org/api/transport/http"
)

// rewriteTransport forces requests to use HTTP if the original scheme was HTTPS.
// This is useful for local test servers like fake-gcs-server that might be configured
// to listen on HTTP but SDKs might default to HTTPS.
type rewriteTransport struct {
	base http.RoundTripper
}

// RoundTrip modifies the request URL scheme to HTTP if it's HTTPS and then
// proceeds with the base RoundTripper.
func (r rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		req.URL.Scheme = "http"
	}
	return r.base.RoundTrip(req)
}

type GCSStorage struct {
	bucket     *storage.BucketHandle
	bucketName string          // Store bucket name for logging
	client     *storage.Client // Store client to close it later
	endpoint   string          // Custom endpoint URL
}

// NewGCSStorage creates a new Google Cloud Storage client.
func NewGCSStorage(bucketName, endpoint, credentialsFile string) (*GCSStorage, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("gcs bucket name cannot be empty")
	}
	ctx := context.Background()

	// Create client options
	var opts []option.ClientOption

	if credentialsFile == "" {
		opts = append(opts, option.WithoutAuthentication())
	} else {
		opts = append(opts, option.WithCredentialsFile(credentialsFile))
	}

	if endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
		// If endpoint starts with http://, configure a custom HTTP client
		// to ensure communication happens over http, as fake-gcs-server expects.
		if strings.HasPrefix(endpoint, "http://") {
			// Base transport with typical production-like settings, but adjusted for local testing.
			customTransport := &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&http.Transport{ // Re-use default dialer settings from http.DefaultTransport
					Proxy:                 http.ProxyFromEnvironment,
					ForceAttemptHTTP2:     true,
					MaxIdleConns:          100,
					IdleConnTimeout:       90 * time.Second,
					TLSHandshakeTimeout:   10 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				}).DialContext,
				MaxIdleConns:          10, // Reduced for local testing
				MaxIdleConnsPerHost:   10, // Reduced for local testing
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				ForceAttemptHTTP2:     false, // Important: Disable HTTP/2 for http endpoints
				TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, // Allow self-signed certs for local http
			}

			// Wrap with rewriteTransport to ensure http scheme
			customRoundTripper := rewriteTransport{base: customTransport}

			// Create an HTTP client using the custom transport, compatible with Google API client options.
			// We need to use googleHTTPTransport.NewClient to correctly wrap our RoundTripper.
			httpClient, err := googleHTTPTransport.NewClient(ctx, append(opts, option.WithHTTPClient(&http.Client{Transport: customRoundTripper}))...)
			if err != nil {
				return nil, fmt.Errorf("failed to create GCS HTTP client with custom transport: %w", err)
			}
			// Clear existing opts and use the fully configured httpClient via option.WithHTTPClient
			opts = []option.ClientOption{option.WithHTTPClient(httpClient)}
			// The endpoint is already part of httpClient's transport configuration indirectly
			// or directly if googleHTTPTransport.NewClient considers it.
			// If WithEndpoint is still needed, it should be compatible.
			// Let's ensure WithEndpoint is added back if it was cleared.
			if endpoint != "" {
				opts = append(opts, option.WithEndpoint(endpoint))
			}
		}
	}

	// Creates a client with specified options
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gcs client: %w", err)
	}

	return &GCSStorage{
		bucket:     client.Bucket(bucketName),
		bucketName: bucketName,
		client:     client,
		endpoint:   endpoint, // Store endpoint for reference
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
func (g *GCSStorage) List(prefix string, recursive bool) ([]string, error) {
	ctx := context.Background()
	var objectNames []string

	query := &storage.Query{
		Prefix:    prefix,
		Delimiter: "/",
	}

	if recursive {
		query.Delimiter = "" // Remove delimiter for recursive listing
	}

	it := g.bucket.Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in gcs bucket %s with prefix %s: %w", g.bucketName, prefix, err)
		}

		// For recursive or actual objects, add them
		if recursive || attrs.Name != "" {
			objectNames = append(objectNames, attrs.Name)
		}

		// For non-recursive, add prefixes (subdirectories)
		if !recursive && attrs.Prefix != "" {
			objectNames = append(objectNames, attrs.Prefix)
		}
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
