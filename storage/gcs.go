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
	"log"
)

// debugGCSTransport wraps an http.RoundTripper to log GCS requests and responses,
// and to rewrite media upload URLs to a custom endpoint if specified.
type debugGCSTransport struct {
	base                 http.RoundTripper
	isCustomEndpoint     bool
	customEndpointScheme string
	customEndpointHost   string // host:port
}

// RoundTrip executes a single HTTP transaction, adding logging before and after,
// and potentially rewriting the URL for media uploads to a custom endpoint.
func (dgt debugGCSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	originalURLStringForLog := r.URL.String() // Capture before potential modification

	if dgt.isCustomEndpoint && r.URL.Host == "storage.googleapis.com" && strings.HasPrefix(r.URL.Path, "/upload/") {
		// Log intent to rewrite
		log.Printf("[GCS_REWRITE] Attempting to rewrite media upload URL from: %s", r.URL.String())
		r.URL.Scheme = dgt.customEndpointScheme
		r.URL.Host = dgt.customEndpointHost
		// The path (e.g., /upload/storage/v1/b/bucket/o/object) should remain unchanged.
		log.Printf("[GCS_REWRITE] Rewritten media upload URL to: %s", r.URL.String())
	}

	logMsg := fmt.Sprintf(">>> [GCS_REQUEST] >>> %v %v", r.Method, r.URL.String())
	if originalURLStringForLog != r.URL.String() {
		logMsg += fmt.Sprintf(" (original: %s)", originalURLStringForLog)
	}
	logMsg += "\n"

	for h, values := range r.Header {
		for _, v := range values {
			logMsg += fmt.Sprintf("%v: %v\n", h, v)
		}
	}
	log.Println(logMsg)

	resp, err := dgt.base.RoundTrip(r)
	if err != nil {
		log.Printf("GCS_ERROR: %v", err)
		return resp, err
	}

	logMsg = fmt.Sprintf("<<< [GCS_RESPONSE: %s] <<< %v %v\n", resp.Status, r.Method, r.URL.String())
	if originalURLStringForLog != r.URL.String() && resp != nil && resp.Request != nil && originalURLStringForLog != resp.Request.URL.String() {
		// If the request URL was rewritten, also log the original URL context for the response
		logMsg = fmt.Sprintf("<<< [GCS_RESPONSE: %s] <<< %v %v (original request URL: %s)\n", resp.Status, r.Method, r.URL.String(), originalURLStringForLog)
	}

	for h, values := range resp.Header {
		for _, v := range values {
			logMsg += fmt.Sprintf("%v: %v\n", h, v)
		}
	}
	log.Println(logMsg)
	return resp, err
}

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

	// Base transport, starts similar to http.DefaultTransport
	var transport http.RoundTripper = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&http.Transport{ // Re-use default dialer settings from http.DefaultTransport
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	var ueScheme, ueHost string
	var ueIsSet bool
	if endpoint != "" { // endpoint is the original user-provided one
		ueIsSet = true
		parsedUserEndpoint, parseErr := url.Parse(endpoint)
		if parseErr != nil {
			return nil, fmt.Errorf("failed to parse custom endpoint URL '%s': %w", endpoint, parseErr)
		}
		ueScheme = parsedUserEndpoint.Scheme
		ueHost = parsedUserEndpoint.Host
		if ueScheme == "" || ueHost == "" {
			return nil, fmt.Errorf("custom endpoint URL '%s' must include scheme and host", endpoint)
		}
	}

	if endpoint != "" {
		if strings.HasPrefix(endpoint, "http://") {
			// For plain HTTP endpoints (like fake-gcs-server):
			// - Disable HTTP/2 (some emulators don't support it well)
			// - Skip TLS verification (not strictly needed for HTTP, but InsecureSkipVerify is for HTTPS with self-signed certs)
			// - Wrap with rewriteTransport to ensure HTTP scheme if SDK tries HTTPS.
			plainHttpTransport := &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&http.Transport{
					Proxy:                 http.ProxyFromEnvironment,
					ForceAttemptHTTP2:     false, // Adjusted
					MaxIdleConns:          100,
					IdleConnTimeout:       90 * time.Second,
					TLSHandshakeTimeout:   10 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				}).DialContext,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				ForceAttemptHTTP2:     false,                                 // Important: Disable HTTP/2 for http endpoints
				TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, // For "https" URLs to "http" server
			}
			transport = rewriteTransport{base: plainHttpTransport}
		} else if strings.HasPrefix(endpoint, "https://") {
			// For HTTPS custom endpoints that might use self-signed certs
			customHttpsTransport := &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&http.Transport{
					Proxy:                 http.ProxyFromEnvironment,
					ForceAttemptHTTP2:     true,
					MaxIdleConns:          100,
					IdleConnTimeout:       90 * time.Second,
					TLSHandshakeTimeout:   10 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				}).DialContext,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				ForceAttemptHTTP2:     true,
				TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, // Allow self-signed
			}
			transport = customHttpsTransport
		}
	}

	// Wrap with debug transport for logging and URL rewriting
	transport = debugGCSTransport{
		base:                 transport,
		isCustomEndpoint:     ueIsSet,
		customEndpointScheme: ueScheme,
		customEndpointHost:   ueHost,
	}
	httpClient := &http.Client{Transport: transport}

	// Prepare options for storage.NewClient
	storageClientOpts := []option.ClientOption{option.WithHTTPClient(httpClient)}

	if credentialsFile == "" {
		storageClientOpts = append(storageClientOpts, option.WithoutAuthentication())
	} else {
		storageClientOpts = append(storageClientOpts, option.WithCredentialsFile(credentialsFile))
	}

	if endpoint != "" {
		// Adjust endpoint to include /storage/v1/ path for GCS, as SDK might need it
		// for resolving all API paths correctly, especially MediaBasePath.
		gcsEndpointForClient := endpoint // Default to original
		parsedURL, parseErr := url.Parse(endpoint)
		if parseErr == nil {
			// Only adjust if the path is empty or just "/"
			// This means endpoint was like "http://host:port" or "http://host:port/"
			if parsedURL.Path == "" || parsedURL.Path == "/" {
				base := strings.TrimSuffix(endpoint, "/")
				gcsEndpointForClient = base + "/storage/v1/"
				log.Printf("Adjusted GCS endpoint for storage.NewClient: %s (original: %s)", gcsEndpointForClient, endpoint)
			} else {
				log.Printf("Using GCS endpoint as is (already has a path component): %s", endpoint)
			}
		} else {
			log.Printf("Warning: Could not parse GCS endpoint '%s' to adjust path: %v. Using it as is.", endpoint, parseErr)
		}
		storageClientOpts = append(storageClientOpts, option.WithEndpoint(gcsEndpointForClient))
	}

	client, err := storage.NewClient(ctx, storageClientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gcs client: %w", err)
	}

	return &GCSStorage{
		bucket:     client.Bucket(bucketName),
		bucketName: bucketName,
		client:     client,
		endpoint:   endpoint,
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
