package storage

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"log"
)

// debugGCSTransport wraps an http.RoundTripper to log GCS requests and responses
type debugGCSTransport struct {
	base http.RoundTripper
}

// RoundTrip executes a single HTTP transaction with debug logging
func (dgt debugGCSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	originalURL := r.URL.String()

	// Log request
	logMsg := fmt.Sprintf(">>> [GCS_REQUEST] >>> %v %v\n", r.Method, originalURL)
	for h, values := range r.Header {
		for _, v := range values {
			logMsg += fmt.Sprintf("%v: %v\n", h, v)
		}
	}
	log.Println(logMsg)

	// Execute request
	resp, err := dgt.base.RoundTrip(r)
	if err != nil {
		log.Printf("GCS_ERROR: %v", err)
		return resp, err
	}

	// Log response
	logMsg = fmt.Sprintf("<<< [GCS_RESPONSE: %s] <<< %v %v\n", resp.Status, r.Method, originalURL)
	for h, values := range resp.Header {
		for _, v := range values {
			logMsg += fmt.Sprintf("%v: %v\n", h, v)
		}
	}
	log.Println(logMsg)

	return resp, err
}

// customEndpointGCSTransport wraps an http.RoundTripper to rewrite URLs for custom GCS endpoints
type customEndpointGCSTransport struct {
	base                 http.RoundTripper
	customEndpointScheme string
	customEndpointHost   string // host:port
}

// RoundTrip rewrites request URLs for custom GCS endpoints
func (cegt customEndpointGCSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Rewrite scheme and host to custom endpoint
	if r.URL.Scheme != cegt.customEndpointScheme || r.URL.Host != cegt.customEndpointHost {
		r.URL.Scheme = cegt.customEndpointScheme
		r.URL.Host = cegt.customEndpointHost
	}

	// Path rewriting logic for fake-gcs-server compatibility
	currentPath := r.URL.Path
	newPath := currentPath

	if strings.HasPrefix(currentPath, "/b/") && strings.Contains(currentPath, "/o") {
		// Rewrite /b/bucket/o to /storage/v1/b/bucket/o
		if !strings.HasPrefix(currentPath, "/storage/v1/") {
			newPath = "/storage/v1" + currentPath
		}
	} else if !strings.HasPrefix(currentPath, "/storage/v1/") &&
		!strings.HasPrefix(currentPath, "/upload/") &&
		strings.Count(currentPath, "/") >= 2 {
		// Rewrite /bucket/object to /storage/v1/b/bucket/o/object
		parts := strings.SplitN(strings.TrimPrefix(currentPath, "/"), "/", 2)
		if len(parts) == 2 {
			newPath = fmt.Sprintf("/storage/v1/b/%s/o/%s", parts[0], parts[1])
			if r.URL.Query().Get("alt") == "" {
				q := r.URL.Query()
				q.Set("alt", "media")
				r.URL.RawQuery = q.Encode()
			}
		}
	}

	if newPath != currentPath {
		r.URL.Path = newPath
	}

	return cegt.base.RoundTrip(r)
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
	debug      bool            // Debug logging flag
}

// NewGCSStorage creates a new Google Cloud Storage client.
func NewGCSStorage(bucketName, endpoint, credentialsFile string, debug bool) (*GCSStorage, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("gcs bucket name cannot be empty")
	}
	ctx := context.Background()
	if debug {
		log.Printf("Initializing GCS storage with bucketName=%s, endpoint=%s, debug=%t", bucketName, endpoint, debug)
	}
	// Base transport, starts similar to http.DefaultTransport
	var transport http.RoundTripper = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
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
		if strings.HasPrefix(endpoint, "http://") {
			// For plain HTTP endpoints (like fake-gcs-server):
			// - Disable HTTP/2 (some emulators don't support it well)
			// - Skip TLS verification (not strictly needed for HTTP, but InsecureSkipVerify is for HTTPS with self-signed certs)
			// - Wrap with rewriteTransport to ensure HTTP scheme if SDK tries HTTPS.
			plainHttpTransport := &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				ForceAttemptHTTP2:     false,
				TLSClientConfig: &tls.Config{
					NextProtos: []string{"http/1.1"},
				},
			}
			transport = rewriteTransport{base: plainHttpTransport}
		} else if strings.HasPrefix(endpoint, "https://") {
			// For HTTPS custom endpoints that might use self-signed certs
			customHttpsTransport := &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
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

	// Chain transports - first custom endpoint, then debug if needed
	if ueIsSet {
		transport = customEndpointGCSTransport{
			base:                 transport,
			customEndpointScheme: ueScheme,
			customEndpointHost:   ueHost,
		}
	}
	if debug {
		transport = debugGCSTransport{
			base: transport,
		}
	}
	httpClient := &http.Client{Transport: transport}

	storageClientOpts := []option.ClientOption{option.WithHTTPClient(httpClient)}

	if credentialsFile == "" {
		storageClientOpts = append(storageClientOpts, option.WithoutAuthentication())
	} else {
		storageClientOpts = append(storageClientOpts, option.WithCredentialsFile(credentialsFile))
	}

	if endpoint != "" {
		log.Printf("Using user-provided GCS endpoint for storage.NewClient: %s", endpoint)
		storageClientOpts = append(storageClientOpts, option.WithEndpoint(endpoint))
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
		debug:      debug,
	}, nil
}

// Upload uploads data to GCS.
// If contentEncoding is provided, it's assumed data is pre-compressed, and GCS object's ContentEncoding metadata is set.
// Otherwise, compressFormat and compressLevel are used for client-side compression.
func (g *GCSStorage) Upload(filename string, reader io.Reader, compressFormat string, compressLevel int, contentEncoding string) error {
	ctx := context.Background()
	objectName := filename
	var finalReader io.Reader = reader
	var gcsObjectContentEncoding string // For GCS metadata

	if contentEncoding != "" {
		g.debugf("GCS Upload: pre-compressed data with contentEncoding: %s for object %s", contentEncoding, objectName)
		actualEncoding := strings.ToLower(contentEncoding)
		switch actualEncoding {
		case "gzip":
			objectName += ".gz"
			gcsObjectContentEncoding = "gzip"
		case "zstd":
			objectName += ".zstd"
			gcsObjectContentEncoding = "zstd" // GCS supports this
		default:
			g.debugf("GCS Upload: unknown contentEncoding '%s' for object %s, uploading as is", contentEncoding, objectName)
			// objectName remains unchanged
		}
	} else if compressFormat != "" && compressFormat != "none" {
		g.debugf("GCS Upload: compressing data with format: %s, level: %d for object %s", compressFormat, compressLevel, objectName)
		var ext string
		finalReader, ext = compressStream(reader, compressFormat, compressLevel)
		objectName += ext
		switch strings.ToLower(compressFormat) {
		case "gzip":
			gcsObjectContentEncoding = "gzip"
		case "zstd":
			gcsObjectContentEncoding = "zstd"
		}
	} else {
		g.debugf("GCS Upload: uploading data uncompressed for object %s", objectName)
	}

	g.debugf("GCS Upload: final object name: %s", objectName)
	obj := g.bucket.Object(objectName)
	writer := obj.NewWriter(ctx)

	if gcsObjectContentEncoding != "" {
		writer.ContentEncoding = gcsObjectContentEncoding
		g.debugf("GCS Upload: setting ContentEncoding metadata to '%s' for object %s", gcsObjectContentEncoding, objectName)
	}
	// writer.ContentType = "application/octet-stream" // Optional: set content type

	_, err := io.Copy(writer, finalReader)
	if err != nil {
		_ = writer.Close() // Attempt to close writer even on error
		return fmt.Errorf("failed to copy data to gcs object %s in bucket %s: %w", objectName, g.bucketName, err)
	}

	err = writer.Close() // Close the writer to finalize the upload
	if err != nil {
		return fmt.Errorf("failed to close gcs writer for object %s in bucket %s: %w", objectName, g.bucketName, err)
	}
	return nil
}

// Download retrieves an object from GCS.
// If noClientDecompression is true, the raw object stream is returned.
// Otherwise, decompressStream is used based on the filename's extension and object's ContentEncoding.
func (g *GCSStorage) Download(filename string, noClientDecompression bool) (io.ReadCloser, error) {
	ctx := context.Background()
	g.debugf("GCS Download: attempting to download object: %s (noClientDecompression: %t)", filename, noClientDecompression)
	obj := g.bucket.Object(filename)

	// Attempt to create a reader for the object
	reader, err := obj.NewReader(ctx)
	if err != nil {
		// TODO: Implement retry logic for .gz, .zst extensions if filename not found,
		// similar to how other storages might handle it, or rely on GCS's ContentEncoding.
		// For now, we assume `filename` is the exact object name.
		return nil, fmt.Errorf("failed to create reader for gcs object %s in bucket %s: %w", filename, g.bucketName, err)
	}

	// If client-side decompression is disabled, return the raw reader.
	if noClientDecompression {
		g.debugf("GCS Download: client-side decompression disabled for object %s", filename)
		return reader, nil
	}

	// Attempt to decompress. `decompressStream` will use the filename to check for .gz/.zstd extensions.
	// GCS SDK's reader also respects ContentEncoding from the object's metadata for certain encodings (like gzip)
	// and might return an already decompressed stream. `decompressStream` should handle this gracefully.
	g.debugf("GCS Download: attempting client-side decompression for object %s", filename)
	// The `filename` passed to `decompressStream` is crucial for it to detect extension-based compression.
	// If GCS already decompressed (e.g. due to `Accept-Encoding: gzip` and object `Content-Encoding: gzip`),
	// `decompressStream` on an uncompressed stream with a `.gz` filename might error or misbehave.
	// However, our `decompressStream` checks magic numbers, so it should be safe.
	// The `reader.attrs.ContentEncoding` could also be inspected here.
	return decompressStream(reader, filename), nil
}

// List returns a list of object names in the GCS bucket matching the prefix.
func (g *GCSStorage) List(prefix string, recursive bool) ([]string, error) {
	ctx := context.Background()
	// Ensure prefix is clean and doesn't start with a slash if it's not just "/"
	// For GCS, a prefix should not typically start with a slash.
	// An empty prefix means list from the root.
	// A prefix like "folder/" means list objects in "folder".
	if prefix != "" && prefix != "/" {
		prefix = strings.TrimLeft(prefix, "/")
	} else if prefix == "/" {
		// If the intention is to list from the root, GCS expects an empty prefix.
		prefix = ""
	}

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
		if errors.Is(err, iterator.Done) {
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
