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

// UploadWithExtension uploads pre-compressed data to GCS with the appropriate extension
func (g *GCSStorage) UploadWithExtension(filename string, reader io.Reader, contentEncoding string) error {
	ctx := context.Background()
	
	var ext string
	switch strings.ToLower(contentEncoding) {
	case "gzip":
		ext = ".gz"
	case "zstd":
		ext = ".zstd"
	}
	
	objectName := filename + ext
	obj := g.bucket.Object(objectName)
	writer := obj.NewWriter(ctx)
	
	// Устанавливаем Content-Encoding для объекта
	if contentEncoding != "" {
		writer.ContentEncoding = contentEncoding
	}

	_, err := io.Copy(writer, reader)
	if err != nil {
		// It's important to close the writer even on error to clean up resources.
		_ = writer.Close() // Ignore close error if copy failed
		return fmt.Errorf("failed to copy pre-compressed data to gcs object %s in bucket %s: %w", objectName, g.bucketName, err)
	}

	// Close the writer to finalize the upload
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close gcs writer for pre-compressed object %s in bucket %s: %w", objectName, g.bucketName, err)
	}
	return nil
}

// Download retrieves an object from GCS and returns a reader for its decompressed content.
func (g *GCSStorage) Download(filename string) (io.ReadCloser, error) {
	ctx := context.Background()
	obj := g.bucket.Object(filename)

	// Attempt to create a reader for the object
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s from gcs bucket %s: %w", filename, g.bucketName, err)
	}

	// Success! Return the reader wrapped in our decompressor.
	// The caller must close the returned reader.
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

func countChar(s string, c rune) int {
	count := 0
	for _, char := range s {
		if char == c {
			count++
		}
	}
	return count
}
