package storage

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

// RemoteStorage defines the interface for interacting with different storage backends.
type RemoteStorage interface {
	// Upload uploads data from the reader to the specified filename.
	// If contentEncoding is provided (e.g., "gzip", "zstd"), the data is assumed to be
	// already compressed in that format, and the corresponding extension (.gz, .zstd)
	// will be appended to the filename. compressFormat and compressLevel are ignored in this case.
	// Otherwise, if contentEncoding is empty, the data will be compressed using
	// compressFormat and compressLevel before uploading, and the appropriate extension
	// will be appended. If compressFormat is empty or "none", no compression is applied.
	Upload(filename string, reader io.Reader, compressFormat string, compressLevel int, contentEncoding string) error

	// Download retrieves the content of the specified filename.
	// Implementations should automatically handle decompression based on common
	// extensions (.gz, .zstd)
	Download(filename string) (io.ReadCloser, error)

	// List returns a list of filenames in the storage backend matching the prefix.
	// The returned filenames might include compression extensions.
	// If recursive is true, it will list all files under the prefix recursively.
	// The prefix should be treated as a directory path when recursive=true.
	List(prefix string, recursive bool) ([]string, error)

	// Close terminates the connection to the storage backend, if applicable.
	Close() error
}

// compressStream wraps the reader with a compression writer based on format and level.
// It returns the reader end of the pipe and the appropriate file extension.
// If format is empty or "none", it returns the original reader and an empty extension.
func compressStream(reader io.Reader, format string, level int) (io.Reader, string) {
	format = strings.ToLower(format)
	if format == "" || format == "none" {
		return reader, ""
	}

	ext := ""
	var compressedReader = reader // Default to original reader

	pr, pw := io.Pipe() // Create pipe for async compression

	switch format {
	case "gzip":
		ext = ".gz"
		go func() {
			// Ensure level is valid for gzip
			if level < gzip.BestSpeed || level > gzip.BestCompression {
				level = gzip.DefaultCompression
			}
			gw, _ := gzip.NewWriterLevel(pw, level)
			_, err := io.Copy(gw, reader)
			// Close the gzip writer *before* closing the pipe writer
			closeErr := gw.Close()
			// Close the pipe writer, propagating the first error encountered
			if err != nil {
				_ = pw.CloseWithError(err)
			} else {
				_ = pw.CloseWithError(closeErr)
			}
		}()
		compressedReader = pr

	case "zstd":
		ext = ".zstd"
		go func() {
			// Ensure level is valid for zstd (maps to zstd levels)
			zstdLevel := zstd.EncoderLevelFromZstd(level) // Use mapping function
			zw, _ := zstd.NewWriter(pw, zstd.WithEncoderLevel(zstdLevel))
			_, err := io.Copy(zw, reader)
			// Close the zstd writer *before* closing the pipe writer
			closeErr := zw.Close()
			// Close the pipe writer, propagating the first error encountered
			if err != nil {
				_ = pw.CloseWithError(err)
			} else {
				_ = pw.CloseWithError(closeErr)
			}
		}()
		compressedReader = pr

	default:
		// No compression or unknown format, return original reader and empty extension
		return reader, ""
	}

	return compressedReader, ext
}

// zstdReaderCloser wraps a zstd.Decoder and the underlying reader to satisfy io.ReadCloser.
type zstdReaderCloser struct {
	*zstd.Decoder
	underlyingReader io.ReadCloser
}

// Close closes the zstd decoder and the underlying reader.
func (zrc *zstdReaderCloser) Close() error {
	// Close the zstd decoder first (releases resources)
	zrc.Decoder.Close()
	// Then close the underlying reader and return its error status.
	return zrc.underlyingReader.Close()
}

// decompressStream wraps the reader with a decompression reader if the filename suggests compression.
// It now returns an io.ReadCloser to ensure the underlying reader can be closed.
// If no known compression extension is found, it returns the original reader.
func decompressStream(reader io.ReadCloser, filename string) io.ReadCloser {
	compressionExtension := GetCompressionExtension(filename)
	switch compressionExtension {
	case ".gz":
		gr, err := gzip.NewReader(reader)
		if err != nil {
			closeErr := reader.Close()
			return &errorReaderCloser{err: fmt.Errorf("failed to create gzip reader for %s: %w, reader closed %w", filename, err, closeErr)}
		}
		// Gzip reader needs to be closed to close the underlying reader.
		return gr // gr implements io.ReadCloser
	case ".zstd":
		zr, err := zstd.NewReader(reader)
		if err != nil {
			closeErr := reader.Close()
			return &errorReaderCloser{err: fmt.Errorf("failed to create zstd reader for %s: %w, reader closed %w", filename, err, closeErr)}
		}
		// Wrap the zstd.Decoder and the original reader in our custom closer.
		return &zstdReaderCloser{Decoder: zr, underlyingReader: reader}
	default:
		// No decompression needed, return original reader
		return reader
	}
}

// GetCompressionExtension extracts known compression extensions (.gz, .zstd) from a filename.
func GetCompressionExtension(filename string) string {
	ext := filepath.Ext(filename)
	lowerExt := strings.ToLower(ext)
	if lowerExt == ".gz" || lowerExt == ".zstd" {
		return ext
	}
	return ""
}

// --- Helper for returning errors from decompressStream ---

// errorReaderCloser is an io.ReadCloser that always returns an error on Read.
type errorReaderCloser struct {
	err error
}

func (e *errorReaderCloser) Read(_ []byte) (n int, err error) {
	return 0, e.err
}

func (e *errorReaderCloser) Close() error {
	return e.err // Or return nil? Let's return the error.
}
