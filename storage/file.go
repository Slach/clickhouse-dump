package storage

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// FileStorage implements RemoteStorage for local filesystem
type FileStorage struct {
	basePath string
	debug    bool
}

// debugf logs only if debug is enabled
func (f *FileStorage) debugf(format string, args ...interface{}) {
	if f.debug {
		log.Printf("[file:debug] "+format, args...)
	}
}

// NewFileStorage creates a new FileStorage instance
func NewFileStorage(basePath string, debug bool) (*FileStorage, error) {
	if !debug && os.Getenv("LOG_LEVEL") == "debug" {
		debug = true
	}
	f := &FileStorage{
		basePath: basePath,
		debug:    debug,
	}
	// Ensure basePath exists and is a directory
	if basePath != "" {
		f.debugf("Creating base directory: %s", basePath)
		if err := os.MkdirAll(basePath, 0755); err != nil {
			f.debugf("Failed to create base path %s: %v", basePath, err)
			return nil, fmt.Errorf("failed to create base path %s: %w", basePath, err)
		}
	}
	f.debugf("Initialized file storage at: %s", basePath)
	return f, nil
}

// Upload writes data to a local file.
// If contentEncoding is provided, it's assumed data is pre-compressed.
// Otherwise, compressFormat and compressLevel are used.
func (f *FileStorage) Upload(filename string, reader io.Reader, compressFormat string, compressLevel int, contentEncoding string) error {
	targetPath := filename
	if !strings.HasPrefix(filename, f.basePath) {
		targetPath = filepath.Join(f.basePath, filename)
	}

	var finalReader io.Reader = reader
	var finalPath string = targetPath
	usedCompression := false

	if contentEncoding != "" {
		f.debugf("Uploading pre-compressed file: %s (contentEncoding: %s)", targetPath, contentEncoding)
		switch strings.ToLower(contentEncoding) {
		case "gzip":
			finalPath += ".gz"
		case "zstd":
			finalPath += ".zstd"
		default:
			f.debugf("Unknown contentEncoding '%s', uploading as is to %s", contentEncoding, targetPath)
		}
		// finalReader is already 'reader'
	} else if compressFormat != "" && compressFormat != "none" {
		f.debugf("Uploading file: %s (compression: %s, level: %d)", targetPath, compressFormat, compressLevel)
		var ext string
		finalReader, ext = compressStream(reader, compressFormat, compressLevel)
		if ext != "" {
			finalPath = targetPath + ext
			usedCompression = true
		}
	} else {
		f.debugf("Uploading file: %s (no compression)", targetPath)
		// finalReader is 'reader', finalPath is 'targetPath'
	}

	// Ensure directory exists
	dir := filepath.Dir(finalPath)
	f.debugf("Creating directory for final path: %s", dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		f.debugf("Failed to create directory %s: %v", dir, err)
		return fmt.Errorf("failed to create directory for %s: %w", finalPath, err)
	}

	// Create the file with the final path
	f.debugf("Creating file: %s", finalPath)
	file, err := os.Create(finalPath)
	if err != nil {
		f.debugf("Failed to create file %s: %v", finalPath, err)
		return fmt.Errorf("failed to create file %s: %w", finalPath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			f.debugf("can't close %s: %v", finalPath, closeErr)
		}
	}()

	if usedCompression {
		f.debugf("Writing compressed data to file: %s", finalPath)
	} else if contentEncoding != "" {
		f.debugf("Writing pre-compressed data (contentEncoding: %s) to file: %s", contentEncoding, finalPath)
	} else {
		f.debugf("Writing uncompressed data to file: %s", finalPath)
	}

	_, err = io.Copy(file, finalReader)
	if err != nil {
		f.debugf("Failed to write to file %s: %v", finalPath, err)
		return fmt.Errorf("failed to write to file %s: %w", finalPath, err)
	}

	f.debugf("Successfully uploaded file: %s", finalPath)
	return nil
}

// Download reads data from a local file.
// If noClientDecompression is true, data is returned as is.
// Otherwise, decompressStream is used.
func (f *FileStorage) Download(fileName string, noClientDecompression bool) (io.ReadCloser, error) {
	f.debugf("Attempting to download file: %s (noClientDecompression: %t)", fileName, noClientDecompression)
	fullPath := fileName
	if !strings.HasPrefix(fileName, f.basePath) {
		fullPath = filepath.Join(f.basePath, fileName)
	}
	f.debugf("Trying to open file: %s", fullPath)
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}

	f.debugf("Successfully opened file: %s", fullPath)
	if noClientDecompression {
		f.debugf("Client side decompression disabled, returning raw file stream for %s", fullPath)
		return file, nil
	}

	// Success! Wrap the file reader with decompression.
	f.debugf("Attempting client side decompression for %s", fullPath)
	return decompressStream(file, fullPath), nil // decompressStream uses fullPath to determine extension
}

// List returns files matching the prefix in the base path
func (f *FileStorage) List(prefix string, recursive bool) ([]string, error) {
	var matches []string

	searchPath := prefix
	if !strings.HasPrefix(searchPath, f.basePath) {
		searchPath = filepath.Join(f.basePath, prefix)
	}
	f.debugf("Listing files with searchPath: %s (recursive: %v)", searchPath, recursive)

	var walkFn filepath.WalkFunc
	walkFn = func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, relErr := filepath.Rel(f.basePath, path)
			if relErr != nil {
				return relErr
			}
			if recursive {
				matches = append(matches, relPath)
			} else {
				dir := filepath.Dir(relPath)
				if dir == filepath.Dir(prefix) || dir == prefix {
					matches = append(matches, relPath)
				}
			}
		}
		return nil
	}

	walkErr := filepath.Walk(searchPath, walkFn)
	if walkErr != nil {
		f.debugf("Failed to walk directory %s: %v", searchPath, walkErr)
		return nil, fmt.Errorf("failed to walk directory %s: %w", searchPath, walkErr)
	}

	f.debugf("Found %d matching files", len(matches))
	return matches, nil
}

// Close is a no-op for local file storage
func (f *FileStorage) Close() error {
	f.debugf("Closing file storage at: %s", f.basePath)
	return nil
}
