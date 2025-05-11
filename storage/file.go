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

func (f *FileStorage) IsDebug() bool {
	return f.debug
}

// NewFileStorage creates a new FileStorage instance
func NewFileStorage(basePath string, debug bool) (*FileStorage, error) {
	// Check environment variable if debug wasn't explicitly set
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

// debugf logs only if debug is enabled
func (f *FileStorage) debugf(format string, args ...interface{}) {
	if f.debug {
		log.Printf("[file:debug] "+format, args...)
	}
}

// Upload writes data to a local file
func (f *FileStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	fullPath := filename
	if !strings.HasPrefix(filename, f.basePath) {
		fullPath = filepath.Join(f.basePath, filename)
	}
	f.debugf("Uploading file: %s (compression: %s level %d)", fullPath, format, level)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	f.debugf("Creating directory: %s", dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		f.debugf("Failed to create directory %s: %v", dir, err)
		return fmt.Errorf("failed to create directory for %s: %w", fullPath, err)
	}

	// Create the file
	f.debugf("Creating file: %s", fullPath)
	file, err := os.Create(fullPath)
	if err != nil {
		f.debugf("Failed to create file %s: %v", fullPath, err)
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			f.debugf("can't close %s: %v", fullPath, closeErr)
		}
	}()

	// Compress and write the data
	compressedReader, ext := compressStream(reader, format, level)
	if ext != "" {
		f.debugf("Applying compression (%s), renaming to %s%s", format, fullPath, ext)
		// If compression was applied, rename the file to include extension
		newPath := fullPath + ext
		if err := os.Rename(fullPath, newPath); err != nil {
			f.debugf("Failed to rename %s to %s: %v", fullPath, newPath, err)
			return fmt.Errorf("failed to rename file: %w", err)
		}
		fullPath = newPath
	}

	f.debugf("Writing data to file: %s", fullPath)
	_, err = io.Copy(file, compressedReader)
	if err != nil {
		f.debugf("Failed to write to file %s: %v", fullPath, err)
		return fmt.Errorf("failed to write to file %s: %w", fullPath, err)
	}

	f.debugf("Successfully uploaded file: %s", fullPath)
	return nil
}

// UploadWithExtension writes pre-compressed data to a local file with the appropriate extension
func (f *FileStorage) UploadWithExtension(filename string, reader io.Reader, contentEncoding string) error {
	fullPath := filename
	if !strings.HasPrefix(filename, f.basePath) {
		fullPath = filepath.Join(f.basePath, filename)
	}
	
	// Add extension based on compression type
	var ext string
	switch strings.ToLower(contentEncoding) {
	case "gzip":
		ext = ".gz"
	case "zstd":
		ext = ".zstd"
	}
	
	if ext != "" {
		fullPath = fullPath + ext
	}
	
	f.debugf("Uploading pre-compressed file: %s (encoding: %s)", fullPath, contentEncoding)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	f.debugf("Creating directory: %s", dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		f.debugf("Failed to create directory %s: %v", dir, err)
		return fmt.Errorf("failed to create directory for %s: %w", fullPath, err)
	}

	// Create the file
	f.debugf("Creating file: %s", fullPath)
	file, err := os.Create(fullPath)
	if err != nil {
		f.debugf("Failed to create file %s: %v", fullPath, err)
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			f.debugf("can't close %s: %v", fullPath, closeErr)
		}
	}()

	f.debugf("Writing pre-compressed data to file: %s", fullPath)
	_, err = io.Copy(file, reader)
	if err != nil {
		f.debugf("Failed to write to file %s: %v", fullPath, err)
		return fmt.Errorf("failed to write to file %s: %w", fullPath, err)
	}

	f.debugf("Successfully uploaded pre-compressed file: %s", fullPath)
	return nil
}

// Download reads data from a local file
func (f *FileStorage) Download(fileName string) (io.ReadCloser, error) {
	f.debugf("Attempting to download file: %s ", fileName)
	if !strings.HasPrefix(fileName, f.basePath) {
		fileName = filepath.Join(f.basePath, fileName)
	}
	f.debugf("Trying to open file: %s", fileName)
	file, err := os.Open(fileName)
	if err == nil {
		f.debugf("Successfully opened file: %s", fileName)
		// Success! Wrap the file reader with decompression.
		return decompressStream(file, fileName), nil
	}
	return nil, fmt.Errorf("failed to open file %s: %v", fileName, err)
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
