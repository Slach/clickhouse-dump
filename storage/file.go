package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileStorage implements RemoteStorage for local filesystem
type FileStorage struct {
	basePath string
}

// NewFileStorage creates a new FileStorage instance
func NewFileStorage(basePath string) (*FileStorage, error) {
	// Ensure basePath exists and is a directory
	if basePath != "" {
		if err := os.MkdirAll(basePath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create base path %s: %w", basePath, err)
		}
	}

	return &FileStorage{
		basePath: basePath,
	}, nil
}

// Upload writes data to a local file
func (f *FileStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	fullPath := filepath.Join(f.basePath, filename)

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", fullPath, err)
	}

	// Create the file
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer file.Close()

	// Compress and write the data
	compressedReader, ext := compressStream(reader, format, level)
	if ext != "" {
		// If compression was applied, rename the file to include extension
		newPath := fullPath + ext
		file.Close()
		os.Rename(fullPath, newPath)
		fullPath = newPath
		file, err = os.Create(fullPath)
		if err != nil {
			return fmt.Errorf("failed to create compressed file %s: %w", fullPath, err)
		}
		defer file.Close()
	}

	_, err = io.Copy(file, compressedReader)
	if err != nil {
		return fmt.Errorf("failed to write to file %s: %w", fullPath, err)
	}

	return nil
}

// Download reads data from a local file
func (f *FileStorage) Download(filename string) (io.ReadCloser, error) {
	extensionsToTry := []string{".gz", ".zstd", ""} // Try compressed first, then raw

	var lastErr error
	for _, ext := range extensionsToTry {
		fullPath := filepath.Join(f.basePath, filename + ext)
		file, err := os.Open(fullPath)
		if err == nil {
			// Success! Wrap the file reader with decompression.
			return decompressStream(file, filename + ext), nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("failed to open any version of %s (tried extensions .gz, .zstd, none): %w", 
		filename, lastErr)
}

// List returns files matching the prefix in the base path
func (f *FileStorage) List(prefix string) ([]string, error) {
	var matches []string

	searchPath := filepath.Join(f.basePath, prefix)
	dir := filepath.Dir(searchPath)
	pattern := filepath.Base(searchPath) + "*"

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		matched, err := filepath.Match(pattern, name)
		if err != nil {
			return nil, fmt.Errorf("invalid pattern %s: %w", pattern, err)
		}
		if matched {
			relPath, err := filepath.Rel(f.basePath, filepath.Join(dir, name))
			if err != nil {
				return nil, fmt.Errorf("failed to get relative path: %w", err)
			}
			matches = append(matches, relPath)
		}
	}

	return matches, nil
}

// Close is a no-op for local file storage
func (f *FileStorage) Close() error {
	return nil
}
