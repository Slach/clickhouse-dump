package storage

import (
	"fmt"
	"io"
	"log"
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
		log.Printf("Creating base directory: %s", basePath)
		if err := os.MkdirAll(basePath, 0755); err != nil {
			log.Printf("Failed to create base path %s: %v", basePath, err)
			return nil, fmt.Errorf("failed to create base path %s: %w", basePath, err)
		}
	}

	log.Printf("Initialized file storage at: %s", basePath)
	return &FileStorage{
		basePath: basePath,
	}, nil
}

// Upload writes data to a local file
func (f *FileStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	fullPath := filepath.Join(f.basePath, filename)
	log.Printf("Uploading file: %s (compression: %s level %d)", fullPath, format, level)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	log.Printf("Creating directory: %s", dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Failed to create directory %s: %v", dir, err)
		return fmt.Errorf("failed to create directory for %s: %w", fullPath, err)
	}

	// Create the file
	log.Printf("Creating file: %s", fullPath)
	file, err := os.Create(fullPath)
	if err != nil {
		log.Printf("Failed to create file %s: %v", fullPath, err)
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer file.Close()

	// Compress and write the data
	compressedReader, ext := compressStream(reader, format, level)
	if ext != "" {
		log.Printf("Applying compression (%s), renaming to %s%s", format, fullPath, ext)
		// If compression was applied, rename the file to include extension
		newPath := fullPath + ext
		file.Close()
		if err := os.Rename(fullPath, newPath); err != nil {
			log.Printf("Failed to rename %s to %s: %v", fullPath, newPath, err)
			return fmt.Errorf("failed to rename file: %w", err)
		}
		fullPath = newPath
		file, err = os.Create(fullPath)
		if err != nil {
			log.Printf("Failed to create compressed file %s: %v", fullPath, err)
			return fmt.Errorf("failed to create compressed file %s: %w", fullPath, err)
		}
		defer file.Close()
	}

	log.Printf("Writing data to file: %s", fullPath)
	_, err = io.Copy(file, compressedReader)
	if err != nil {
		log.Printf("Failed to write to file %s: %v", fullPath, err)
		return fmt.Errorf("failed to write to file %s: %w", fullPath, err)
	}

	log.Printf("Successfully uploaded file: %s", fullPath)
	return nil
}

// Download reads data from a local file
func (f *FileStorage) Download(filename string) (io.ReadCloser, error) {
	extensionsToTry := []string{".gz", ".zstd", ""} // Try compressed first, then raw
	log.Printf("Attempting to download file: %s (trying extensions: %v)", filename, extensionsToTry)

	var lastErr error
	for _, ext := range extensionsToTry {
		fullPath := filepath.Join(f.basePath, filename + ext)
		log.Printf("Trying to open file: %s", fullPath)
		file, err := os.Open(fullPath)
		if err == nil {
			log.Printf("Successfully opened file: %s", fullPath)
			// Success! Wrap the file reader with decompression.
			return decompressStream(file, filename + ext), nil
		}
		log.Printf("Failed to open file %s: %v", fullPath, err)
		lastErr = err
	}

	err := fmt.Errorf("failed to open any version of %s (tried extensions .gz, .zstd, none): %w", 
		filename, lastErr)
	log.Printf("%v", err)
	return nil, err
}

// List returns files matching the prefix in the base path
func (f *FileStorage) List(prefix string) ([]string, error) {
	log.Printf("Listing files with prefix: %s", prefix)
	var matches []string

	searchPath := filepath.Join(f.basePath, prefix)
	dir := filepath.Dir(searchPath)
	pattern := filepath.Base(searchPath) + "*"
	log.Printf("Searching in directory: %s with pattern: %s", dir, pattern)

	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("Failed to read directory %s: %v", dir, err)
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		matched, err := filepath.Match(pattern, name)
		if err != nil {
			log.Printf("Invalid pattern %s: %v", pattern, err)
			return nil, fmt.Errorf("invalid pattern %s: %w", pattern, err)
		}
		if matched {
			relPath, err := filepath.Rel(f.basePath, filepath.Join(dir, name))
			if err != nil {
				log.Printf("Failed to get relative path for %s: %v", name, err)
				return nil, fmt.Errorf("failed to get relative path: %w", err)
			}
			log.Printf("Found matching file: %s", relPath)
			matches = append(matches, relPath)
		}
	}

	log.Printf("Found %d matching files", len(matches))
	return matches, nil
}

// Close is a no-op for local file storage
func (f *FileStorage) Close() error {
	log.Printf("Closing file storage at: %s", f.basePath)
	return nil
}
