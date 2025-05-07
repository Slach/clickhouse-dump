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

// NewFileStorage creates a new FileStorage instance
func NewFileStorage(basePath string, debug bool) (*FileStorage, error) {
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
		log.Printf(format, args...)
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
	defer file.Close()

	// Compress and write the data
	compressedReader, ext := compressStream(reader, format, level)
	if ext != "" {
		f.debugf("Applying compression (%s), renaming to %s%s", format, fullPath, ext)
		// If compression was applied, rename the file to include extension
		newPath := fullPath + ext
		file.Close()
		if err := os.Rename(fullPath, newPath); err != nil {
			f.debugf("Failed to rename %s to %s: %v", fullPath, newPath, err)
			return fmt.Errorf("failed to rename file: %w", err)
		}
		fullPath = newPath
		file, err = os.Create(fullPath)
		if err != nil {
			f.debugf("Failed to create compressed file %s: %v", fullPath, err)
			return fmt.Errorf("failed to create compressed file %s: %w", fullPath, err)
		}
		defer file.Close()
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

// Download reads data from a local file
func (f *FileStorage) Download(filename string) (io.ReadCloser, error) {
	extensionsToTry := []string{".gz", ".zstd", ""} // Try compressed first, then raw
	f.debugf("Attempting to download file: %s (trying extensions: %v)", filename, extensionsToTry)

	var lastErr error
	for _, ext := range extensionsToTry {
		fullPath := filename + ext
		if !strings.HasPrefix(fullPath, f.basePath) {
			fullPath = filepath.Join(f.basePath, filename+ext)
		}
		f.debugf("Trying to open file: %s", fullPath)
		file, err := os.Open(fullPath)
		if err == nil {
			f.debugf("Successfully opened file: %s", fullPath)
			// Success! Wrap the file reader with decompression.
			return decompressStream(file, filename+ext), nil
		}
		f.debugf("Failed to open file %s: %v", fullPath, err)
		lastErr = err
	}

	err := fmt.Errorf("failed to open any version of %s (tried extensions .gz, .zstd, none): %w",
		filename, lastErr)
	f.debugf("%v", err)
	return nil, err
}

// List returns files matching the prefix in the base path
func (f *FileStorage) List(prefix string, recursive bool) ([]string, error) {
	f.debugf("Listing files with prefix: %s (recursive: %v)", prefix, recursive)
	var matches []string

	searchPath := prefix
	if !strings.HasPrefix(searchPath, f.basePath) {
		searchPath = filepath.Join(f.basePath, prefix)
	}

	var walkFn filepath.WalkFunc
	if recursive {
		walkFn = func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				relPath, err := filepath.Rel(f.basePath, path)
				if err != nil {
					return err
				}
				if strings.HasPrefix(relPath, prefix) {
					matches = append(matches, relPath)
				}
			}
			return nil
		}
	} else {
		walkFn = func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				relPath, err := filepath.Rel(f.basePath, path)
				if err != nil {
					return err
				}
				dir := filepath.Dir(relPath)
				if dir == filepath.Dir(prefix) || dir == prefix {
					matches = append(matches, relPath)
				}
			}
			return nil
		}
	}

	err := filepath.Walk(searchPath, walkFn)
	if err != nil {
		f.debugf("Failed to walk directory %s: %v", searchPath, err)
		return nil, fmt.Errorf("failed to walk directory %s: %w", searchPath, err)
	}

	f.debugf("Found %d matching files", len(matches))
	return matches, nil
}

// Close is a no-op for local file storage
func (f *FileStorage) Close() error {
	f.debugf("Closing file storage at: %s", f.basePath)
	return nil
}
