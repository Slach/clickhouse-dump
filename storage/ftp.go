package storage

import (
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

type FTPStorage struct {
	client *ftp.ServerConn
	host   string // Store host for logging/reconnect?
	user   string
	debug  bool // Debug flag
	// Password not stored for security
}

// debugf logs debug messages if debug is enabled
func (f *FTPStorage) debugf(format string, args ...interface{}) {
	if f.debug {
		log.Printf("[ftp:debug] "+format, args...)
	}
}

// NewFTPStorage creates a new FTP storage client.
func NewFTPStorage(host, user, password string, debug bool) (*FTPStorage, error) {
	if host == "" || user == "" { // Password can potentially be empty for anonymous
		return nil, fmt.Errorf("ftp host and user cannot be empty")
	}

	// Add default port if not specified
	if !strings.Contains(host, ":") {
		host = net.JoinHostPort(host, "21")
	}

	if debug {
		log.Printf("[ftp:debug] Connecting to FTP server %s with user %s", host, user)
	}

	// Dial with timeout
	c, err := ftp.Dial(host, ftp.DialWithTimeout(10*time.Second))
	if err != nil {
		if debug {
			log.Printf("[ftp:debug] Failed to dial FTP host %s: %v", host, err)
		}
		return nil, fmt.Errorf("failed to dial ftp host %s: %w", host, err)
	}

	if debug {
		log.Printf("[ftp:debug] Successfully connected to FTP server %s", host)
	}

	// Login with timeout
	err = c.Login(user, password)
	if err != nil {
		if debug {
			log.Printf("[ftp:debug] Failed to login to FTP host %s with user %s: %v", host, user, err)
		}
		c.Quit() // Attempt to close connection on login failure
		return nil, fmt.Errorf("failed to login to ftp host %s with user %s: %w", host, user, err)
	}

	if debug {
		log.Printf("[ftp:debug] Successfully logged in to FTP server %s with user %s", host, user)
	}

	return &FTPStorage{
		client: c,
		host:   host,
		user:   user,
		debug:  debug,
	}, nil
}

// Upload compresses and uploads data to the specified filename (filename + compression extension) via FTP.
func (f *FTPStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	compressedReader, ext := compressStream(reader, format, level)
	remoteFilename := filename + ext

	f.debugf("Uploading file: %s (compression: %s level %d)", remoteFilename, format, level)

	// Ensure directory exists
	dir := filepath.Dir(remoteFilename)
	if dir != "." && dir != "/" {
		f.debugf("Creating directory structure: %s", dir)
		// Split the path and create each directory
		parts := strings.Split(strings.TrimPrefix(dir, "/"), "/")
		currentPath := ""

		for _, part := range parts {
			if part == "" {
				continue
			}

			if currentPath != "" {
				currentPath += "/"
			}
			currentPath += part

			f.debugf("Checking directory: %s", currentPath)
			entries, err := f.client.List(currentPath)
			if err != nil || len(entries) == 0 {
				f.debugf("Creating directory: %s", currentPath)
				mkdirErr := f.client.MakeDir(currentPath)
				if mkdirErr != nil {
					f.debugf("Failed to create directory %s: %v", currentPath, mkdirErr)
					// Continue anyway - the directory might already exist
				}
			}
		}
	}

	f.debugf("Storing file: %s", remoteFilename)
	err := f.client.Stor(remoteFilename, compressedReader)
	if err != nil {
		f.debugf("Failed to upload %s: %v", remoteFilename, err)
		return fmt.Errorf("failed to upload %s to ftp host %s: %w", remoteFilename, f.host, err)
	}

	f.debugf("Successfully uploaded file: %s", remoteFilename)
	return nil
}

// Download retrieves a file from FTP and returns a reader for its decompressed content.
// It tries common compression extensions (.gz, .zstd) if the base filename doesn't exist.
func (f *FTPStorage) Download(filename string) (io.ReadCloser, error) {
	extensionsToTry := []string{".gz", ".zstd", ""} // Try compressed first, then raw

	f.debugf("Attempting to download file: %s (will try extensions: %v)", filename, extensionsToTry)

	var lastErr error
	for _, ext := range extensionsToTry {
		remoteFilename := filename + ext
		f.debugf("Trying to download: %s", remoteFilename)

		resp, retrErr := f.client.Retr(remoteFilename)

		if retrErr == nil {
			// Success! Wrap the response reader with decompression.
			f.debugf("Successfully downloaded file: %s", remoteFilename)
			// The caller must close the returned reader, which will close the underlying FTP data connection.
			decompressedStream := decompressStream(resp, remoteFilename) // Handles decompression based on remoteFilename extension
			// We need to ensure the underlying *ftp.Response is closed when the decompressed stream is closed.
			// decompressStream returns an io.ReadCloser, so this should work.
			return decompressedStream, nil
		}

		// Handle error
		f.debugf("Failed to download %s: %v", remoteFilename, retrErr)
		lastErr = fmt.Errorf("failed attempt to download %s from ftp host %s: %w", remoteFilename, f.host, retrErr)

		if strings.Contains(retrErr.Error(), "550") || strings.Contains(strings.ToLower(retrErr.Error()), ftp.StatusText(ftp.StatusFileUnavailable)) {
			f.debugf("File %s not found (550), trying next extension", remoteFilename)
			continue
		}

		// If it's not a recognized "not found" error, return it immediately
		f.debugf("Encountered non-404 error for %s: %v", remoteFilename, retrErr)
		return nil, lastErr
	}

	// If we tried all extensions and none worked, return the last error encountered
	f.debugf("All download attempts failed for %s with extensions %v", filename, extensionsToTry)
	return nil, fmt.Errorf("file %s not found on ftp host %s with extensions %v: %w", filename, f.host, extensionsToTry, lastErr)
}

// List returns a list of filenames in the FTP server's current directory matching the prefix.
// Note: This lists based on the *current working directory* on the FTP server.
// It might be necessary to change directory (`Cwd`) before listing if dumps are in subdirs.
// FTP LIST command output parsing can be fragile.
func (f *FTPStorage) List(prefix string, recursive bool) ([]string, error) {
	var matchingFiles []string

	f.debugf("Listing files with prefix: %s (recursive: %v)", prefix, recursive)

	// Get current working directory
	cwd, err := f.client.CurrentDir()
	if err != nil {
		f.debugf("Failed to get current directory: %v", err)
		return nil, fmt.Errorf("failed to get current directory: %w", err)
	}
	f.debugf("Current directory: %s", cwd)

	// Change to prefix directory if it's a path
	prefixDir := filepath.Dir(prefix)
	if prefixDir != "." {
		f.debugf("Changing to directory: %s", prefixDir)
		if err := f.client.ChangeDir(prefixDir); err != nil {
			f.debugf("Failed to change to directory %s: %v", prefixDir, err)
			// Try to create the directory structure if it doesn't exist
			if strings.Contains(err.Error(), "550") {
				f.debugf("Directory %s not found, attempting to create", prefixDir)
				// Create directory structure
				parts := strings.Split(strings.TrimPrefix(prefixDir, "/"), "/")
				currentPath := ""

				for _, part := range parts {
					if part == "" {
						continue
					}

					if currentPath != "" {
						currentPath += "/"
					}
					currentPath += part

					f.debugf("Checking directory: %s", currentPath)
					entries, listErr := f.client.List(currentPath)
					if listErr != nil || len(entries) == 0 {
						f.debugf("Creating directory: %s", currentPath)
						mkdirErr := f.client.MakeDir(currentPath)
						if mkdirErr != nil {
							f.debugf("Failed to create directory %s: %v", currentPath, mkdirErr)
						}
					}
				}

				// Try changing to the directory again
				if err := f.client.ChangeDir(prefixDir); err != nil {
					f.debugf("Still failed to change to directory %s after creation attempt: %v", prefixDir, err)
					return nil, fmt.Errorf("failed to change to directory %s: %w", prefixDir, err)
				}
			} else {
				return nil, fmt.Errorf("failed to change to directory %s: %w", prefixDir, err)
			}
		}
		defer func() {
			f.debugf("Restoring original directory: %s", cwd)
			if err := f.client.ChangeDir(cwd); err != nil {
				f.debugf("Failed to restore original directory %s: %v", cwd, err)
			}
		}()
	}

	f.debugf("Listing entries in current directory")
	entries, err := f.client.List(".")
	if err != nil {
		f.debugf("Failed to list files: %v", err)
		return nil, fmt.Errorf("failed to list files: %w", err)
	}
	f.debugf("Found %d entries", len(entries))

	for _, entry := range entries {
		if entry.Type == ftp.EntryTypeFile {
			fullPath := filepath.Join(prefixDir, entry.Name)
			f.debugf("Checking file: %s against prefix: %s", fullPath, prefix)
			if strings.HasPrefix(fullPath, prefix) {
				f.debugf("Adding matching file: %s", fullPath)
				matchingFiles = append(matchingFiles, fullPath)
			}
		} else if recursive && entry.Type == ftp.EntryTypeFolder && entry.Name != "." && entry.Name != ".." {
			f.debugf("Recursively listing subdirectory: %s", entry.Name)
			subPrefix := filepath.Join(prefixDir, entry.Name)
			if prefix != "" {
				subPrefix = filepath.Join(prefix, entry.Name)
			}
			subFiles, err := f.List(subPrefix, true)
			if err != nil {
				f.debugf("Error listing subdirectory %s: %v", entry.Name, err)
				return nil, err
			}
			f.debugf("Adding %d files from subdirectory %s", len(subFiles), entry.Name)
			matchingFiles = append(matchingFiles, subFiles...)
		}
	}

	f.debugf("Found %d matching files for prefix: %s", len(matchingFiles), prefix)
	return matchingFiles, nil
}

// Close closes the FTP connection.
func (f *FTPStorage) Close() error {
	if f.client != nil {
		f.debugf("Closing FTP connection to %s", f.host)
		err := f.client.Quit()
		if err != nil {
			f.debugf("Error closing FTP connection: %v", err)
		} else {
			f.debugf("FTP connection closed successfully")
		}
		return err
	}
	f.debugf("No FTP client to close")
	return nil
}
