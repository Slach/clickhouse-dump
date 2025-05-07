package storage

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

type FTPStorage struct {
	client *ftp.ServerConn
	host   string // Store host for logging/reconnect?
	user   string
	// Password not stored for security
}

// NewFTPStorage creates a new FTP storage client.
func NewFTPStorage(host, user, password string) (*FTPStorage, error) {
	if host == "" || user == "" { // Password can potentially be empty for anonymous
		return nil, fmt.Errorf("ftp host and user cannot be empty")
	}

	// Add default port if not specified
	if !strings.Contains(host, ":") {
		host = net.JoinHostPort(host, "21")
	}

	// Dial with timeout
	c, err := ftp.Dial(host, ftp.DialWithTimeout(10*time.Second))
	if err != nil {
		return nil, fmt.Errorf("failed to dial ftp host %s: %w", host, err)
	}

	// Login with timeout
	err = c.Login(user, password)
	if err != nil {
		c.Quit() // Attempt to close connection on login failure
		return nil, fmt.Errorf("failed to login to ftp host %s with user %s: %w", host, user, err)
	}

	// Optional: Set passive mode? Often needed.
	// c.Pasv(true) // Or handle potential errors

	return &FTPStorage{
		client: c,
		host:   host,
		user:   user,
	}, nil
}

// Upload compresses and uploads data to the specified filename (filename + compression extension) via FTP.
func (f *FTPStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	compressedReader, ext := compressStream(reader, format, level)
	remoteFilename := filename + ext
	err := f.client.Stor(remoteFilename, compressedReader)
	if err != nil {
		return fmt.Errorf("failed to upload %s to ftp host %s: %w", remoteFilename, f.host, err)
	}
	return nil
}

// Download retrieves a file from FTP and returns a reader for its decompressed content.
// It tries common compression extensions (.gz, .zstd) if the base filename doesn't exist.
func (f *FTPStorage) Download(filename string) (io.ReadCloser, error) {
	extensionsToTry := []string{".gz", ".zstd", ""} // Try compressed first, then raw

	var lastErr error
	for _, ext := range extensionsToTry {
		remoteFilename := filename + ext
		resp, retrErr := f.client.Retr(remoteFilename)

		if retrErr == nil {
			// Success! Wrap the response reader with decompression.
			// The caller must close the returned reader, which will close the underlying FTP data connection.
			decompressedStream := decompressStream(resp, remoteFilename) // Handles decompression based on remoteFilename extension
			// We need to ensure the underlying *ftp.Response is closed when the decompressed stream is closed.
			// decompressStream returns an io.ReadCloser, so this should work.
			return decompressedStream, nil
		}

		// Handle error
		lastErr = fmt.Errorf("failed attempt to download %s from ftp host %s: %w", remoteFilename, f.host, retrErr)

		if strings.Contains(retrErr.Error(), "550") || strings.Contains(strings.ToLower(retrErr.Error()), ftp.StatusText(ftp.StatusFileUnavailable)) {
			continue
		}

		// If it's not a recognized "not found" error, return it immediately
		return nil, lastErr
	}

	// If we tried all extensions and none worked, return the last error encountered
	return nil, fmt.Errorf("file %s not found on ftp host %s with extensions %v: %w", filename, f.host, extensionsToTry, lastErr)
}

// List returns a list of filenames in the FTP server's current directory matching the prefix.
// Note: This lists based on the *current working directory* on the FTP server.
// It might be necessary to change directory (`Cwd`) before listing if dumps are in subdirs.
// FTP LIST command output parsing can be fragile.
func (f *FTPStorage) List(prefix string, recursive bool) ([]string, error) {
	var matchingFiles []string

	// Get current working directory
	cwd, err := f.client.CurrentDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get current directory: %w", err)
	}

	// Change to prefix directory if it's a path
	prefixDir := filepath.Dir(prefix)
	if prefixDir != "." {
		if err := f.client.ChangeDir(prefixDir); err != nil {
			return nil, fmt.Errorf("failed to change to directory %s: %w", prefixDir, err)
		}
		defer f.client.ChangeDir(cwd) // Restore original directory
	}

	entries, err := f.client.List(".")
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	for _, entry := range entries {
		if entry.Type == ftp.EntryTypeFile {
			fullPath := filepath.Join(prefixDir, entry.Name)
			if strings.HasPrefix(fullPath, prefix) {
				matchingFiles = append(matchingFiles, fullPath)
			}
		} else if recursive && entry.Type == ftp.EntryTypeFolder {
			// Recursively list subdirectories
			subFiles, err := f.List(filepath.Join(prefixDir, entry.Name, "*"), true)
			if err != nil {
				return nil, err
			}
			matchingFiles = append(matchingFiles, subFiles...)
		}
	}

	return matchingFiles, nil
}

// Close closes the FTP connection.
func (f *FTPStorage) Close() error {
	if f.client != nil {
		return f.client.Quit()
	}
	return nil
}
