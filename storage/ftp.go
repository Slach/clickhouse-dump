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

// FTPStorage stores connection details and provides methods for FTP operations
type FTPStorage struct {
	client   *ftp.ServerConn
	host     string // Store host for logging/reconnect
	user     string
	password string // Store password for reconnection
	debug    bool   // Debug flag
}

func (f *FTPStorage) debugf(format string, args ...interface{}) {
	if f.debug {
		log.Printf("[ftp:debug] "+format, args...)
	}
}

// reconnect closes the current connection and establishes a new one
func (f *FTPStorage) reconnect() error {
	// Close existing connection if any
	if f.client != nil {
		f.debugf("Closing existing FTP connection before reconnect")
		_ = f.client.Quit() // Ignore errors on close
	}

	f.debugf("Reconnecting to FTP server %s with user %s", f.host, f.user)

	// Dial with timeout
	c, err := ftp.Dial(f.host, ftp.DialWithTimeout(10*time.Second))
	if err != nil {
		f.debugf("Failed to dial FTP host %s during reconnect: %v", f.host, err)
		return fmt.Errorf("failed to dial ftp host %s during reconnect: %w", f.host, err)
	}

	// Login with timeout
	err = c.Login(f.user, f.password)
	if err != nil {
		f.debugf("Failed to login to FTP host %s with user %s during reconnect: %v", f.host, f.user, err)
		if quitErr := c.Quit(); quitErr != nil {
			f.debugf("Can't quit FTP connection after failed login: %v", quitErr)
		}
		return fmt.Errorf("failed to login to ftp host %s with user %s during reconnect: %w", f.host, f.user, err)
	}

	f.debugf("Successfully reconnected to FTP server %s", f.host)
	f.client = c
	return nil
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
		if quitErr := c.Quit(); quitErr != nil {
			log.Printf("can't ftp quit error: %v", quitErr)
		}
		return nil, fmt.Errorf("failed to login to ftp host %s with user %s: %w", host, user, err)
	}

	if debug {
		log.Printf("[ftp:debug] Successfully logged in to FTP server %s with user %s", host, user)
	}

	return &FTPStorage{
		client:   c,
		host:     host,
		user:     user,
		password: password, // Store password for reconnection
		debug:    debug,
	}, nil
}

// Upload uploads data via FTP.
// If contentEncoding is provided, it's assumed data is pre-compressed.
// Otherwise, compressFormat and compressLevel are used for client-side compression.
func (f *FTPStorage) Upload(filename string, reader io.Reader, compressFormat string, compressLevel int, contentEncoding string) error {
	remoteFilename := filename
	var finalReader = reader

	if contentEncoding != "" {
		f.debugf("FTP Upload: pre-compressed data with contentEncoding: %s for remote file %s", contentEncoding, remoteFilename)
		switch strings.ToLower(contentEncoding) {
		case "gzip":
			remoteFilename += ".gz"
		case "zstd":
			remoteFilename += ".zstd"
		default:
			f.debugf("FTP Upload: unknown contentEncoding '%s' for remote file %s, uploading as is", contentEncoding, remoteFilename)
		}
	} else if compressFormat != "" && compressFormat != "none" {
		f.debugf("FTP Upload: compressing data with format: %s, level: %d for remote file %s", compressFormat, compressLevel, remoteFilename)
		var ext string
		finalReader, ext = compressStream(reader, compressFormat, compressLevel)
		remoteFilename += ext
	} else {
		f.debugf("FTP Upload: uploading data uncompressed for remote file %s", remoteFilename)
	}

	f.debugf("FTP Upload: final remote path: %s", remoteFilename)

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
	err := f.client.Stor(remoteFilename, finalReader)
	if err != nil {
		f.debugf("Failed to upload %s: %v", remoteFilename, err)
		return fmt.Errorf("failed to upload %s to ftp host %s: %w", remoteFilename, f.host, err)
	}

	f.debugf("Successfully uploaded file: %s", remoteFilename)
	return nil
}

// Download retrieves a file from FTP.
// If noClientDecompression is true, the raw file stream is returned.
// Otherwise, decompressStream is used based on the filename's extension.
func (f *FTPStorage) Download(filename string) (io.ReadCloser, error) {
	f.debugf("attempting to download file: %s", filename)
	// TODO: Implement retry for .gz, .zst if filename not found, if desired.
	// For now, assumes `filename` is the exact remote path.

	// Try direct download
	resp, err := f.client.Retr(filename)
	if err == nil {
		f.debugf("Successfully retrieved response for file: %s", filename)
		return decompressStream(resp, filename), nil
	}

	f.debugf("Failed to download %s directly: %v", filename, err)

	// Check for passive mode responses - these are not errors
	if strings.Contains(err.Error(), "229") || // Extended Passive Mode
		strings.Contains(err.Error(), "227") { // Standard Passive Mode

		// Reconnect and try again
		f.debugf("Received passive mode response, reconnecting and retrying")
		if reconnectErr := f.reconnect(); reconnectErr != nil {
			f.debugf("Failed to reconnect: %v", reconnectErr)
			return nil, fmt.Errorf("failed to download %s: %w", filename, err)
		}

		// Try again after reconnection
		resp, retryErr := f.client.Retr(filename)
		if retryErr == nil {
			f.debugf("Successfully downloaded file after reconnection: %s", filename)
			return decompressStream(resp, filename), nil
		}

		f.debugf("Failed to download %s after reconnection: %v", filename, retryErr)
		return nil, fmt.Errorf("failed to download %s after reconnection: %w", filename, retryErr)
	}

	return nil, fmt.Errorf("failed to download %s: %w", filename, err)
}

// List returns a list of filenames in the FTP server's current directory matching the prefix.
func (f *FTPStorage) List(prefix string, recursive bool) ([]string, error) {
	var matchingFiles []string

	f.debugf("Listing files with prefix: %s (recursive: %v)", prefix, recursive)

	// Normalize prefix - remove leading slash for FTP paths
	prefix = strings.TrimPrefix(prefix, "/")

	// Use Walk to recursively list all files
	walker := f.client.Walk(prefix)
	for walker.Next() {
		if err := walker.Err(); err != nil {
			f.debugf("Error walking path: %v", err)
			continue // Skip errors and continue
		}

		entry := walker.Stat()
		// Skip directories if we only want files
		if entry.Type == ftp.EntryTypeFile {
			f.debugf("Adding file: %s", walker.Path())
			matchingFiles = append(matchingFiles, walker.Path())
		}
	}

	f.debugf("Found %d matching files", len(matchingFiles))
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
