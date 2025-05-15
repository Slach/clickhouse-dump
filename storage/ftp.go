package storage

import (
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/secsy/goftp"
)

// ftpLoggerAdapter implements io.Writer for goftp.Config.Logger
type ftpLoggerAdapter struct {
	debug bool
}

func (l *ftpLoggerAdapter) Write(p []byte) (n int, err error) {
	if l.debug {
		log.Printf("[ftp] %s", strings.TrimSpace(string(p)))
	}
	return len(p), nil
}

// FTPStorage implements RemoteStorage for FTP servers.
type FTPStorage struct {
	client        *goftp.Client
	host          string
	user          string
	password      string
	debug         bool
	config        *goftp.Config
	dirCache      map[string]struct{}
	cacheDirMutex sync.RWMutex // Mutex for directory operations
}

func (f *FTPStorage) debugf(format string, args ...interface{}) {
	if f.debug {
		log.Printf("[ftp:debug] "+format, args...)
	}
}

// mkdirAllFTP ensures the full directory path exists on the FTP server.
// It creates directories recursively using a thread-safe cache to avoid redundant checks.
func (f *FTPStorage) mkdirAllFTP(path string) error {
	f.debugf("Ensuring directory structure for %s", path)
	isAbsolute := strings.HasPrefix(path, "/")
	trimmedPath := strings.Trim(filepath.ToSlash(filepath.Clean(path)), "/")

	if trimmedPath == "" || trimmedPath == "." {
		f.debugf("Path %s requires no directory creation.", path)
		return nil
	}

	parts := strings.Split(trimmedPath, "/")
	currentPathToMake := ""

	for i, part := range parts {
		if part == "" {
			continue
		}
		if i == 0 {
			if isAbsolute {
				currentPathToMake = "/" + part
			} else {
				currentPathToMake = part
			}
		} else {
			currentPathToMake = currentPathToMake + "/" + part
		}

		f.cacheDirMutex.RLock()
		if _, exists := f.dirCache[currentPathToMake]; exists {
			f.debugf("%s already created", currentPathToMake)
			f.cacheDirMutex.RUnlock()
			continue
		}

		f.cacheDirMutex.Lock()
		_, err := f.client.Mkdir(currentPathToMake)
		if err != nil {
			f.debugf("Directory creation error (likely exists): %v", err)
		} else {
			f.debugf("Successfully created directory: %s", currentPathToMake)
		}

		// Mark directory as created in cache regardless of error
		f.dirCache[currentPathToMake] = struct{}{}
		f.cacheDirMutex.Unlock()
	}
	return nil
}

// NewFTPStorage creates a new FTPStorage instance.
func NewFTPStorage(host, user, password string, debug bool) (*FTPStorage, error) {
	if host == "" || user == "" {
		return nil, fmt.Errorf("ftp host and user cannot be empty")
	}

	// Add default port if not specified
	if !strings.Contains(host, ":") {
		host = net.JoinHostPort(host, "21")
	}

	config := goftp.Config{
		User:     user,
		Password: password,
		Timeout:  10 * time.Second,
	}

	if debug {
		config.Logger = &ftpLoggerAdapter{debug: true}
		log.Printf("[ftp:debug] Connecting to FTP server %s with user %s", host, user)
	}

	client, err := goftp.DialConfig(config, host)
	if err != nil {
		if debug {
			log.Printf("[ftp:debug] Failed to connect to FTP server: %v", err)
		}
		return nil, fmt.Errorf("failed to connect to ftp host %s: %w", host, err)
	}

	if debug {
		log.Printf("[ftp:debug] Successfully connected to FTP server %s", host)
	}

	return &FTPStorage{
		client:   client,
		host:     host,
		user:     user,
		password: password,
		debug:    debug,
		config:   &config,
	}, nil
}

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
	}

	f.debugf("FTP Upload: final remote path: %s", remoteFilename)

	// Ensure parent directories exist
	dir := filepath.ToSlash(filepath.Dir(remoteFilename)) // Normalize to forward slashes

	if dir != "." && dir != "/" {
		f.debugf("Ensuring parent directories recursively for: %s", dir)
		if err := f.mkdirAllFTP(dir); err != nil {
			// mkdirAllFTP provides detailed error messages
			return fmt.Errorf("failed to ensure directory structure for %s on ftp host %s: %w", dir, f.host, err)
		}
	}

	// Store the file
	f.debugf("Storing file: %s", remoteFilename)
	if err := f.client.Store(remoteFilename, finalReader); err != nil {
		f.debugf("Failed to store file: %v", err)
		return fmt.Errorf("failed to store file %s on ftp host %s: %w", remoteFilename, f.host, err)
	}

	f.debugf("Successfully uploaded file: %s", remoteFilename)
	return nil
}

func (f *FTPStorage) Download(filename string) (io.ReadCloser, error) {
	f.debugf("attempting to download file: %s", filename)

	// Create a pipe to stream the download
	pr, pw := io.Pipe()

	go func() {
		defer func() {
			if closeErr := pw.Close(); closeErr != nil {
				log.Printf("can't close ftp pipe writer: %v", closeErr)
			}
		}()
		err := f.client.Retrieve(filename, pw)
		if err != nil {
			f.debugf("Failed to download file: %v", err)
			_ = pw.CloseWithError(fmt.Errorf("failed to download %s from ftp host %s: %w", filename, f.host, err))
		}
	}()

	return decompressStream(pr, filename), nil
}

func (f *FTPStorage) List(prefix string, recursive bool) ([]string, error) {
	var matchingFiles []string

	f.debugf("Listing files with prefix: %s (recursive: %v)", prefix, recursive)

	// Normalize prefix
	prefix = filepath.ToSlash(strings.TrimPrefix(prefix, "/"))
	if prefix == "." { // ReadDir(".") is fine, but if prefix was originally "/", it becomes "" after TrimPrefix, then "." by Clean.
		prefix = "" // For ReadDir, "" usually means current directory, similar to "."
	}

	// Get listing
	f.debugf("Reading directory: '%s'", prefix)
	entries, err := f.client.ReadDir(prefix)
	if err != nil {
		f.debugf("Error listing directory '%s': %v", prefix, err)
		return nil, fmt.Errorf("error listing ftp directory '%s' on host %s: %w", prefix, f.host, err)
	}

	for _, entry := range entries {
		// Construct path using forward slashes for FTP
		var path string
		if prefix == "" {
			path = entry.Name()
		} else {
			path = prefix + "/" + entry.Name()
		}

		if entry.IsDir() {
			if recursive {
				subFiles, err := f.List(path, recursive)
				if err != nil {
					return nil, err
				}
				matchingFiles = append(matchingFiles, subFiles...)
			}
		} else {
			matchingFiles = append(matchingFiles, path)
		}
	}

	f.debugf("Found %d matching files", len(matchingFiles))
	return matchingFiles, nil
}

func (f *FTPStorage) Close() error {
	if f.client != nil {
		f.debugf("Closing FTP connection to %s", f.host)
		err := f.client.Close()
		if err != nil {
			f.debugf("Error closing FTP connection: %v", err)
		} else {
			f.debugf("FTP connection closed successfully")
		}
		return err
	}
	return nil
}
