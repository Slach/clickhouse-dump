package storage

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPStorage struct {
	client *sftp.Client
	conn   *ssh.Client
	host   string
	user   string
	debug  bool
}

func (s *SFTPStorage) debugf(format string, args ...interface{}) {
	if s.debug {
		log.Printf("[sftp:debug] "+format, args...)
	}
}

// NewSFTPStorage creates a new SFTP storage client.
func NewSFTPStorage(host, user, password string, debug bool) (*SFTPStorage, error) {
	s := &SFTPStorage{
		host:  host,
		user:  user,
		debug: debug,
	}

	s.debugf("Initializing SFTP storage with host=%s, user=%s", host, user)

	if host == "" || user == "" { // Password might be empty if using key auth (not implemented here)
		return nil, fmt.Errorf("sftp host and user cannot be empty")
	}

	// Add default port if not specified
	if !strings.Contains(host, ":") {
		s.debugf("No port specified, using default port 22")
		host = net.JoinHostPort(host, "22")
	}

	// Configure SSH client
	// WARNING: InsecureIgnoreHostKey is insecure! Use known_hosts in production.
	s.debugf("Configuring SSH client with timeout of 10 seconds")
	sshConfig := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
			// TODO: Add support for key-based authentication
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // FIXME: Not secure for production
		Timeout:         10 * time.Second,
	}

	// Dial SSH connection
	s.debugf("Attempting to establish SSH connection to %s", host)
	conn, err := ssh.Dial("tcp", host, sshConfig)
	if err != nil {
		s.debugf("Failed to establish SSH connection: %v", err)
		return nil, fmt.Errorf("failed to dial ssh for sftp host %s: %w", host, err)
	}
	s.debugf("SSH connection established successfully")

	// Create SFTP client from SSH connection
	s.debugf("Creating SFTP client from SSH connection")
	client, err := sftp.NewClient(conn)
	if err != nil {
		s.debugf("Failed to create SFTP client: %v", err)
		if closeErr := conn.Close(); closeErr != nil {
			s.debugf("Failed to close SSH connection: %v", closeErr)
			log.Printf("can't close sftp connection: %v", closeErr)
		} else {
			s.debugf("SSH connection closed successfully")
		}
		return nil, fmt.Errorf("failed to create sftp client for host %s: %w", host, err)
	}
	s.debugf("SFTP client created successfully")

	s.client = client
	s.conn = conn

	s.debugf("Connected to SFTP server %s as user %s", host, user)

	return s, nil
}

// Upload uploads data via SFTP.
// If contentEncoding is provided, it's assumed data is pre-compressed.
// Otherwise, compressFormat and compressLevel are used for client-side compression.
func (s *SFTPStorage) Upload(filename string, reader io.Reader, compressFormat string, compressLevel int, contentEncoding string) error {
	remoteFilename := filename
	var finalReader = reader

	if contentEncoding != "" {
		s.debugf("SFTP Upload: pre-compressed data with contentEncoding: %s for remote file %s", contentEncoding, remoteFilename)
		switch strings.ToLower(contentEncoding) {
		case "gzip":
			remoteFilename += ".gz"
		case "zstd":
			remoteFilename += ".zstd"
		default:
			s.debugf("SFTP Upload: unknown contentEncoding '%s' for remote file %s, uploading as is", contentEncoding, remoteFilename)
		}
	} else if compressFormat != "" && compressFormat != "none" {
		s.debugf("SFTP Upload: compressing data with format: %s, level: %d for remote file %s", compressFormat, compressLevel, remoteFilename)
		var ext string
		finalReader, ext = compressStream(reader, compressFormat, compressLevel)
		remoteFilename += ext
	} else {
		s.debugf("SFTP Upload: uploading data uncompressed for remote file %s", remoteFilename)
	}

	s.debugf("SFTP Upload: final remote path: %s", remoteFilename)

	// Create the remote file
	s.debugf("Attempting to create remote file: %s", remoteFilename)
	dstFile, err := s.client.Create(remoteFilename)
	if err != nil {
		s.debugf("Failed to create remote file: %v", err)
		// Check if directory needs to be created
		if os.IsNotExist(err) || strings.Contains(err.Error(), "no such file") { // Error messages vary
			// Attempt to create parent directory
			parentDir := filepath.Dir(remoteFilename)
			if parentDir != "." && parentDir != "/" {
				// Ensure parent directory exists
				s.debugf("Creating SFTP directory: %s", parentDir)
				if mkdirErr := s.client.MkdirAll(parentDir); mkdirErr != nil {
					s.debugf("Failed to create directory with MkdirAll: %v", mkdirErr)
					// Try to create each directory in the path separately
					// This helps with some SFTP servers that have permission restrictions
					s.debugf("Attempting to create directories one by one")
					dirs := strings.Split(strings.Trim(parentDir, "/"), "/")
					currentPath := ""
					for _, dir := range dirs {
						if dir == "" {
							continue
						}
						if currentPath != "" {
							currentPath += "/"
						}
						currentPath += dir
						// Try to create, ignore errors if directory already exists
						s.debugf("Creating directory: %s", currentPath)
						if err := s.client.Mkdir(currentPath); err != nil {
							s.debugf("Directory creation returned: %v (may already exist)", err)
						}
					}
					// Try again after attempting to create directories
					s.debugf("Retrying file creation after directory creation")
					dstFile, err = s.client.Create(remoteFilename)
					if err != nil {
						s.debugf("Still failed to create file after directory creation: %v", err)
						return fmt.Errorf("failed to create remote directory %s for sftp upload on %s: %w", parentDir, s.host, mkdirErr)
					}
					s.debugf("File creation successful after directory creation")
				} else {
					// Retry creating the file
					s.debugf("Directory creation successful, retrying file creation")
					dstFile, err = s.client.Create(remoteFilename)
				}
			}
		}
		// If still error after potential mkdir
		if err != nil {
			s.debugf("Failed to create remote file after all attempts: %v", err)
			return fmt.Errorf("failed to create remote file %s for sftp upload on %s: %w", remoteFilename, s.host, err)
		}
	}
	s.debugf("Remote file created successfully")
	defer func() {
		if closeErr := dstFile.Close(); closeErr != nil {
			s.debugf("SFTP Upload: error closing remote file %s: %v", remoteFilename, closeErr)
		} else {
			s.debugf("SFTP Upload: remote file %s closed successfully after copy", remoteFilename)
		}
	}()

	// Copy data to the remote file
	s.debugf("Copying data to remote file: %s", remoteFilename)
	bytesWritten, err := io.Copy(dstFile, finalReader)
	if err != nil {
		s.debugf("Failed to copy data to remote file %s: %v", remoteFilename, err)
		return fmt.Errorf("failed to copy data to remote file %s via sftp on %s: %w", remoteFilename, s.host, err)
	}
	s.debugf("Successfully copied %d bytes to remote file %s", bytesWritten, remoteFilename)

	// dstFile.Close() is handled by defer, but an explicit close here ensures it happens before function returns.
	// However, defer is generally safer for all exit paths. The current defer is fine.
	return nil
}

// Download retrieves a file from SFTP.
// If noClientDecompression is true, the raw file stream is returned.
// Otherwise, decompressStream is used based on the filename's extension.
func (s *SFTPStorage) Download(filename string, noClientDecompression bool) (io.ReadCloser, error) {
	s.debugf("SFTP Download: attempting to download file: %s (noClientDecompression: %t)", filename, noClientDecompression)
	// TODO: Implement retry for .gz, .zst if filename not found, if desired.
	// For now, assumes `filename` is the exact remote path.

	file, err := s.client.Open(filename)
	if err != nil {
		s.debugf("Failed to open file for download: %v", err)
		return nil, fmt.Errorf("failed to download %s from sftp host %s: %w", filename, s.host, err)
	}
	s.debugf("File opened successfully for download")

	s.debugf("File opened successfully for download")

	if noClientDecompression {
		s.debugf("SFTP Download: client-side decompression disabled for file %s", filename)
		return file, nil // file is an sftp.File which is an io.ReadCloser
	}

	// Attempt to decompress. `decompressStream` uses the filename for extension detection.
	s.debugf("SFTP Download: attempting client-side decompression for file: %s", filename)
	return decompressStream(file, filename), nil
}

// List returns a list of filenames in the SFTP server matching the prefix.
// If recursive is true, it will list all files under the prefix recursively.
func (s *SFTPStorage) List(prefix string, recursive bool) ([]string, error) {
	s.debugf("Listing files with prefix: %s (recursive: %v)", prefix, recursive)

	var matchingFiles []string

	// Define the starting path for traversal
	startPath := "."
	if prefix != "" {
		// If prefix is specified, start from its directory
		prefixDir := filepath.Dir(prefix)
		if prefixDir != "." {
			startPath = prefixDir
		}
	}

	s.debugf("Starting SFTP walk from directory: %s", startPath)

	// Check if the starting path exists
	_, err := s.client.Stat(startPath)
	if err != nil {
		s.debugf("Start path does not exist: %s, error: %v", startPath, err)
		// If the path doesn't exist, return an empty list
		return []string{}, nil
	}

	// Start traversal from the specified path
	walker := s.client.Walk(startPath)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			s.debugf("Error walking SFTP path: %v", err)
			return nil, fmt.Errorf("error walking sftp path: %w", err)
		}

		path := walker.Path()
		s.debugf("Examining path: %s", path)

		// Check if the path matches the prefix
		if prefix != "" && !strings.HasPrefix(path, prefix) {
			if walker.Stat().IsDir() {
				// If this is a directory and it doesn't match the prefix,
				// check if it might contain files with the needed prefix
				if !strings.HasPrefix(prefix, path+"/") {
					s.debugf("Skipping directory that doesn't match prefix: %s", path)
					walker.SkipDir()
				} else {
					s.debugf("Entering directory that might contain matching files: %s", path)
				}
			}
			continue
		}

		if !walker.Stat().IsDir() {
			// For files, check if they match the recursion conditions
			if recursive || filepath.Dir(path) == filepath.Dir(prefix) || prefix == "" {
				s.debugf("Found matching file: %s", path)
				matchingFiles = append(matchingFiles, path)
			}
		} else {
			s.debugf("Found directory: %s", path)
		}
	}

	s.debugf("Found %d matching files", len(matchingFiles))
	return matchingFiles, nil
}

// Close closes the SFTP client and the underlying SSH connection.
func (s *SFTPStorage) Close() error {
	s.debugf("Closing SFTP storage connections")

	var firstErr error
	if s.client != nil {
		s.debugf("Closing SFTP client")
		err := s.client.Close()
		if err != nil {
			s.debugf("Failed to close SFTP client: %v", err)
			firstErr = fmt.Errorf("failed to close sftp client: %w", err)
		} else {
			s.debugf("SFTP client closed successfully")
		}
	} else {
		s.debugf("SFTP client was nil, nothing to close")
	}

	if s.conn != nil {
		s.debugf("Closing SSH connection")
		err := s.conn.Close()
		if err != nil {
			s.debugf("Failed to close SSH connection: %v", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to close ssh connection: %w", err)
			}
		} else {
			s.debugf("SSH connection closed successfully")
		}
	} else {
		s.debugf("SSH connection was nil, nothing to close")
	}

	return firstErr
}
