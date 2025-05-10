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
	conn   *ssh.Client // Keep SSH connection to close it
	host   string      // Store host for logging
	user   string
	debug  bool // Debug flag
}

func (s *SFTPStorage) debugf(format string, args ...interface{}) {
	if s.debug {
		log.Printf("[sftp:debug] "+format, args...)
	}
}

// NewSFTPStorage creates a new SFTP storage client.
func NewSFTPStorage(host, user, password string, debug bool) (*SFTPStorage, error) {
	if host == "" || user == "" { // Password might be empty if using key auth (not implemented here)
		return nil, fmt.Errorf("sftp host and user cannot be empty")
	}

	// Add default port if not specified
	if !strings.Contains(host, ":") {
		host = net.JoinHostPort(host, "22")
	}

	// Configure SSH client
	// WARNING: InsecureIgnoreHostKey is insecure! Use known_hosts in production.
	sshConfig := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
			// TODO: Add support for key-based authentication
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // FIXME: Not secure for production
		Timeout:         10 * time.Second,            // Add connection timeout
	}

	// Dial SSH connection
	conn, err := ssh.Dial("tcp", host, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial ssh for sftp host %s: %w", host, err)
	}

	// Create SFTP client from SSH connection
	client, err := sftp.NewClient(conn)
	if err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			log.Printf("can't close sftp connection: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to create sftp client for host %s: %w", host, err)
	}

	s := &SFTPStorage{
		client: client,
		conn:   conn,
		host:   host,
		user:   user,
		debug:  debug,
	}

	if debug {
		log.Printf("[sftp:debug] Connected to SFTP server %s as user %s", host, user)
	}

	return s, nil
}

// Upload compresses and uploads data to the specified filename (filename + compression extension) via SFTP.
func (s *SFTPStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	compressedReader, ext := compressStream(reader, format, level)
	remoteFilename := filename + ext

	// Create the remote file
	dstFile, err := s.client.Create(remoteFilename)
	if err != nil {
		// Check if directory needs to be created
		if os.IsNotExist(err) || strings.Contains(err.Error(), "no such file") { // Error messages vary
			// Attempt to create parent directory
			parentDir := filepath.Dir(remoteFilename)
			if parentDir != "." && parentDir != "/" {
				// Ensure parent directory exists
				log.Printf("Creating SFTP directory: %s", parentDir)
				if mkdirErr := s.client.MkdirAll(parentDir); mkdirErr != nil {
					// Try to create each directory in the path separately
					// This helps with some SFTP servers that have permission restrictions
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
						_ = s.client.Mkdir(currentPath)
					}
					// Try again after attempting to create directories
					dstFile, err = s.client.Create(remoteFilename)
					if err != nil {
						return fmt.Errorf("failed to create remote directory %s for sftp upload on %s: %w", parentDir, s.host, mkdirErr)
					}
				} else {
					// Retry creating the file
					dstFile, err = s.client.Create(remoteFilename)
				}
			}
		}
		// If still error after potential mkdir
		if err != nil {
			return fmt.Errorf("failed to create remote file %s for sftp upload on %s: %w", remoteFilename, s.host, err)
		}
	}
	defer dstFile.Close() // Ensure file is closed

	// Copy data to the remote file
	_, err = io.Copy(dstFile, compressedReader)
	if err != nil {
		return fmt.Errorf("failed to copy data to remote file %s via sftp on %s: %w", remoteFilename, s.host, err)
	}

	// Close is important to finalize write
	err = dstFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close remote file %s after sftp upload on %s: %w", remoteFilename, s.host, err)
	}

	return nil
}

// Download retrieves a file from SFTP and returns a reader for its decompressed content.
func (s *SFTPStorage) Download(filename string) (io.ReadCloser, error) {
	file, err := s.client.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s from sftp host %s: %w", filename, s.host, err)
	}

	// Success! Wrap the file reader with decompression.
	// The caller must close the returned reader, which will close the underlying sftp.File.
	return decompressStream(file, filename), nil
}

// List returns a list of filenames in the SFTP server matching the prefix.
// If recursive is true, it will list all files under the prefix recursively.
func (s *SFTPStorage) List(prefix string, recursive bool) ([]string, error) {
	var matchingFiles []string

	walker := s.client.Walk(".")
	for walker.Step() {
		if err := walker.Err(); err != nil {
			return nil, fmt.Errorf("error walking sftp path: %w", err)
		}

		path := walker.Path()
		if !strings.HasPrefix(path, prefix) {
			if walker.Stat().IsDir() {
				walker.SkipDir() // Skip directories that don't match prefix
			}
			continue
		}

		if !walker.Stat().IsDir() {
			if recursive || filepath.Dir(path) == filepath.Dir(prefix) {
				matchingFiles = append(matchingFiles, path)
			}
		}
	}

	return matchingFiles, nil
}

// Close closes the SFTP client and the underlying SSH connection.
func (s *SFTPStorage) Close() error {
	var firstErr error
	if s.client != nil {
		err := s.client.Close()
		if err != nil {
			firstErr = fmt.Errorf("failed to close sftp client: %w", err)
		}
	}
	if s.conn != nil {
		err := s.conn.Close()
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close ssh connection: %w", err)
		}
	}
	return firstErr
}
