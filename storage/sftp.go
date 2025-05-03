package storage

import (
	"io"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPStorage struct {
	client *sftp.Client
}

func NewSFTPStorage(host, user, password string) (*SFTPStorage, error) {
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", host, config)
	if err != nil {
		return nil, err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}

	return &SFTPStorage{
		client: client,
	}, nil
}

func (s *SFTPStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	compressedReader, ext := compressStream(reader, format, level)
	dstFile, err := s.client.Create(filename + ext)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, compressedReader)
	return err
}

func (s *SFTPStorage) Download(filename string) (io.ReadCloser, error) {
	file, err := s.client.Open(filename + ".gz")
	if err != nil {
		return nil, err
	}
	return io.NopCloser(decompressStream(file, filename+".gz")), nil
}
