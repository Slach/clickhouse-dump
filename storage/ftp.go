package storage

import (
	"io"

	"github.com/jlaffaye/ftp"
)

type FTPStorage struct {
	client *ftp.ServerConn
}

func NewFTPStorage(host, user, password string) (*FTPStorage, error) {
	c, err := ftp.Dial(host)
	if err != nil {
		return nil, err
	}

	err = c.Login(user, password)
	if err != nil {
		return nil, err
	}

	return &FTPStorage{
		client: c,
	}, nil
}

func (f *FTPStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	compressedReader, ext := compressStream(reader, format, level)
	return f.client.Stor(filename+ext, compressedReader)
}

func (f *FTPStorage) Download(filename string) (io.ReadCloser, error) {
	resp, err := f.client.Retr(filename + ".gz")
	if err != nil {
		return nil, err
	}
	return io.NopCloser(decompressStream(resp, filename+".gz")), nil
}
