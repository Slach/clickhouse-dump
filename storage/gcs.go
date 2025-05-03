package storage

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type GCSStorage struct {
	bucket *storage.BucketHandle
}

func NewGCSStorage(bucketName string) (*GCSStorage, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &GCSStorage{
		bucket: client.Bucket(bucketName),
	}, nil
}

func (g *GCSStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	ctx := context.Background()
	compressedReader, ext := compressStream(reader, format, level)
	writer := g.bucket.Object(filename + ext).NewWriter(ctx)
	_, err := io.Copy(writer, compressedReader)
	if err != nil {
		return err
	}
	return writer.Close()
}

func (g *GCSStorage) Download(filename string) (io.ReadCloser, error) {
	ctx := context.Background()
	reader, err := g.bucket.Object(filename + ".gz").NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(decompressStream(reader, filename+".gz")), nil
}
