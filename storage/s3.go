package storage

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Storage struct {
	bucket     string
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
}

func NewS3Storage(bucket, region string) (*S3Storage, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)

	return &S3Storage{
		bucket:     bucket,
		client:     client,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
	}, nil
}

func (s *S3Storage) Upload(filename string, reader io.Reader, format string, level int) error {
	compressedReader, ext := compressStream(reader, format, level)
	_, err := s.uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filename + ext),
		Body:   compressedReader,
	})
	return err
}

func (s *S3Storage) Download(filename string) (io.ReadCloser, error) {
	writer, err := os.CreateTemp(filename, strings.ReplaceAll(filename, "/", "_"))
	if err != nil {
		return nil, err
	}
	_, err = s.downloader.Download(context.TODO(), writer, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		return nil, err
	}
	return io.NopCloser(decompressStream(writer, filename)), nil
}
