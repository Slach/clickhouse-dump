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

func NewS3Storage(bucket, region, accessKey, secretKey, endpoint string, debug bool) (*S3Storage, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	// Use explicit credentials if provided
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(aws.CredentialsProviderFunc(
			func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretKey,
				}, nil
			})))
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return nil, err
	}

	clientOpts := []func(*s3.Options){}
	if endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // Needed for MinIO and other S3-compatible services
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)

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

func (s *S3Storage) List(prefix string) ([]string, error) {
	ctx := context.Background()
	var objectNames []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			objectNames = append(objectNames, *obj.Key)
		}
	}

	return objectNames, nil
}

func (s *S3Storage) Close() error {
	// AWS SDK v2 clients don't need explicit closing
	return nil
}
