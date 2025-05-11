package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	awsV2Logging "github.com/aws/smithy-go/logging"
)

type S3LogAdapter struct {
}

func newS3Logger() S3LogAdapter {
	return S3LogAdapter{}
}

func (apapter S3LogAdapter) Logf(severity awsV2Logging.Classification, msg string, args ...interface{}) {
	msg = fmt.Sprintf("[s3:%s] %s\n", severity, msg)
	log.Printf(msg, args...)
}

type S3Storage struct {
	bucket     string
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	debug      bool
}

func (s *S3Storage) IsDebug() bool {
	return s.debug
}

func NewS3Storage(bucket, region, accessKey, secretKey, endpoint string, debug bool) (*S3Storage, error) {
	// Check environment variable if debug wasn't explicitly set
	if !debug && os.Getenv("LOG_LEVEL") == "debug" {
		debug = true
	}
	if debug {
		log.Printf("Initializing S3 storage with bucket=%s, region=%s, endpoint=%s", bucket, region, endpoint)
	}
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

	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	// Configure debug logging if enabled
	if debug {
		cfg.Logger = newS3Logger()
		cfg.ClientLogMode = aws.LogRequest | aws.LogResponse | aws.LogRetries
	}

	clientOpts := []func(*s3.Options){}
	if endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // Needed for MinIO and other S3-compatible services
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)

	s3Storage := &S3Storage{
		bucket:     bucket,
		client:     client,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
		debug:      debug,
	}

	if debug {
		log.Printf("S3 storage initialized successfully")
	}
	return s3Storage, nil
}

func (s *S3Storage) Upload(filename string, reader io.Reader, format string, level int) error {
	compressedReader, ext := compressStream(reader, format, level)
	s3Key := strings.TrimPrefix(filename, "/") + ext
	_, err := s.uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Key),
		Body:   compressedReader,
	})
	return err
}

func (s *S3Storage) UploadWithExtension(filename string, reader io.Reader, contentEncoding string) error {
	var ext string
	switch strings.ToLower(contentEncoding) {
	case "gzip":
		ext = ".gz"
	case "zstd":
		ext = ".zstd"
	}
	
	s3Key := strings.TrimPrefix(filename, "/") + ext
	
	// Set the appropriate Content-Encoding for the object
	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Key),
		Body:   reader,
	}
	
	if contentEncoding != "" {
		uploadInput.ContentEncoding = aws.String(contentEncoding)
	}
	
	_, err := s.uploader.Upload(context.Background(), uploadInput)
	return err
}

// tempFileCloser wraps an io.ReadCloser (usually *os.File)
// and ensures the temporary file is deleted when Close is called.
type tempFileCloser struct {
	io.ReadCloser
	name string // Path to temporary file
}

// Close closes the underlying ReadCloser and then deletes the temporary file.
func (tfc *tempFileCloser) Close() error {
	closeErr := tfc.ReadCloser.Close()
	removeErr := os.Remove(tfc.name)
	if closeErr != nil {
		// If closing the reader failed, return this error.
		// Log the removal error if it also occurred.
		if removeErr != nil {
			log.Printf("also failed to remove temporary file %s: %v", tfc.name, removeErr)
		}
		return closeErr
	}
	// If closing was successful, return the removal error (if any).
	return removeErr
}

func (s *S3Storage) Download(filename string) (io.ReadCloser, error) {
	s3Key := strings.TrimPrefix(filename, "/")

	// Create a temporary file for download
	tempFile, err := os.CreateTemp("", "s3-download-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}

	// Try to download
	_, err = s.downloader.Download(context.Background(), tempFile, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Key),
	})

	if err == nil {
		// Download successful
		// Move the pointer to the beginning of the file to read its contents
		if _, seekErr := tempFile.Seek(0, io.SeekStart); seekErr != nil {
			_ = tempFile.Close()           // Try to close
			_ = os.Remove(tempFile.Name()) // Try to delete
			return nil, fmt.Errorf("failed to seek temporary file: %w", seekErr)
		}

		// Wrap the file in a decompressor and custom closer to delete the temporary file
		decompressedStream := decompressStream(tempFile, s3Key) // tempFile is an io.ReadCloser
		return &tempFileCloser{ReadCloser: decompressedStream, name: tempFile.Name()}, nil
	}

	// Download failed, clean up the current tempFile
	_ = tempFile.Close()
	_ = os.Remove(tempFile.Name()) // Delete the (likely empty or partially written) temporary file

	// Check if the error is NoSuchKey (or equivalent)
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return nil, fmt.Errorf("object %s not found in S3: %w", s3Key, err)
	}

	// Для других ошибок S3 возвращаем немедленно
	return nil, fmt.Errorf("failed to download %s from S3: %w", s3Key, err)
}

func (s *S3Storage) List(prefix string, recursive bool) ([]string, error) {
	ctx := context.Background()
	var objectNames []string

	s3Prefix := strings.TrimPrefix(prefix, "/")
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(s3Prefix),
		Delimiter: aws.String("/"),
	}

	if recursive {
		input.Delimiter = nil // Remove delimiter for recursive listing
	}

	paginator := s3.NewListObjectsV2Paginator(s.client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		// Add objects
		for _, obj := range page.Contents {
			objectNames = append(objectNames, *obj.Key)
		}

		// For non-recursive, add common prefixes (subdirectories)
		if !recursive {
			for _, cp := range page.CommonPrefixes {
				objectNames = append(objectNames, *cp.Prefix)
			}
		}
	}

	return objectNames, nil
}

func (s *S3Storage) Close() error {
	// AWS SDK v2 clients don't need explicit closing
	return nil
}
