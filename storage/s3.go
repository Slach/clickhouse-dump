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

func (s *S3Storage) debugf(format string, args ...interface{}) {
	if s.debug {
		log.Printf("[s3:debug] "+format, args...)
	}
}

func NewS3Storage(bucket, region, accessKey, secretKey, endpoint string, debug bool) (*S3Storage, error) {
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

	var clientOpts []func(*s3.Options)
	if endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // Needed for MinIO and other S3-compatible services
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)

	if debug {
		log.Printf("S3 storage initialized successfully")
	}
	return &S3Storage{
		bucket:     bucket,
		client:     client,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
		debug:      debug,
	}, nil
}

// Upload uploads data to S3.
// If contentEncoding is provided, it's assumed data is pre-compressed and ContentEncoding header is set.
// Otherwise, compressFormat and compressLevel are used for client-side compression.
func (s *S3Storage) Upload(filename string, reader io.Reader, compressFormat string, compressLevel int, contentEncoding string) error {
	s3Key := strings.TrimPrefix(filename, "/")
	var finalReader = reader
	var s3ObjectContentEncoding *string // For S3 metadata

	if contentEncoding != "" {
		s.debugf("S3 Upload: pre-compressed data with contentEncoding: %s for key %s", contentEncoding, s3Key)
		actualEncoding := strings.ToLower(contentEncoding)
		switch actualEncoding {
		case "gzip":
			s3Key += ".gz"
			s3ObjectContentEncoding = aws.String("gzip")
		case "zstd":
			s3Key += ".zstd"
			s3ObjectContentEncoding = aws.String("zstd") // S3 might not natively understand zstd for ContentEncoding, but we set it.
		default:
			s.debugf("S3 Upload: unknown contentEncoding '%s' for key %s, uploading as is", contentEncoding, s3Key)
			// s3Key remains unchanged, s3ObjectContentEncoding remains nil
		}
	} else if compressFormat != "" && compressFormat != "none" {
		s.debugf("S3 Upload: compressing data with format: %s, level: %d for key %s", compressFormat, compressLevel, s3Key)
		var ext string
		finalReader, ext = compressStream(reader, compressFormat, compressLevel)
		s3Key += ext
		// Set ContentEncoding based on client-side compression for S3 metadata
		switch strings.ToLower(compressFormat) {
		case "gzip":
			s3ObjectContentEncoding = aws.String("gzip")
		case "zstd":
			// As above, S3 might not use this, but good to set if we compressed it this way.
			s3ObjectContentEncoding = aws.String("zstd")
		}
	} else {
		s.debugf("S3 Upload: uploading data uncompressed for key %s", s3Key)
	}

	s.debugf("S3 Upload: final S3 key: %s", s3Key)
	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Key),
		Body:   finalReader,
	}
	if s3ObjectContentEncoding != nil {
		uploadInput.ContentEncoding = s3ObjectContentEncoding
		s.debugf("S3 Upload: setting ContentEncoding HTTP header to '%s' for key %s", *s3ObjectContentEncoding, s3Key)
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

// Download retrieves an object from S3.
// If noClientDecompression is true, the raw object stream is returned.
// Otherwise, decompressStream is used based on the filename's extension.
func (s *S3Storage) Download(filename string, noClientDecompression bool) (io.ReadCloser, error) {
	s3Key := strings.TrimPrefix(filename, "/")
	s.debugf("S3 Download: attempting to download key: %s (noClientDecompression: %t)", s3Key, noClientDecompression)

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
		var streamToReturn io.ReadCloser = tempFile // tempFile is an io.ReadCloser
		if !noClientDecompression {
			s.debugf("S3 Download: attempting client-side decompression for key %s (downloaded to %s)", s3Key, tempFile.Name())
			streamToReturn = decompressStream(tempFile, s3Key) // s3Key (original filename) is used for extension detection
		} else {
			s.debugf("S3 Download: client-side decompression disabled for key %s (downloaded to %s)", s3Key, tempFile.Name())
		}
		return &tempFileCloser{ReadCloser: streamToReturn, name: tempFile.Name()}, nil
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
