package storage

import (
	"context"
	"fmt"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	cfg, err := config.LoadDefaultConfig(context.TODO(), opts...)
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
	_, err := s.uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Key),
		Body:   compressedReader,
	})
	return err
}

// tempFileCloser оборачивает io.ReadCloser (обычно *os.File)
// и гарантирует удаление временного файла при вызове Close.
type tempFileCloser struct {
	io.ReadCloser
	name string // Путь к временному файлу
}

// Close закрывает нижележащий ReadCloser, а затем удаляет временный файл.
func (tfc *tempFileCloser) Close() error {
	closeErr := tfc.ReadCloser.Close()
	removeErr := os.Remove(tfc.name)
	if closeErr != nil {
		// Если закрытие ридера не удалось, возвращаем эту ошибку.
		// Логгируем ошибку удаления, если она также произошла.
		if removeErr != nil {
			log.Printf("also failed to remove temporary file %s: %v", tfc.name, removeErr)
		}
		return closeErr
	}
	// Если закрытие прошло успешно, возвращаем ошибку удаления (если есть).
	return removeErr
}

func (s *S3Storage) Download(filename string) (io.ReadCloser, error) {
	extensionsToTry := []string{".gz", ".zstd", ""} // Сначала пробуем сжатые, потом сырой
	baseS3Key := strings.TrimPrefix(filename, "/")
	var lastErr error

	for _, ext := range extensionsToTry {
		s3Key := baseS3Key + ext

		// Создаем временный файл для загрузки
		tempFile, err := os.CreateTemp("", "s3-download-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temporary file: %w", err)
		}

		// Пытаемся загрузить
		_, err = s.downloader.Download(context.TODO(), tempFile, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s3Key),
		})

		if err == nil {
			// Загрузка успешна
			// Перемещаем указатель в начало файла для чтения его содержимого
			if _, seekErr := tempFile.Seek(0, io.SeekStart); seekErr != nil {
				_ = tempFile.Close()           // Пытаемся закрыть
				_ = os.Remove(tempFile.Name()) // Пытаемся удалить
				return nil, fmt.Errorf("failed to seek temporary file: %w", seekErr)
			}

			// Оборачиваем файл в декомпрессор и кастомный closer для удаления временного файла
			decompressedStream := decompressStream(tempFile, s3Key) // tempFile это io.ReadCloser
			return &tempFileCloser{ReadCloser: decompressedStream, name: tempFile.Name()}, nil
		}

		// Загрузка не удалась, очищаем текущий tempFile
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name()) // Удаляем (вероятно, пустой или частично записанный) временный файл

		// Проверяем, является ли ошибка NoSuchKey (или эквивалентной)
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			lastErr = fmt.Errorf("object %s not found in S3: %w", s3Key, err)
			continue // Пробуем следующее расширение
		}

		// Для других ошибок S3 возвращаем немедленно
		return nil, fmt.Errorf("failed to download %s from S3: %w", s3Key, err)
	}

	// Если все попытки не увенчались успехом (вероятно, все были NoSuchKey)
	if lastErr != nil {
		return nil, lastErr
	}
	// Эта ветка не должна достигаться, если extensionsToTry не пуст, но как запасной вариант:
	return nil, fmt.Errorf("object %s not found with any compression extension", baseS3Key)
}

func (s *S3Storage) List(prefix string, recursive bool) ([]string, error) {
	ctx := context.TODO()
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
