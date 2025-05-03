package storage

import (
	"context"
	"io"
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type AzBlobStorage struct {
	containerURL azblob.ContainerURL
}

func NewAzBlobStorage(accountName, accountKey, containerName string) (*AzBlobStorage, error) {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	URL, _ := url.Parse("https://" + accountName + ".blob.core.windows.net/" + containerName)
	containerURL := azblob.NewContainerURL(*URL, pipeline)

	return &AzBlobStorage{
		containerURL: containerURL,
	}, nil
}

func (a *AzBlobStorage) Upload(filename string, reader io.Reader, format string, level int) error {
	ctx := context.Background()
	compressedReader, ext := compressStream(reader, format, level)
	blobURL := a.containerURL.NewBlockBlobURL(filename + ext)
	_, err := azblob.UploadStreamToBlockBlob(ctx, compressedReader, blobURL, azblob.UploadStreamToBlockBlobOptions{})
	return err
}

func (a *AzBlobStorage) Download(filename string) (io.ReadCloser, error) {
	ctx := context.Background()
	blobURL := a.containerURL.NewBlockBlobURL(filename + ".gz")
	response, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return io.NopCloser(decompressStream(response.Body(azblob.RetryReaderOptions{}), filename+".gz")), nil
}
