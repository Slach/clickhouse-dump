package storage

import (
	"io"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

type RemoteStorage interface {
	Upload(filename string, reader io.Reader, format string, level int) error
	Download(filename string) (io.ReadCloser, error)
}

func compressStream(reader io.Reader, format string, level int) (io.Reader, string) {
	switch format {
	case "gzip":
		pr, pw := io.Pipe()
		go func() {
			gw, _ := gzip.NewWriterLevel(pw, level)
			_, err := io.Copy(gw, reader)
			_ = gw.Close()
			_ = pw.CloseWithError(err)
		}()
		return pr, ".gz"
	case "zstd":
		pr, pw := io.Pipe()
		go func() {
			zw, _ := zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
			_, err := io.Copy(zw, reader)
			_ = zw.Close()
			_ = pw.CloseWithError(err)
		}()
		return pr, ".zstd"
	default:
		return reader, ""
	}
}

func decompressStream(reader io.Reader, filename string) io.Reader {
	ext := filepath.Ext(filename)
	switch strings.ToLower(ext) {
	case ".gz":
		gr, _ := gzip.NewReader(reader)
		return gr
	case ".zstd":
		zr, _ := zstd.NewReader(reader)
		return zr
	default:
		return reader
	}
}

