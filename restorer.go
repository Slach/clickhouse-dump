package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"path"
	"strings"
	"sync"

	"github.com/Slach/clickhouse-dump/storage"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

type Restorer struct {
	config  *Config
	client  *ClickHouseClient
	storage storage.RemoteStorage
}

// NewRestorer creates a new Restorer instance, initializing the necessary storage backend.
func NewRestorer(config *Config) (*Restorer, error) {
	var s storage.RemoteStorage
	var err error

	// Initialize storage based on config
	// Ensure StorageConfig is populated correctly from flags/env
	switch config.StorageType {
	case "file":
		s, err = storage.NewFileStorage(config.StorageConfig["path"], config.Debug)
	case "s3":
		s, err = storage.NewS3Storage(
			config.StorageConfig["bucket"],
			config.StorageConfig["region"],
			config.StorageConfig["account"],
			config.StorageConfig["key"],
			config.StorageConfig["endpoint"],
			config.Debug,
		)
	case "gcs":
		s, err = storage.NewGCSStorage(config.StorageConfig["bucket"], config.StorageConfig["endpoint"], config.StorageConfig["key"], config.Debug)
	case "azblob":
		s, err = storage.NewAzBlobStorage(config.StorageConfig["account"], config.StorageConfig["key"], config.StorageConfig["container"], config.StorageConfig["endpoint"], config.Debug)
	case "sftp":
		s, err = storage.NewSFTPStorage(config.StorageConfig["host"], config.StorageConfig["user"], config.StorageConfig["password"], config.Debug)
	case "ftp":
		s, err = storage.NewFTPStorage(config.StorageConfig["host"], config.StorageConfig["user"], config.StorageConfig["password"], config.Debug)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.StorageType)
	}

	if err != nil {
		return nil, err
	}

	return &Restorer{
		config:  config,
		client:  NewClickHouseClient(config),
		storage: s,
	}, nil
}

// Restore orchestrates the restoration process from remote storage.
func (r *Restorer) Restore() error {
	if r.storage == nil {
		return fmt.Errorf("restorer storage is not initialized")
	}
	// Ensure storage connection is closed eventually
	defer func() {
		if err := r.storage.Close(); err != nil {
			log.Printf("Warning: failed to close storage connection: %v", err)
		}
	}()

	// --- Restore Databases ---
	// Handle path joining properly - storage path may or may not end with /
	backupPrefix := path.Join(r.config.StorageConfig["path"], r.config.BackupName)
	log.Printf("Listing storage items with prefix: %s (recursive)", backupPrefix)

	files, err := r.storage.List(backupPrefix, true)
	if err != nil {
		return fmt.Errorf("failed to list files in storage with prefix %s: %w", backupPrefix, err)
	}

	// Filter for database files
	var dbFiles []string
	dbSuffix := "database.sql"
	for _, file := range files {
		if strings.Contains(file, dbSuffix) {
			dbFiles = append(dbFiles, file)
		}
	}

	log.Printf("Found %d database files to restore. Parallelism: %d", len(dbFiles), r.config.Parallel)
	if len(dbFiles) > 0 {
		semDb := make(chan struct{}, r.config.Parallel)
		var wgDb sync.WaitGroup
		errChanDb := make(chan error, len(dbFiles))

		for _, dbFile := range dbFiles {
			wgDb.Add(1)
			go func(dbf string) {
				defer wgDb.Done()
				semDb <- struct{}{}
				defer func() { <-semDb }()

				log.Printf("Restoring database from %s...", dbf)
				reader, downloadErr := r.storage.Download(dbf)
				if downloadErr != nil {
					errChanDb <- fmt.Errorf("failed to download database file %s: %w", dbf, downloadErr)
					return
				}
				// restoreSchema handles closing the reader
				if restoreErr := r.restoreSchema(reader); restoreErr != nil {
					errChanDb <- fmt.Errorf("failed to restore database from %s: %w", dbf, restoreErr)
					return
				}
				log.Printf("Successfully restored database from %s.", dbf)
			}(dbFile)
		}
		wgDb.Wait()
		close(errChanDb)

		var firstDbErr error
		for errItem := range errChanDb {
			if firstDbErr == nil {
				firstDbErr = errItem
			}
			log.Printf("Error during database restoration: %v", errItem)
		}
		if firstDbErr != nil {
			return fmt.Errorf("failed during database restoration: %w", firstDbErr)
		}
	}

	// --- Restore Tables (Schemas) ---
	var schemaFiles []string
	schemaSuffix := ".schema.sql"
	for _, file := range files {
		if strings.Contains(file, schemaSuffix) {
			schemaFiles = append(schemaFiles, file)
		}
	}

	log.Printf("Found %d schema files to restore. Parallelism: %d", len(schemaFiles), r.config.Parallel)
	if len(schemaFiles) > 0 {
		semSchema := make(chan struct{}, r.config.Parallel)
		var wgSchema sync.WaitGroup
		errChanSchema := make(chan error, len(schemaFiles))

		for _, schemaFile := range schemaFiles {
			wgSchema.Add(1)
			go func(sf string) {
				defer wgSchema.Done()
				semSchema <- struct{}{}
				defer func() { <-semSchema }()

				log.Printf("Restoring schema from %s...", sf)
				reader, downloadErr := r.storage.Download(sf)
				if downloadErr != nil {
					errChanSchema <- fmt.Errorf("failed to download schema file %s: %w", sf, downloadErr)
					return
				}
				// restoreSchema handles closing the reader
				if restoreErr := r.restoreSchema(reader); restoreErr != nil {
					errChanSchema <- fmt.Errorf("failed to restore schema from %s: %w", sf, restoreErr)
					return
				}
				log.Printf("Successfully restored schema from %s.", sf)
			}(schemaFile)
		}
		wgSchema.Wait()
		close(errChanSchema)

		var firstSchemaErr error
		for errItem := range errChanSchema {
			if firstSchemaErr == nil {
				firstSchemaErr = errItem
			}
			log.Printf("Error during schema restoration: %v", errItem)
		}
		if firstSchemaErr != nil {
			return fmt.Errorf("failed during schema restoration: %w", firstSchemaErr)
		}
	}

	// --- Restore Data ---
	var dataFiles []string
	dataSuffix := ".data.sql"
	for _, file := range files {
		if strings.Contains(file, dataSuffix) {
			dataFiles = append(dataFiles, file)
		}
	}

	log.Printf("Found %d data files to restore. Parallelism: %d", len(dataFiles), r.config.Parallel)
	if len(dataFiles) > 0 {
		semData := make(chan struct{}, r.config.Parallel)
		var wgData sync.WaitGroup
		errChanData := make(chan error, len(dataFiles))

		for _, dataFile := range dataFiles {
			wgData.Add(1)
			go func(df string) {
				defer wgData.Done()
				semData <- struct{}{}
				defer func() { <-semData }()

				log.Printf("Restoring data from %s...", df)
				// Всегда запрашиваем распаковку на стороне клиента для файлов данных (через decompressStream в storage)
				reader, downloadErr := r.storage.Download(df)
				if downloadErr != nil {
					errChanData <- fmt.Errorf("failed to download data file %s: %w", df, downloadErr)
					return
				}
				// restoreData handles closing the reader
				if restoreErr := r.restoreData(reader); restoreErr != nil {
					errChanData <- fmt.Errorf("failed to restore data from %s: %w", df, restoreErr)
					return
				}
				log.Printf("Successfully restored data from %s.", df)
			}(dataFile)
		}
		wgData.Wait()
		close(errChanData)

		var firstDataErr error
		for errItem := range errChanData {
			if firstDataErr == nil {
				firstDataErr = errItem
			}
			log.Printf("Error during data restoration: %v", errItem)
		}
		if firstDataErr != nil {
			return fmt.Errorf("failed during data restoration: %w", firstDataErr)
		}
	}

	log.Println("Restore completed successfully.")
	return nil
}

// restoreSchema reads schema definition from the reader and executes it.
func (r *Restorer) restoreSchema(reader io.ReadCloser) error {
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Printf("Warning: failed to close schema reader: %v", closeErr)
		}
	}()
	content, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read schema content: %w", err)
	}

	query := string(content)
	if strings.TrimSpace(query) == "" {
		log.Println("Schema file is empty, skipping.")
		return nil
	}

	log.Printf("Executing schema query: %s...", strings.Split(query, "\n")[0]) // Log first line
	_, err = r.client.ExecuteQuery(query)
	if err != nil {
		return fmt.Errorf("failed to execute schema query: %w", err)
	}
	return nil
}

// restoreData reads data statements from the reader, parses respecting quotes, and executes them.
func (r *Restorer) restoreData(reader io.ReadCloser) error {
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Printf("Warning: failed to close data reader: %v", closeErr)
		}
	}()
	return r.executeStatementsFromStream(reader)
}

// executeStatementsFromStream reads SQL statements separated by semicolons respecting quotes from the reader.
func (r *Restorer) executeStatementsFromStream(reader io.ReadCloser) error {
	bufReader := bufio.NewReader(reader)
	var statementBuilder strings.Builder
	var inSingleQuotes, inDoubleQuotes, inBackticks bool
	var escaped bool
	var statementCount int

	for {
		runeValue, _, err := bufReader.ReadRune()
		if err != nil {
			if err == io.EOF {
				// End of file reached, process any remaining statement
				finalStatement := strings.TrimSpace(statementBuilder.String())
				if finalStatement != "" {
					statementCount++
					log.Printf("Executing final statement %d (EOF)...", statementCount)
					if execErr := r.executeSingleStatement(finalStatement); execErr != nil {
						return fmt.Errorf("failed executing final statement %d: %w", statementCount, execErr)
					}
				}
				break // Exit loop successfully
			}
			return fmt.Errorf("error reading data stream: %w", err) // Return other read errors
		}

		// Append the rune regardless of state first
		statementBuilder.WriteRune(runeValue)

		// State machine logic
		if escaped {
			// Previous character was escape, so this character is literal
			escaped = false
		} else if runeValue == '\\' {
			// Current character is escape, next one is literal
			escaped = true
		} else if runeValue == '\'' && !inDoubleQuotes && !inBackticks {
			inSingleQuotes = !inSingleQuotes
		} else if runeValue == '"' && !inSingleQuotes && !inBackticks {
			inDoubleQuotes = !inDoubleQuotes
		} else if runeValue == '`' && !inSingleQuotes && !inDoubleQuotes {
			inBackticks = !inBackticks
		} else if runeValue == ';' && !inSingleQuotes && !inDoubleQuotes && !inBackticks {
			// Statement terminator found outside quotes
			statement := strings.TrimSpace(statementBuilder.String())
			if statement != "" {
				statementCount++
				log.Printf("Executing statement %d...", statementCount)
				if execErr := r.executeSingleStatement(statement); execErr != nil {
					return fmt.Errorf("failed executing statement %d: %w", statementCount, execErr)
				}
			}
			// Reset for the next statement
			statementBuilder.Reset()
			escaped = false // Reset escaped state for new statement
		} else {
			// Regular character, reset escaped if it wasn't consumed by a quote
			escaped = false
		}
	}

	log.Printf("Finished processing stream, executed %d statements.", statementCount)
	return nil
}

// executeSingleStatement executes a single SQL statement, potentially compressing it before sending.
func (r *Restorer) executeSingleStatement(query string) error {
	var err error
	compressFormat := strings.ToLower(r.config.CompressFormat)

	if compressFormat == "gzip" || compressFormat == "zstd" {
		var compressedBody bytes.Buffer
		var contentEncoding string
		originalLength := len(query)

		if compressFormat == "gzip" {
			level := r.config.CompressLevel
			if level < gzip.BestSpeed || level > gzip.BestCompression {
				level = gzip.DefaultCompression
			}
			gw, _ := gzip.NewWriterLevel(&compressedBody, level)
			_, copyErr := io.WriteString(gw, query)
			closeErr := gw.Close()
			if copyErr != nil {
				return fmt.Errorf("failed to write query to gzip writer: %w", copyErr)
			}
			if closeErr != nil {
				return fmt.Errorf("failed to close gzip writer: %w", closeErr)
			}
			contentEncoding = "gzip"
		} else { // zstd
			zstdLevel := zstd.EncoderLevelFromZstd(r.config.CompressLevel)
			zw, _ := zstd.NewWriter(&compressedBody, zstd.WithEncoderLevel(zstdLevel))
			_, copyErr := io.WriteString(zw, query)
			closeErr := zw.Close()
			if copyErr != nil {
				return fmt.Errorf("failed to write query to zstd writer: %w", copyErr)
			}
			if closeErr != nil {
				return fmt.Errorf("failed to close zstd writer: %w", closeErr)
			}
			contentEncoding = "zstd"
		}

		log.Printf("Executing statement compressed with %s (original length %d, compressed length %d)...", contentEncoding, originalLength, compressedBody.Len())
		_, err = r.client.ExecuteQueryWithBody(bytes.NewReader(compressedBody.Bytes()), contentEncoding, query)

	} else {
		_, err = r.client.ExecuteQuery(query)
	}

	if err != nil {
		return err
	}
	return nil
}
