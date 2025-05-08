package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Slach/clickhouse-dump/storage"
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
		s, err = storage.NewGCSStorage(config.StorageConfig["bucket"], config.StorageConfig["endpoint"], config.StorageConfig["key"])
	case "azblob":
		s, err = storage.NewAzBlobStorage(config.StorageConfig["account"], config.StorageConfig["key"], config.StorageConfig["container"])
	case "sftp":
		s, err = storage.NewSFTPStorage(config.StorageConfig["host"], config.StorageConfig["user"], config.StorageConfig["password"])
	case "ftp":
		s, err = storage.NewFTPStorage(config.StorageConfig["host"], config.StorageConfig["user"], config.StorageConfig["password"])
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
	basePath := strings.TrimRight(r.config.StorageConfig["path"], "/")
	backupPrefix := fmt.Sprintf("%s/%s", basePath, r.config.BackupName)
	log.Printf("Listing storage items with prefix: %s (recursive)", backupPrefix)

	files, err := r.storage.List(backupPrefix, true)
	if err != nil {
		return fmt.Errorf("failed to list files in storage with prefix %s: %w", backupPrefix, err)
	}

	// Filter for database files
	var dbFiles []string
	dbSuffix := ".database.sql"
	for _, file := range files {
		if strings.HasSuffix(file, dbSuffix) {
			dbFiles = append(dbFiles, file)
		}
	}

	log.Printf("Found %d database files to restore.", len(dbFiles))
	for _, dbFile := range dbFiles {
		log.Printf("Restoring database from %s...", dbFile)
		reader, downloadErr := r.storage.Download(dbFile)
		if downloadErr != nil {
			return fmt.Errorf("failed to download database file %s: %w", dbFile, downloadErr)
		}
		if restoreErr := r.restoreSchema(reader); restoreErr != nil {
			return fmt.Errorf("failed to restore database from %s: %w", dbFile, restoreErr)
		}
		log.Printf("Successfully restored database from %s.", dbFile)
	}

	// --- Restore Tables ---
	var schemaFiles []string
	schemaSuffix := ".schema.sql"
	for _, file := range files {
		if strings.HasSuffix(file, schemaSuffix) {
			schemaFiles = append(schemaFiles, file)
		}
	}

	log.Printf("Found %d schema files to restore.", len(schemaFiles))
	for _, schemaFile := range schemaFiles {
		log.Printf("Restoring schema from %s...", schemaFile)
		reader, downloadErr := r.storage.Download(schemaFile)
		if downloadErr != nil {
			return fmt.Errorf("failed to download schema file %s: %w", schemaFile, downloadErr)
		}
		if restoreErr := r.restoreSchema(reader); restoreErr != nil {
			return fmt.Errorf("failed to restore schema from %s: %w", schemaFile, restoreErr)
		}
		log.Printf("Successfully restored schema from %s.", schemaFile)
	}

	// --- Restore Data ---
	var dataFiles []string
	dataSuffix := ".data.sql"
	for _, file := range files {
		if strings.HasSuffix(file, dataSuffix) {
			dataFiles = append(dataFiles, file)
		}
	}

	log.Printf("Found %d data files to restore.", len(dataFiles))
	for _, dataFile := range dataFiles {
		log.Printf("Restoring data from %s...", dataFile)
		reader, downloadErr := r.storage.Download(dataFile)
		if downloadErr != nil {
			return fmt.Errorf("failed to download data file %s: %w", dataFile, downloadErr)
		}
		if restoreErr := r.restoreData(reader); restoreErr != nil {
			return fmt.Errorf("failed to restore data from %s: %w", dataFile, downloadErr)
		}
		log.Printf("Successfully restored data from %s.", dataFile)
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

// executeSingleStatement executes a single SQL statement.
func (r *Restorer) executeSingleStatement(query string) error {
	// Optional: Add logging for the query being executed (be careful with sensitive data)
	// log.Printf("Executing query: %s", query)
	_, err := r.client.ExecuteQuery(query)
	if err != nil {
		return err
	}
	return nil
}
