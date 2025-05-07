package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
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
	if config.StorageConfig == nil {
		return nil, fmt.Errorf("storage configuration is missing")
	}

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
		// Ensure required SFTP config keys exist
		host, u, pass := config.StorageConfig["host"], config.StorageConfig["user"], config.StorageConfig["password"]
		if host == "" || u == "" {
			return nil, fmt.Errorf("sftp storage requires 'host' and 'user' in storage config")
		}
		s, err = storage.NewSFTPStorage(host, u, pass)
	case "ftp":
		// Ensure required FTP config keys exist
		host, u, pass := config.StorageConfig["host"], config.StorageConfig["user"], config.StorageConfig["password"]
		if host == "" || u == "" {
			return nil, fmt.Errorf("ftp storage requires 'host' and 'user' in storage config")
		}
		s, err = storage.NewFTPStorage(host, u, pass)
	case "": // Handle case where no storage type is specified (e.g., local testing?)
		return nil, fmt.Errorf("storage type must be specified")
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.StorageType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage type %s: %w", config.StorageType, err)
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

	// --- Restore Schema ---
	schemaPrefix := fmt.Sprintf("%s/%s/", r.config.StorageConfig["path"], r.config.BackupName)
	schemaSuffix := ".schema.sql"
	log.Printf("Listing schema files with prefix: %s*%s", schemaPrefix, schemaSuffix)

	allFiles, err := r.storage.List(schemaPrefix)
	if err != nil {
		return fmt.Errorf("failed to list files in storage with prefix %s: %w", schemaPrefix, err)
	}

	// Filter for actual schema files (List might return other files with the same prefix)
	var schemaFiles []string
	for _, file := range allFiles {
		if strings.HasSuffix(file, schemaSuffix) {
			// Basic check: ensure it looks like database.table.schema.sql[.compression_ext]
			// We rely on Download handling the compression extension later.
			baseName := strings.TrimSuffix(file, storage.GetCompressionExtension(file))
			if strings.HasSuffix(baseName, schemaSuffix) {
				// Extract db/table from path like "path/db/table.schema.sql"
				parts := strings.Split(strings.TrimPrefix(baseName, schemaPrefix), "/")
				if len(parts) == 3 && parts[2] != "" {
					schemaFiles = append(schemaFiles, baseName)
				}
			}
		}
	}

	log.Printf("Found %d schema files to restore.", len(schemaFiles))
	for _, schemaFileBase := range schemaFiles {
		log.Printf("Restoring schema from %s...", schemaFileBase)
		reader, downloadEr := r.storage.Download(schemaFileBase) // Download will handle finding .gz, .zstd etc.
		if downloadEr != nil {
			return fmt.Errorf("failed to download schema file %s: %w", schemaFileBase, downloadEr)
		}
		// Ensure the downloaded stream is closed
		func() {
			defer func() {
				if closeErr := reader.Close(); closeErr != nil {
					log.Printf("Warning: failed to close reader for %s: %v", schemaFileBase, closeErr)
				}
			}()
			if err := r.restoreSchema(reader); err != nil {
				// Wrap error with filename context
				err = fmt.Errorf("failed to restore schema from %s: %w", schemaFileBase, err)
			}
		}() // Execute the closure immediately
		log.Printf("Successfully restored schema from %s.", schemaFileBase)
	}

	// --- Restore Data ---
	dataSuffix := ".data.sql"
	log.Printf("Listing data files with prefix: %s*%s", schemaPrefix, dataSuffix)
	// Re-list or filter `allFiles` if List is expensive and returned everything needed
	// Assuming List is efficient enough, or we need to re-list for data files specifically
	allFiles, err = r.storage.List(schemaPrefix) // Re-list or reuse previous list if appropriate
	if err != nil {
		return fmt.Errorf("failed to list files in storage with prefix %s: %w", schemaPrefix, err)
	}

	var dataFiles []string
	for _, file := range allFiles {
		if strings.HasSuffix(file, dataSuffix) {
			baseName := strings.TrimSuffix(file, storage.GetCompressionExtension(file))
			if strings.HasSuffix(baseName, dataSuffix) {
				// Extract db/table from path like "path/db/table.data.sql"
				parts := strings.Split(strings.TrimPrefix(baseName, schemaPrefix), "/")
				if len(parts) == 3 && parts[2] != "" {
					dataFiles = append(dataFiles, baseName)
				}
			}
		}
	}

	log.Printf("Found %d data files to restore.", len(dataFiles))
	for _, dataFileBase := range dataFiles {
		log.Printf("Restoring data from %s...", dataFileBase)
		reader, err := r.storage.Download(dataFileBase)
		if err != nil {
			return fmt.Errorf("failed to download data file %s: %w", dataFileBase, err)
		}
		// Process data stream
		err = r.restoreData(reader) // restoreData now handles closing the reader
		if err != nil {
			// Wrap error with filename context
			return fmt.Errorf("failed to restore data from %s: %w", dataFileBase, err)
		}
		log.Printf("Successfully restored data from %s.", dataFileBase)
	}

	log.Println("Restore completed successfully.")
	return nil
}

// restoreSchema reads schema definition from the reader and executes it.
func (r *Restorer) restoreSchema(reader io.Reader) error {
	content, err := io.ReadAll(reader) // Schemas are usually small
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

// restoreData reads data statements from the reader, parses them respecting quotes, and executes them.
// It ensures the reader is closed.
func (r *Restorer) restoreData(reader io.ReadCloser) error {
	defer func() {
		if cerr := reader.Close(); cerr != nil {
			log.Printf("Warning: failed to close data reader: %v", cerr)
		}
	}()
	return r.executeStatementsFromStream(reader)
}

// executeStatementsFromStream reads SQL statements separated by semicolons (respecting quotes)
// from the reader and executes them.
func (r *Restorer) executeStatementsFromStream(reader io.Reader) error {
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
	if _, err := r.client.ExecuteQuery(query); err != nil {
		return err
	}
	return nil
}
