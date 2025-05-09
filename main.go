package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

var app = &cli.App{
	Name:  "clickhouse-dump",
	Usage: "Dump and restore ClickHouse tables to/from remote storage",
	Flags: []cli.Flag{
		// ClickHouse Connection Flags
		&cli.StringFlag{
			Name:     "host",
			Aliases:  []string{"H"},
			Value:    "localhost",
			Usage:    "ClickHouse host",
			EnvVars:  []string{"CLICKHOUSE_HOST"},
			Required: false, // Make optional if defaults are acceptable or env var used
		},
		&cli.IntFlag{
			Name:     "port",
			Aliases:  []string{"p"},
			Value:    8123,
			Usage:    "ClickHouse HTTP port",
			EnvVars:  []string{"CLICKHOUSE_PORT"},
			Required: false,
		},
		&cli.StringFlag{
			Name:     "user",
			Aliases:  []string{"u"},
			Value:    "default",
			Usage:    "ClickHouse user",
			EnvVars:  []string{"CLICKHOUSE_USER"},
			Required: false,
		},
		&cli.StringFlag{
			Name:     "password",
			Aliases:  []string{"P"},
			Usage:    "ClickHouse password",
			EnvVars:  []string{"CLICKHOUSE_PASSWORD"},
			Required: false, // Often provided via env var
		},
		&cli.StringFlag{
			Name:    "databases",
			Aliases: []string{"d"},
			Value:   ".*",
			Usage:   "Regexp pattern for databases to include",
			EnvVars: []string{"CLICKHOUSE_DATABASES"},
		},
		&cli.StringFlag{
			Name:    "exclude-databases",
			Value:   "^system$|^INFORMATION_SCHEMA$|^information_schema$",
			Usage:   "Regexp pattern for databases to exclude",
			EnvVars: []string{"EXCLUDE_DATABASES"},
		},
		&cli.StringFlag{
			Name:    "tables",
			Aliases: []string{"t"},
			Value:   ".*",
			Usage:   "Regexp pattern for tables to include",
			EnvVars: []string{"TABLES"},
		},
		&cli.StringFlag{
			Name:    "exclude-tables",
			Value:   "",
			Usage:   "Regexp pattern for tables to exclude",
			EnvVars: []string{"EXCLUDE_TABLES"},
		},
		// Dump Specific Flags (can be moved to dump command if needed)
		&cli.IntFlag{
			Name:    "batch-size",
			Value:   100000, // Adjusted default
			Usage:   "Batch size for SQL Insert statements (dump only)",
			EnvVars: []string{"BATCH_SIZE"},
		},
		&cli.StringFlag{
			Name:    "compress-format",
			Value:   "gzip",
			Usage:   "Compression format: gzip, zstd, or none (dump only)",
			EnvVars: []string{"COMPRESS_FORMAT"},
		},
		&cli.IntFlag{
			Name:    "compress-level",
			Value:   6, // Default for gzip
			Usage:   "Compression level (gzip: 1-9, zstd: 1-22) (dump only)",
			EnvVars: []string{"COMPRESS_LEVEL"},
		},
		&cli.BoolFlag{
			Name:    "debug",
			Usage:   "Enable debug logging",
			EnvVars: []string{"DEBUG"},
		},
		// Storage Common Flags
		&cli.StringFlag{
			Name:     "storage-type",
			Usage:    "Storage backend type: file, s3, gcs, azblob, sftp, ftp",
			EnvVars:  []string{"STORAGE_TYPE"},
			Required: true, // Required for both dump and restore
		},
		// Storage Specific Flags (using map approach in getConfig)
		&cli.StringFlag{
			Name:    "storage-bucket",
			Usage:   "S3/GCS bucket name",
			EnvVars: []string{"STORAGE_BUCKET"},
		},
		&cli.StringFlag{
			Name:    "storage-region",
			Usage:   "S3 region",
			EnvVars: []string{"STORAGE_REGION"},
		},
		&cli.StringFlag{
			Name:    "storage-account",
			Usage:   "Storage account name/access key (S3: access key ID, Azure: account name)",
			EnvVars: []string{"AWS_ACCESS_KEY_ID", "STORAGE_ACCOUNT"},
		},
		&cli.StringFlag{
			Name:    "storage-key",
			Usage:   "Storage secret key (S3: secret access key, Azure: account key, GCS: path to credentials JSON)",
			EnvVars: []string{"AWS_SECRET_ACCESS_KEY", "STORAGE_KEY"},
		},
		&cli.StringFlag{
			Name:    "storage-endpoint",
			Usage:   "Custom endpoint URL (S3: for MinIO/etc, GCS: for fake-gcs-server, Azure: for Azurite)",
			EnvVars: []string{"STORAGE_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:    "storage-container",
			Usage:   "Azure Blob Storage container name",
			EnvVars: []string{"STORAGE_CONTAINER"},
		},
		&cli.StringFlag{
			Name:    "storage-host",
			Usage:   "SFTP/FTP host (and optional port like host:port)",
			EnvVars: []string{"STORAGE_HOST"},
		},
		&cli.StringFlag{
			Name:    "storage-user",
			Usage:   "SFTP/FTP user",
			EnvVars: []string{"STORAGE_USER"},
		},
		&cli.StringFlag{
			Name:    "storage-password",
			Usage:   "SFTP/FTP password",
			EnvVars: []string{"STORAGE_PASSWORD"},
		},
		&cli.StringFlag{
			Name:    "storage-path",
			Usage:   "Base path in storage for dump/restore files",
			Value:   "",
			EnvVars: []string{"STORAGE_PATH"},
		},
	},
	Commands: []*cli.Command{
		{
			Name:      "dump",
			Usage:     "Dump ClickHouse tables schema and data to remote storage",
			Action:    RunDumper,
			ArgsUsage: "BACKUP_NAME",
		},
		{
			Name:      "restore",
			Usage:     "Restore ClickHouse tables from remote storage",
			Action:    RunRestorer,
			ArgsUsage: "BACKUP_NAME",
		},
	},
}

func main() {

	// Setup logging
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err) // Use log.Fatal to print error and exit(1)
	}
}

func RunDumper(c *cli.Context) error {
	if c.Args().Len() == 0 {
		return fmt.Errorf("backup name is required as argument")
	}
	backupName := c.Args().First()

	config, err := getConfig(c)
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}
	config.BackupName = backupName
	dumper, err := NewDumper(config)
	if err != nil {
		return fmt.Errorf("failed to initialize dumper: %w", err)
	}
	log.Println("Starting dump process...")
	err = dumper.Dump()
	if err == nil {
		log.Println("Dump completed successfully.")
	} else {
		log.Printf("Dump failed: %v", err)
	}
	return err
}

func RunRestorer(c *cli.Context) error {
	if c.Args().Len() == 0 {
		return fmt.Errorf("backup name is required as argument")
	}
	backupName := c.Args().First()

	config, err := getConfig(c)
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}
	config.BackupName = backupName
	restorer, err := NewRestorer(config)
	if err != nil {
		return fmt.Errorf("failed to initialize restorer: %w", err)
	}
	log.Println("Starting restore process...")
	err = restorer.Restore()
	// Restore() already logs success/failure details, just return error status
	return err
}

// getConfig extracts configuration from command line context, including storage details.
func getConfig(c *cli.Context) (*Config, error) {
	// Basic ClickHouse config
	config := &Config{
		Host:             c.String("host"),
		Port:             c.Int("port"),
		User:             c.String("user"),
		Password:         c.String("password"),
		Databases:        c.String("databases"),
		ExcludeDatabases: c.String("exclude-databases"),
		Tables:           c.String("tables"),
		ExcludeTables:    c.String("exclude-tables"),
		BatchSize:        c.Int("batch-size"),         // Used by dumper
		CompressFormat:   c.String("compress-format"), // Used by dumper
		CompressLevel:    c.Int("compress-level"),     // Used by dumper
		StorageType:      strings.ToLower(c.String("storage-type")),
		StorageConfig: map[string]string{
			"host":      c.String("storage-host"),
			"user":      c.String("storage-user"),
			"password":  c.String("storage-password"),
			"path":      c.String("storage-path"),
			"bucket":    c.String("storage-bucket"),
			"region":    c.String("storage-region"),
			"account":   c.String("storage-account"),
			"key":       c.String("storage-key"),
			"endpoint":  c.String("storage-endpoint"),
			"container": c.String("storage-container"),
		},
		Debug: c.Bool("debug"),
	}
	if config.ExcludeDatabases == "" {
		config.ExcludeDatabases = "^system$|^INFORMATION_SCHEMA$|^information_schema$"
	}
	// Populate StorageConfig based on StorageType
	switch config.StorageType {
	case "file":
		// For file storage, path is required and treated as local directory
		if config.StorageConfig["path"] == "" {
			return nil, fmt.Errorf("storage-path is required for file storage type")
		}
	case "s3":
		if config.StorageConfig["bucket"] == "" {
			return nil, fmt.Errorf("storage-bucket is required for s3 storage type")
		}
	case "gcs":
		if config.StorageConfig["bucket"] == "" {
			return nil, fmt.Errorf("storage-bucket is required for gcs storage type")
		}
	case "azblob":
		if config.StorageConfig["account"] == "" || config.StorageConfig["key"] == "" || config.StorageConfig["container"] == "" {
			return nil, fmt.Errorf("storage-account, storage-key, and storage-container are required for azblob storage type")
		}
	case "sftp", "ftp":
		if config.StorageConfig["host"] == "" || config.StorageConfig["user"] == "" {
			return nil, fmt.Errorf("storage-host and storage-user are required for %s storage type", config.StorageType)
		}
	case "":
		return nil, fmt.Errorf("storage-type must be specified")
	default:
		return nil, fmt.Errorf("unsupported storage-type: %s", config.StorageType)
	}

	// Basic validation for ClickHouse connection details (optional, depends on requirements)
	if config.Host == "" {
		return nil, fmt.Errorf("clickhouse host is required")
	}

	return config, nil
}
