package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v3"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

var app = &cli.Command{
	Name:    "clickhouse-dump",
	Usage:   "Dump and restore ClickHouse tables to/from remote storage",
	Version: fmt.Sprintf("%s (commit %s, built at %s)", version, commit, date),
	Flags: []cli.Flag{
		// ClickHouse Connection Flags
		&cli.StringFlag{
			Name:     "host",
			Aliases:  []string{"H"},
			Value:    "localhost",
			Usage:    "ClickHouse host",
			Sources:  cli.EnvVars("CLICKHOUSE_HOST"),
			Required: false, // Make optional if defaults are acceptable or env var used
		},
		&cli.IntFlag{
			Name:     "port",
			Aliases:  []string{"p"},
			Value:    8123,
			Usage:    "ClickHouse HTTP port",
			Sources:  cli.EnvVars("CLICKHOUSE_PORT"),
			Required: false,
		},
		&cli.StringFlag{
			Name:     "user",
			Aliases:  []string{"u"},
			Value:    "default",
			Usage:    "ClickHouse user",
			Sources:  cli.EnvVars("CLICKHOUSE_USER"),
			Required: false,
		},
		&cli.StringFlag{
			Name:     "password",
			Aliases:  []string{"P"},
			Usage:    "ClickHouse password",
			Sources:  cli.EnvVars("CLICKHOUSE_PASSWORD"),
			Required: false, // Often provided via env var
		},
		&cli.StringFlag{
			Name:    "databases",
			Aliases: []string{"d"},
			Value:   ".*",
			Usage:   "Regexp pattern for databases to include",
			Sources: cli.EnvVars("CLICKHOUSE_DATABASES"),
		},
		&cli.StringFlag{
			Name:    "exclude-databases",
			Value:   "^system$|^INFORMATION_SCHEMA$|^information_schema$",
			Usage:   "Regexp pattern for databases to exclude",
			Sources: cli.EnvVars("EXCLUDE_DATABASES"),
		},
		&cli.StringFlag{
			Name:    "tables",
			Aliases: []string{"t"},
			Value:   ".*",
			Usage:   "Regexp pattern for tables to include",
			Sources: cli.EnvVars("TABLES"),
		},
		&cli.StringFlag{
			Name:    "exclude-tables",
			Value:   "",
			Usage:   "Regexp pattern for tables to exclude",
			Sources: cli.EnvVars("EXCLUDE_TABLES"),
		},
		// Dump Specific Flags (can be moved to dump command if needed)
		&cli.IntFlag{
			Name:    "batch-size",
			Value:   100000, // Adjusted default
			Usage:   "Batch size for SQL Insert statements (dump only)",
			Sources: cli.EnvVars("BATCH_SIZE"),
		},
		&cli.StringFlag{
			Name:    "compress-format",
			Value:   "gzip",
			Usage:   "Compression format: gzip, zstd, or none (dump only)",
			Sources: cli.EnvVars("COMPRESS_FORMAT"),
		},
		&cli.IntFlag{
			Name:    "compress-level",
			Value:   6, // Default for gzip
			Usage:   "Compression level (gzip: 1-9, zstd: 1-22) (dump only)",
			Sources: cli.EnvVars("COMPRESS_LEVEL"),
		},
		&cli.BoolFlag{
			Name:    "debug",
			Usage:   "Enable debug logging",
			Sources: cli.EnvVars("DEBUG"),
		},
		&cli.IntFlag{
			Name:    "parallel",
			Value:   1,
			Usage:   "Number of parallel table processing operations",
			Sources: cli.EnvVars("PARALLEL"),
		},
		// Storage Common Flags
		&cli.StringFlag{
			Name:     "storage-type",
			Usage:    "Storage backend type: file, s3, gcs, azblob, sftp, ftp",
			Sources:  cli.EnvVars("STORAGE_TYPE"),
			Required: true, // Required for both dump and restore
		},
		// Storage Specific Flags (using map approach in getConfig)
		&cli.StringFlag{
			Name:    "storage-bucket",
			Usage:   "S3/GCS bucket name",
			Sources: cli.EnvVars("STORAGE_BUCKET"),
		},
		&cli.StringFlag{
			Name:    "storage-region",
			Usage:   "S3 region",
			Sources: cli.EnvVars("STORAGE_REGION"),
		},
		&cli.StringFlag{
			Name:    "storage-account",
			Usage:   "Storage account name/access key (S3: access key ID, Azure: account name)",
			Sources: cli.EnvVars("AWS_ACCESS_KEY_ID", "STORAGE_ACCOUNT"),
		},
		&cli.StringFlag{
			Name:    "storage-key",
			Usage:   "Storage secret key (S3: secret access key, Azure: account key, GCS: path to credentials JSON)",
			Sources: cli.EnvVars("AWS_SECRET_ACCESS_KEY", "STORAGE_KEY"),
		},
		&cli.StringFlag{
			Name:    "storage-endpoint",
			Usage:   "Custom endpoint URL (S3: for MinIO/etc, GCS: for fake-gcs-server, Azure: for Azurite)",
			Sources: cli.EnvVars("STORAGE_ENDPOINT"),
		},
		&cli.StringFlag{
			Name:    "storage-container",
			Usage:   "Azure Blob Storage container name",
			Sources: cli.EnvVars("STORAGE_CONTAINER"),
		},
		&cli.StringFlag{
			Name:    "storage-host",
			Usage:   "SFTP/FTP host (and optional port like host:port)",
			Sources: cli.EnvVars("STORAGE_HOST"),
		},
		&cli.StringFlag{
			Name:    "storage-user",
			Usage:   "SFTP/FTP user",
			Sources: cli.EnvVars("STORAGE_USER"),
		},
		&cli.StringFlag{
			Name:    "storage-password",
			Usage:   "SFTP/FTP password",
			Sources: cli.EnvVars("STORAGE_PASSWORD"),
		},
		&cli.StringFlag{
			Name:    "storage-path",
			Usage:   "Base path in storage for dump/restore files",
			Value:   "",
			Sources: cli.EnvVars("STORAGE_PATH"),
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

	err := app.Run(context.Background(), os.Args)
	if err != nil {
		log.Fatal(err) // Use log.Fatal to print error and exit(1)
	}
}

func RunDumper(ctx context.Context, cmd *cli.Command) error {
	if cmd.Args().Len() == 0 {
		return fmt.Errorf("backup name is required as argument")
	}
	backupName := cmd.Args().First()

	config, err := getConfig(cmd)
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}
	config.BackupName = backupName

	// Create ClickHouse client to check version
	client := NewClickHouseClient(config)
	if err := checkClickHouseVersion(client); err != nil {
		return err
	}

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

func RunRestorer(ctx context.Context, cmd *cli.Command) error {
	if cmd.Args().Len() == 0 {
		return fmt.Errorf("backup name is required as argument")
	}
	backupName := cmd.Args().First()

	config, err := getConfig(cmd)
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}
	config.BackupName = backupName

	// Create ClickHouse client to check version
	client := NewClickHouseClient(config)
	if err := checkClickHouseVersion(client); err != nil {
		return err
	}

	restorer, err := NewRestorer(config)
	if err != nil {
		return fmt.Errorf("failed to initialize restorer: %w", err)
	}
	log.Println("Starting restore process...")
	err = restorer.Restore()
	// Restore() already logs success/failure details, just return error status
	return err
}

// checkClickHouseVersion verifies that the ClickHouse server is at least version 24.10
func checkClickHouseVersion(client *ClickHouseClient) error {
	query := "SELECT version()"
	respBytes, err := client.ExecuteQuery(query)
	if err != nil {
		return fmt.Errorf("failed to check ClickHouse version: %w", err)
	}

	version := strings.TrimSpace(string(respBytes))
	log.Printf("Connected to ClickHouse version: %s", version)

	// Extract major and minor version numbers
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return fmt.Errorf("unexpected ClickHouse version format: %s", version)
	}

	var major, minor int
	if _, err := fmt.Sscanf(parts[0], "%d", &major); err != nil {
		return fmt.Errorf("failed to parse major version from %s: %w", version, err)
	}

	if _, err := fmt.Sscanf(parts[1], "%d", &minor); err != nil {
		return fmt.Errorf("failed to parse minor version from %s: %w", version, err)
	}

	// Check if version is at least 24.10
	if major < 24 || (major == 24 && minor < 10) {
		return fmt.Errorf("unsupported ClickHouse version: %s. Minimum required version is 24.10", version)
	}

	return nil
}

// getConfig extracts configuration from command line context, including storage details.
func getConfig(cmd *cli.Command) (*Config, error) {
	// Basic ClickHouse config
	config := &Config{
		Host:             cmd.String("host"),
		Port:             cmd.Int("port"),
		User:             cmd.String("user"),
		Password:         cmd.String("password"),
		Databases:        cmd.String("databases"),
		ExcludeDatabases: cmd.String("exclude-databases"),
		Tables:           cmd.String("tables"),
		ExcludeTables:    cmd.String("exclude-tables"),
		BatchSize:        cmd.Int("batch-size"),
		CompressFormat:   cmd.String("compress-format"),
		CompressLevel:    cmd.Int("compress-level"),
		StorageType:      strings.ToLower(cmd.String("storage-type")),
		StorageConfig: map[string]string{
			"host":      cmd.String("storage-host"),
			"user":      cmd.String("storage-user"),
			"password":  cmd.String("storage-password"),
			"path":      cmd.String("storage-path"),
			"bucket":    cmd.String("storage-bucket"),
			"region":    cmd.String("storage-region"),
			"account":   cmd.String("storage-account"),
			"key":       cmd.String("storage-key"),
			"endpoint":  cmd.String("storage-endpoint"),
			"container": cmd.String("storage-container"),
		},
		Debug:    cmd.Bool("debug"),
		Parallel: cmd.Int("parallel"),
	}

	if config.Parallel < 1 {
		return nil, fmt.Errorf("--parallel must be at least 1")
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
