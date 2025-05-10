# ClickHouse Dump

A simple tool for sql dumping and restoring ClickHouse databases to/from various storage backends. 
Main motivation is create simple tool for export/import small (less 10Gb) data via different clickhouse-servers, without direct access to disk.
Use https://github.com/Altinity/clickhouse-backup for production backup.

## Features

- Dump and restore ClickHouse databases and tables
- Filter databases and tables using regular expressions
- Support for multiple storage backends:
  - Local file system
  - Amazon S3 (and compatible services like MinIO)
  - Google Cloud Storage
  - Azure Blob Storage
  - SFTP
  - FTP
- Compression support (gzip, zstd)
- Configurable batch sizes for optimal performance

## Installation

```bash
go install github.com/Slach/clickhouse-dump@latest
```

Or download the latest binary from the [releases page](https://github.com/Slach/clickhouse-dump/releases).

## Usage

### Basic Commands

```bash
# Dump a database
clickhouse-dump dump BACKUP_NAME

# Restore from a backup
clickhouse-dump restore BACKUP_NAME
```

### Connection Parameters

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--host`, `-H` | `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `--port`, `-p` | `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `--user`, `-u` | `CLICKHOUSE_USER` | `default` | ClickHouse user |
| `--password`, `-P` | `CLICKHOUSE_PASSWORD` | | ClickHouse password |

### Filtering Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--databases`, `-d` | `CLICKHOUSE_DATABASES` | `.*` | Regexp pattern for databases to include |
| `--exclude-databases` | `EXCLUDE_DATABASES` | `^system$\|^INFORMATION_SCHEMA$\|^information_schema$` | Regexp pattern for databases to exclude |
| `--tables`, `-t` | `TABLES` | `.*` | Regexp pattern for tables to include |
| `--exclude-tables` | `EXCLUDE_TABLES` | | Regexp pattern for tables to exclude |

### Dump Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--batch-size` | `BATCH_SIZE` | `100000` | Batch size for SQL Insert statements |
| `--compress-format` | `COMPRESS_FORMAT` | `gzip` | Compression format: gzip, zstd, or none |
| `--compress-level` | `COMPRESS_LEVEL` | `6` | Compression level (gzip: 1-9, zstd: 1-22) |

### Storage Options

| Flag | Environment Variable | Required For | Description |
|------|---------------------|--------------|-------------|
| `--storage-type` | `STORAGE_TYPE` | All | Storage backend type: file, s3, gcs, azblob, sftp, ftp |
| `--storage-path` | `STORAGE_PATH` | file | Base path in storage for dump/restore files |
| `--storage-bucket` | `STORAGE_BUCKET` | s3, gcs | S3/GCS bucket name |
| `--storage-region` | `STORAGE_REGION` | s3 | S3 region |
| `--storage-account` | `AWS_ACCESS_KEY_ID`, `STORAGE_ACCOUNT` | s3, azblob | Storage account name/access key |
| `--storage-key` | `AWS_SECRET_ACCESS_KEY`, `STORAGE_KEY` | s3, gcs, azblob | Storage secret key |
| `--storage-endpoint` | `STORAGE_ENDPOINT` | s3, gcs, azblob | Custom endpoint URL |
| `--storage-container` | `STORAGE_CONTAINER` | azblob | Azure Blob Storage container name |
| `--storage-host` | `STORAGE_HOST` | sftp, ftp | SFTP/FTP host (and optional port) |
| `--storage-user` | `STORAGE_USER` | sftp, ftp | SFTP/FTP user |
| `--storage-password` | `STORAGE_PASSWORD` | sftp, ftp | SFTP/FTP password |

### Other Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--debug` | `DEBUG` | `false` | Enable debug logging |

## Examples

### Dump to Local File System

```bash
clickhouse-dump --storage-type file --storage-path /backups dump my_backup
```

### Dump to S3

```bash
clickhouse-dump --storage-type s3 --storage-bucket my-bucket --storage-region us-east-1 \
  --storage-account <access_key_id> --storage-key <secret_key> \
  dump my_backup
```

### Restore from S3

```bash
clickhouse-dump --storage-type s3 --storage-bucket my-bucket --storage-region us-east-1 \
  --storage-account <access_key_id> --storage-key <secret_key> \
  restore my_backup
```

### Dump Specific Databases with Compression

```bash
clickhouse-dump --databases "^(db1|db2)$" --compress-format zstd --compress-level 19 \
  --storage-type file --storage-path /backups dump my_backup
```

## License

MIT
