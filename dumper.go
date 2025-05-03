package main

import (
	"bytes"
	"fmt"
	"github.com/Slach/clickhouse-dump/storage"
	"strings"
)

type Dumper struct {
	config  *Config
	client  *ClickHouseClient
	storage storage.RemoteStorage
}

func NewDumper(config *Config) (*Dumper, error) {
	var s storage.RemoteStorage
	var err error

	switch config.StorageType {
	case "s3":
		s, err = storage.NewS3Storage(config.StorageConfig["bucket"], config.StorageConfig["region"])
	case "gcs":
		s, err = storage.NewGCSStorage(config.StorageConfig["bucket"])
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

	return &Dumper{
		config:  config,
		client:  NewClickHouseClient(config),
		storage: s,
	}, nil
}

func (d *Dumper) Dump() error {
	tables, err := d.getTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		if err := d.dumpSchema(table); err != nil {
			return err
		}
		if err := d.dumpData(table); err != nil {
			return err
		}
	}

	return nil
}

func (d *Dumper) getTables() ([]string, error) {
	query := "SHOW TABLES"
	resp, err := d.client.ExecuteQuery(query)
	if err != nil {
		return nil, err
	}

	tables := strings.Split(strings.TrimSpace(string(resp)), "\n")
	return tables, nil
}

func (d *Dumper) dumpSchema(table string) error {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", table)
	resp, err := d.client.ExecuteQuery(query)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/%s.%s.schema.sql", d.config.StoragePath, d.config.Database, table)
	return d.storage.Upload(filename, bytes.NewReader(resp), d.config.CompressFormat, d.config.CompressLevel)
}

func (d *Dumper) dumpData(table string) error {
	query := fmt.Sprintf("SELECT * FROM %s FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size=%d", table, d.config.BatchSize)
	body, err := d.client.ExecuteQueryStreaming(query)
	if err != nil {
		return err
	}
	defer body.Close()

	filename := fmt.Sprintf("%s/%s.%s.data.sql", d.config.StoragePath, d.config.Database, table)
	return d.storage.Upload(filename, body, d.config.CompressFormat, d.config.CompressLevel)
}
