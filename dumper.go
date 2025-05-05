package main

import (
	"bytes"
	"fmt"
	"github.com/Slach/clickhouse-dump/storage"
	"log"
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
	case "file":
		s, err = storage.NewFileStorage(config.StorageConfig["path"])
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
	dbTables, err := d.getTables()
	if err != nil {
		return err
	}

	totalTables := 0
	for db := range dbTables {
		totalTables += len(dbTables[db])
	}
	log.Printf("found %d tables across %d databases for dump", totalTables, len(dbTables))

	for db, tables := range dbTables {
		for _, table := range tables {
			fullTableName := fmt.Sprintf("%s.%s", db, table)
			if err := d.dumpSchema(fullTableName); err != nil {
				return err
			}
			if err := d.dumpData(fullTableName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *Dumper) getTables() (map[string][]string, error) {
	query := fmt.Sprintf(`
		SELECT 
			database, 
			name 
		FROM system.tables 
		WHERE 
			match(database, '%s') AND 
			NOT match(database, '%s') AND
			match(name, '%s') AND
			(NOT match(name, '%s') OR '%s' = '')
		FORMAT TabSeparated`,
		d.config.Databases,
		d.config.ExcludeDatabases,
		d.config.Tables,
		d.config.ExcludeTables,
		d.config.ExcludeTables)

	resp, err := d.client.ExecuteQuery(query)
	if err != nil {
		return nil, err
	}

	tables := make(map[string][]string)
	lines := strings.Split(strings.TrimSpace(string(resp)), "\n")
	for _, line := range lines {
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			continue
		}
		db := parts[0]
		table := parts[1]
		tables[db] = append(tables[db], table)
	}

	return tables, nil
}

func (d *Dumper) dumpSchema(table string) error {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", table)
	resp, err := d.client.ExecuteQuery(query)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/%s.schema.sql", d.config.StorageConfig["path"], table)
	return d.storage.Upload(filename, bytes.NewReader(resp), d.config.CompressFormat, d.config.CompressLevel)
}

func (d *Dumper) dumpData(table string) error {
	query := fmt.Sprintf("SELECT * FROM %s FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size=%d", table, d.config.BatchSize)
	body, err := d.client.ExecuteQueryStreaming(query)
	if err != nil {
		return err
	}
	defer body.Close()

	filename := fmt.Sprintf("%s/%s.data.sql", d.config.StorageConfig["path"], table)
	return d.storage.Upload(filename, body, d.config.CompressFormat, d.config.CompressLevel)
}
