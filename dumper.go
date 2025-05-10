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
		s, err = storage.NewAzBlobStorage(config.StorageConfig["account"], config.StorageConfig["key"], config.StorageConfig["container"], config.StorageConfig["endpoint"])
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

func (d *Dumper) GetDatabases() ([]string, error) {
	where := make([]string, 0, 2)
	if d.config.Databases != "" {
		where = append(where, fmt.Sprintf("match(name, '%s')", d.config.Databases))
	}
	if d.config.ExcludeDatabases != "" {
		where = append(where, fmt.Sprintf("NOT match(name, '%s')", d.config.ExcludeDatabases))
	}
	query := fmt.Sprintf(`
		SELECT name 
		FROM system.databases 
		WHERE %s 
		FORMAT TSVRaw`,
		strings.Join(where, " AND "),
	)

	resp, err := d.client.ExecuteQuery(query)
	if err != nil {
		return nil, err
	}

	var databases []string
	lines := strings.Split(strings.TrimSpace(string(resp)), "\n")
	for _, line := range lines {
		if line != "" {
			databases = append(databases, line)
		}
	}

	return databases, nil
}

func (d *Dumper) dumpDatabaseSchema(dbName string) error {
	query := fmt.Sprintf("SHOW CREATE DATABASE `%s` SETTINGS format_display_secrets_in_show_and_select=1 FORMAT TSVRaw", dbName)
	resp, err := d.client.ExecuteQuery(query)
	if err != nil {
		return err
	}

	// Replace CREATE DATABASE with CREATE DATABASE IF NOT EXISTS
	createStmt := strings.Replace(string(resp), "CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1)

	filename := fmt.Sprintf("%s/%s/%s.database.sql", d.config.StorageConfig["path"], d.config.BackupName, dbName)
	return d.storage.Upload(filename, strings.NewReader(createStmt), d.config.CompressFormat, d.config.CompressLevel)
}

func (d *Dumper) Dump() error {
	// First dump database schemas
	databases, err := d.GetDatabases()
	if err != nil {
		return err
	}

	for _, db := range databases {
		if err := d.dumpDatabaseSchema(db); err != nil {
			return err
		}
	}

	// Then dump tables
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
			if err := d.dumpSchema(db, table); err != nil {
				return err
			}
			if err := d.dumpData(db, table); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *Dumper) getTables() (map[string][]string, error) {
	where := make([]string, 0, 4)
	if d.config.Databases != "" {
		where = append(where, fmt.Sprintf("match(database, '%s')", d.config.Databases))
	}
	if d.config.ExcludeDatabases != "" {
		where = append(where, fmt.Sprintf("NOT match(database, '%s')", d.config.ExcludeDatabases))
	}
	if d.config.Tables != "" {
		where = append(where, fmt.Sprintf("match(name, '%s')", d.config.Tables))
	}
	if d.config.ExcludeTables != "" {
		where = append(where, fmt.Sprintf("NOT match(name, '%s')", d.config.ExcludeTables))
	}
	query := fmt.Sprintf(`
		SELECT 
			database, 
			name 
		FROM system.tables 
		WHERE %s`, strings.Join(where, " AND "))

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

func (d *Dumper) dumpSchema(dbName, tableName string) error {
	query := fmt.Sprintf("SELECT create_table_query FROM system.tables WHERE database='%s' AND name='%s' SETTINGS format_display_secrets_in_show_and_select=1 FORMAT TSVRaw", dbName, tableName)
	d.debugf("Schema query: %s", query)
	resp, err := d.client.ExecuteQuery(query)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/%s/%s/%s.schema.sql", d.config.StorageConfig["path"], d.config.BackupName, dbName, tableName)
	return d.storage.Upload(filename, bytes.NewReader(resp), d.config.CompressFormat, d.config.CompressLevel)
}

func (d *Dumper) dumpData(dbName, tableName string) error {
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size=%d, output_format_sql_insert_table_name='`%s`.`%s`'", dbName, tableName, d.config.BatchSize, dbName, tableName)
	d.debugf("Data query: %s", query)
	body, err := d.client.ExecuteQueryStreaming(query)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := body.Close(); closeErr != nil {
			log.Printf("can't close dumpData reader body: %v", closeErr)
		}
	}()
	filename := fmt.Sprintf("%s/%s/%s/%s.data.sql", d.config.StorageConfig["path"], d.config.BackupName, dbName, tableName)
	return d.storage.Upload(filename, body, d.config.CompressFormat, d.config.CompressLevel)
}

func (d *Dumper) debugf(msg string, args ...interface{}) {
	if d.config.Debug {
		if len(args) > 0 {
			log.Printf(msg, args...)
		} else {
			log.Println(msg)
		}
	}
}
