package main

import (
	"bytes"
	"fmt"
	"github.com/Slach/clickhouse-dump/storage"
	"path/filepath"
	"strings"
)

// ... existing code ...

func (d *Dumper) dumpSchema(table string) error {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", table)
	resp, err := d.client.ExecuteQuery(query)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s.%s.schema.sql", d.config.Database, table)
	remotePath := filepath.Join(d.config.StoragePath, filename)
	return d.storage.Upload(remotePath, bytes.NewReader(resp), d.config.CompressFormat, d.config.CompressLevel)
}

func (d *Dumper) dumpData(table string) error {
	query := fmt.Sprintf("SELECT * FROM %s FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size=%d", table, d.config.BatchSize)
	body, err := d.client.ExecuteQueryStreaming(query)
	if err != nil {
		return err
	}
	defer body.Close()

	filename := fmt.Sprintf("%s.%s.data.sql", d.config.Database, table)
	remotePath := filepath.Join(d.config.StoragePath, filename)
	return d.storage.Upload(remotePath, body, d.config.CompressFormat, d.config.CompressLevel)
}
