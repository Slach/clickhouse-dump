package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Restorer struct {
	config *Config
	client *ClickHouseClient
}

func NewRestorer(config *Config) *Restorer {
	return &Restorer{
		config: config,
		client: NewClickHouseClient(config),
	}
}

func (r *Restorer) Restore() error {
	schemaFiles, err := filepath.Glob(fmt.Sprintf("%s.*.schema.sql", r.config.Database))
	if err != nil {
		return err
	}

	for _, schemaFile := range schemaFiles {
		if err := r.restoreSchema(schemaFile); err != nil {
			return err
		}
	}

	dataFiles, err := filepath.Glob(fmt.Sprintf("%s.*.data.sql", r.config.Database))
	if err != nil {
		return err
	}

	for _, dataFile := range dataFiles {
		if err := r.restoreData(dataFile); err != nil {
			return err
		}
	}

	return nil
}

func (r *Restorer) restoreSchema(schemaFile string) error {
	content, err := os.ReadFile(schemaFile)
	if err != nil {
		return err
	}

	_, err = r.client.ExecuteQuery(string(content))
	return err
}

func (r *Restorer) restoreData(dataFile string) error {
	file, err := os.Open(dataFile)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		query := scanner.Text()
		if strings.TrimSpace(query) != "" {
			_, err := r.client.ExecuteQuery(query)
			if err != nil {
				return err
			}
		}
	}

	return scanner.Err()
}
