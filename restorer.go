package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/Slach/clickhouse-dump/storage"
)

// ... existing code ...

func (r *Restorer) Restore() error {
	// ... existing code ...

	schemaPrefix := filepath.Join(r.config.StoragePath, fmt.Sprintf("%s.", r.config.Database)) // Include storage path
	schemaSuffix := ".schema.sql"

	// ... rest of Restore() ...

	dataSuffix := ".data.sql"
	dataPrefix := filepath.Join(r.config.StoragePath, fmt.Sprintf("%s.", r.config.Database)) // Include storage path

	// ... rest of file ...
}
