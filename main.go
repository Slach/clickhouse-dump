package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "clickhouse-dump",
		Usage: "Dump and restore ClickHouse tables to/from remote storage",
		Flags: []cli.Flag{
			// ... existing flags ...

			// Add new storage-path flag
			&cli.StringFlag{
				Name:    "storage-path",
				Usage:   "Base path on remote storage for upload/download",
				EnvVars: []string{"STORAGE_PATH"},
				Value:   "", // Default to empty (root of storage)
			},
		},
		Commands: []*cli.Command{
			// ... existing commands ...
		},
	}

	// ... rest of main() ...
}

// ... rest of file ...

func getConfig(c *cli.Context) (*Config, error) {
	config := &Config{
		// ... existing fields ...
		StoragePath:    strings.TrimPrefix(c.String("storage-path"), "/"), // Ensure no leading slash
	}

	// ... rest of getConfig() ...
}
