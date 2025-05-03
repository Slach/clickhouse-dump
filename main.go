package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "clickhouse-dump",
		Usage: "Dump and restore ClickHouse tables",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "host",
				Aliases:  []string{"H"},
				Value:    "localhost",
				Usage:    "ClickHouse host",
				Required: true,
			},
			&cli.IntFlag{
				Name:     "port",
				Aliases:  []string{"p"},
				Value:    8123,
				Usage:    "ClickHouse HTTP port",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "user",
				Aliases:  []string{"u"},
				Value:    "default",
				Usage:    "ClickHouse user",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "password",
				Aliases:  []string{"P"},
				Usage:    "ClickHouse password",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "database",
				Aliases:  []string{"d"},
				Usage:    "ClickHouse database",
				Required: true,
			},
			&cli.IntFlag{
				Name:  "batch-size",
				Value: 1000000,
				Usage: "Batch size for SQL Insert statements",
			},
			&cli.StringFlag{
				Name:  "compress-format",
				Value: "gzip",
				Usage: "Compression format (gzip or zstd)",
			},
			&cli.IntFlag{
				Name:  "compress-level",
				Value: 6,
				Usage: "Compression level (1-9 for gzip, 1-22 for zstd)",
			},
		},
		Commands: []*cli.Command{
			{
				Name:   "dump",
				Usage:  "Dump ClickHouse tables schema and data",
				Action: runDumper,
			},
			{
				Name:   "restore",
				Usage:  "Restore ClickHouse tables from dumped files",
				Action: runRestorer,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runDumper(c *cli.Context) error {
	config := getConfig(c)
	dumper, err := NewDumper(config)
	if err != nil {
		return err
	}
	return dumper.Dump()
}

func runRestorer(c *cli.Context) error {
	config := getConfig(c)
	restorer := NewRestorer(config)
	return restorer.Restore()
}

func getConfig(c *cli.Context) *Config {
	return &Config{
		Host:           c.String("host"),
		Port:           c.Int("port"),
		User:           c.String("user"),
		Password:       c.String("password"),
		Database:       c.String("database"),
		BatchSize:      c.Int("batch-size"),
		CompressFormat: c.String("compress-format"),
		CompressLevel:  c.Int("compress-level"),
	}
}
