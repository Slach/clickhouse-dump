package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestE2E(t *testing.T) {
	ctx := context.Background()

	testCases := []string{
		"default",
		"include_specific_db",
		"exclude_tables",
		"complex_pattern",
	}

	storageTypes := []string{
		"s3",
		"gcs",
		"azblob",
		"ftp",
		"sftp",
		"file",
	}

	for _, storageType := range storageTypes {
		for _, testCase := range testCases {
			t.Run(fmt.Sprintf("%s_%s", storageType, testCase), func(t *testing.T) {
				clickhouseContainer, err := startClickHouseContainer(ctx)
				require.NoError(t, err, "Failed to start ClickHouse container")
				defer func() {
					require.NoError(t, clearTestTables(ctx, t, clickhouseContainer))
					require.NoError(t, clickhouseContainer.Terminate(ctx))
				}()

				switch storageType {
				case "s3":
					testS3Storage(ctx, t, clickhouseContainer, testCase)
				case "gcs":
					testGCSStorage(ctx, t, clickhouseContainer, testCase)
				case "azblob":
					testAzureBlobStorage(ctx, t, clickhouseContainer, testCase)
				case "ftp":
					testFTPStorage(ctx, t, clickhouseContainer, testCase)
				case "sftp":
					testSFTPStorage(ctx, t, clickhouseContainer, testCase)
				case "file":
					testFileStorage(ctx, t, clickhouseContainer, testCase)
				default:
					t.Fatalf("unknown storage type: %s", storageType)
				}
			})
		}
	}
}

func startClickHouseContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"8123/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_SKIP_USER_SETUP": "1",
		},
		WaitingFor: wait.ForHTTP("/").WithPort("8123/tcp"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func testS3Storage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_backup_" + testCase
	minioContainer, err := startMinioContainer(ctx)
	require.NoError(t, err, "Failed to start Minio container")
	defer func() {
		require.NoError(t, minioContainer.Terminate(ctx))
	}()

	minioHost, err := minioContainer.Host(ctx)
	require.NoError(t, err, "Failed to get Minio host")

	minioPort, err := minioContainer.MappedPort(ctx, "9000")
	require.NoError(t, err, "Failed to get Minio port")

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":     "s3",
		"storage-bucket":   "testbucket",
		"storage-region":   "us-east-1",
		"storage-path":     "",
		"storage-host":     minioHost + ":" + minioPort.Port(),
		"storage-account":  "minioadmin",
		"storage-key":      "minioadmin",
		"debug":            "true",
	}, testCase, backupName)
}

func runMainTestScenario(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, storageFlags map[string]string, testCase string, backupName string) {
	// Clear any existing tables first
	require.NoError(t, clearTestTables(ctx, t, clickhouseContainer))

	// Create fresh test tables and insert data
	require.NoError(t, createTestTables(ctx, t, clickhouseContainer))

	// Get ClickHouse connection details
	host, err := clickhouseContainer.Host(ctx)
	require.NoError(t, err)
	port, err := clickhouseContainer.MappedPort(ctx, "8123")
	require.NoError(t, err)

	// Define test cases
	testCases := map[string]struct {
		databases        string
		excludeDatabases string
		tables           string
		excludeTables    string
		expectedFiles    []string
		expectedRestored []string
		expectedMissing  []string
	}{
		"default": {
			databases:        ".*",
			excludeDatabases: "system|INFORMATION_SCHEMA|information_schema|system_db",
			tables:           ".*",
			excludeTables:    "",
			expectedFiles: []string{
				fmt.Sprintf("%s/test_db1/users.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/users.data.sql", backupName),
				fmt.Sprintf("%s/test_db1/logs.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/logs.data.sql", backupName),
				fmt.Sprintf("%s/test_db1/audit_log.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/audit_log.data.sql", backupName),
				fmt.Sprintf("%s/test_db2/products.schema.sql", backupName),
				fmt.Sprintf("%s/test_db2/products.data.sql", backupName),
				fmt.Sprintf("%s/test_db2/inventory.schema.sql", backupName),
				fmt.Sprintf("%s/test_db2/inventory.data.sql", backupName),
				fmt.Sprintf("%s/test_db3/metrics.schema.sql", backupName),
				fmt.Sprintf("%s/test_db3/metrics.data.sql", backupName),
				fmt.Sprintf("%s/logs_2023/events.schema.sql", backupName),
				fmt.Sprintf("%s/logs_2023/events.data.sql", backupName),
				fmt.Sprintf("%s/logs_2024/events.schema.sql", backupName),
				fmt.Sprintf("%s/logs_2024/events.data.sql", backupName),
			},
			expectedRestored: []string{
				"test_db1.users",
				"test_db1.logs",
				"test_db1.audit_log",
				"test_db2.products",
				"test_db2.inventory",
				"test_db3.metrics",
				"logs_2023.events",
				"logs_2024.events",
			},
			expectedMissing: []string{
				"system_db.settings",
			},
		},
		"include_specific_db": {
			databases:        "test_db1|logs_2024",
			excludeDatabases: "",
			tables:           ".*",
			excludeTables:    "",
			expectedFiles: []string{
				fmt.Sprintf("%s/test_db1/users.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/users.data.sql", backupName),
				fmt.Sprintf("%s/test_db1/logs.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/logs.data.sql", backupName),
				fmt.Sprintf("%s/test_db1/audit_log.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/audit_log.data.sql", backupName),
				fmt.Sprintf("%s/test_db1/events.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/events.data.sql", backupName),
			},
			expectedRestored: []string{
				"test_db1.users",
				"test_db1.logs",
				"test_db1.audit_log",
				"logs_2024.events",
			},
			expectedMissing: []string{
				"test_db2.products",
				"test_db2.inventory",
				"test_db3.metrics",
				"logs_2023.events",
				"system_db.settings",
			},
		},
		"exclude_tables": {
			databases:        ".*",
			excludeDatabases: "system_db",
			tables:           ".*",
			excludeTables:    "logs|metrics",
			expectedFiles: []string{
				fmt.Sprintf("%s/test_db1/users.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/users.data.sql", backupName),
				fmt.Sprintf("%s/test_db1/audit_log.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/audit_log.data.sql", backupName),
				fmt.Sprintf("%s/test_db2/products.schema.sql", backupName),
				fmt.Sprintf("%s/test_db2/products.data.sql", backupName),
				fmt.Sprintf("%s/test_db2/inventory.schema.sql", backupName),
				fmt.Sprintf("%s/test_db2/inventory.data.sql", backupName),
			},
			expectedRestored: []string{
				"test_db1.users",
				"test_db1.audit_log",
				"test_db2.products",
				"test_db2.inventory",
			},
			expectedMissing: []string{
				"test_db1.logs",
				"test_db3.metrics",
				"logs_2023.events",
				"logs_2024.events",
				"system_db.settings",
			},
		},
		"complex_pattern": {
			databases:        "test_db[12]|logs_202[34]",
			excludeDatabases: "logs_2023",
			tables:           "users|products|events",
			excludeTables:    "",
			expectedFiles: []string{
				"test_db1.users.schema.sql",
				"test_db1.users.data.sql",
				"test_db2.products.schema.sql",
				"test_db2.products.data.sql",
				"logs_2024.events.schema.sql",
				"logs_2024.events.data.sql",
			},
			expectedRestored: []string{
				"test_db1.users",
				"test_db2.products",
				"logs_2024.events",
			},
			expectedMissing: []string{
				"test_db1.logs",
				"test_db1.audit_log",
				"test_db2.inventory",
				"test_db3.metrics",
				"logs_2023.events",
				"system_db.settings",
			},
		},
	}

	tc := testCases[testCase]
	if tc.expectedFiles == nil {
		t.Fatalf("unknown test case: %s", testCase)
	}

	// Build args from test case and storage flags
	args := []string{
		"clickhouse-dump",
		"--host=" + host,
		"--port=" + strconv.Itoa(port.Int()),
		"--user=default",
		"--databases=" + tc.databases,
		"--exclude-databases=" + tc.excludeDatabases,
		"--tables=" + tc.tables,
		"--exclude-tables=" + tc.excludeTables,
		"--batch-size=100000",
		"--compress-format=gzip",
		"--compress-level=6",
	}
	storageFlagsSlice := make([]string, 0)

	for paramName, paramValue := range storageFlags {
		storageFlagsSlice = append(storageFlagsSlice, fmt.Sprintf("--%s=%s", paramName, paramValue))
	}
	args = append(args, storageFlagsSlice...)

	// Test 1: Dump
	dumpArgs := append(args, "dump", backupName)
	dumpErr := app.Run(dumpArgs)
	require.NoError(t, dumpErr, "fail to execute dump command %v", dumpArgs)
	// Verify dump files were created
	if storageFlags["storage-type"] == "file" {
		require.NoError(t, verifyDumpResults(storageFlags["storage-path"], tc.expectedFiles))
	}

	// Clear tables before restore
	require.NoError(t, clearTestTables(ctx, t, clickhouseContainer))

	// Test 2: Restore
	restoreArgs := append(args, "restore", backupName)
	restoreErr := app.Run(restoreArgs)
	require.NoError(t, restoreErr, "fail to execute dump command %v", restoreArgs)

	for _, table := range tc.expectedRestored {
		_, existsErr := executeTestQueryWithResult(ctx, t, clickhouseContainer, fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
		require.NoError(t, existsErr, "table %s should exist after restore", table)

		// Then verify test data was restored correctly
		var expectedData string
		switch table {
		case "test_db1.users":
			expectedData = "1\tAlice\n2\tBob\n"
		case "test_db1.logs":
			expectedData = "1\tlog entry 1\n2\tlog entry 2\n"
		case "test_db1.audit_log":
			expectedData = "1\tlogin\n2\tlogout\n"
		case "test_db2.products":
			expectedData = "1\tProduct A\n2\tProduct B\n"
		case "test_db2.inventory":
			expectedData = "1\tItem 1\n2\tItem 2\n"
		case "test_db3.metrics":
			expectedData = "1\tmetric1\n2\tmetric2\n"
		case "logs_2023.events":
			expectedData = "1\tevent1\n2\tevent2\n"
		case "logs_2024.events":
			expectedData = "1\tevent1\n2\tevent2\n"
		default:
			t.Fatalf("unexpected table in verification: %s", table)
		}

		verifyDataErr := verifyTestData(ctx, t, clickhouseContainer, table, expectedData)
		require.NoError(t, verifyDataErr, "table %s data verification failed", table)
	}

	// Verify excluded tables were NOT restored
	for _, table := range tc.expectedMissing {
		_, err := executeTestQueryWithResult(ctx, t, clickhouseContainer, fmt.Sprintf("SELECT * FROM %s", table))
		require.Error(t, err, "table %s should not exist after restore", table)
	}
}

func testGCSStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_gcs_" + testCase
	gcsContainer, err := startFakeGCSContainer(ctx)
	require.NoError(t, err, "Failed to start fake GCS container")
	defer func() {
		require.NoError(t, gcsContainer.Terminate(ctx))
	}()

	gcsHost, err := gcsContainer.Host(ctx)
	require.NoError(t, err, "Failed to get GCS host")

	gcsPort, err := gcsContainer.MappedPort(ctx, "4443")
	require.NoError(t, err, "Failed to get GCS port")

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":   "gcs",
		"storage-bucket": "testbucket",
		"storage-path":   "",
		"storage-host":   gcsHost + ":" + gcsPort.Port(),
		"debug":          "true",
	}, testCase, backupName)
}

func testAzureBlobStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_azblob_" + testCase
	azuriteContainer, err := startAzuriteContainer(ctx)
	require.NoError(t, err, "Failed to start Azurite container")
	defer func() {
		require.NoError(t, azuriteContainer.Terminate(ctx))
	}()

	azuriteHost, err := azuriteContainer.Host(ctx)
	require.NoError(t, err, "Failed to get Azurite host")

	azuritePort, err := azuriteContainer.MappedPort(ctx, "10000")
	require.NoError(t, err, "Failed to get Azurite port")

	// For Azurite (local testing), we need to use special credentials and endpoint
	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":      "azblob",
		"storage-account":   "devstoreaccount1",
		"storage-key":       "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
		"storage-container": "testcontainer",
		"storage-path":      "",
		"storage-endpoint":  fmt.Sprintf("http://%s:%s/devstoreaccount1", azuriteHost, azuritePort.Port()),
		"debug":            "true",
	}, testCase, backupName)
}

func testFTPStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_ftp_" + testCase
	ftpContainer, err := startFTPContainer(ctx)
	require.NoError(t, err, "Failed to start FTP container")
	defer func() {
		require.NoError(t, ftpContainer.Terminate(ctx))
	}()

	ftpHost, err := ftpContainer.Host(ctx)
	require.NoError(t, err, "Failed to get FTP host")

	ftpPort, err := ftpContainer.MappedPort(ctx, "21")
	require.NoError(t, err, "Failed to get FTP port")

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":     "ftp",
		"storage-host":     ftpHost + ":" + ftpPort.Port(),
		"storage-user":     "testuser",
		"storage-password": "testpass",
		"storage-path":     "",
		"debug":           "true",
	}, testCase, backupName)
}

func testSFTPStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_sftp_" + testCase
	sftpContainer, err := startSFTPContainer(ctx)
	require.NoError(t, err, "Failed to start SFTP container")
	defer func() {
		require.NoError(t, sftpContainer.Terminate(ctx))
	}()

	sftpHost, err := sftpContainer.Host(ctx)
	require.NoError(t, err, "Failed to get SFTP host")

	sftpPort, err := sftpContainer.MappedPort(ctx, "22")
	require.NoError(t, err, "Failed to get SFTP port")

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":     "sftp",
		"storage-host":     sftpHost + ":" + sftpPort.Port(),
		"storage-user":     "testuser",
		"storage-password": "testpass",
		"storage-path":     "",
		"debug":           "true",
	}, testCase, backupName)
}

func testFileStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	// Create temp directory for test
	tempDir := t.TempDir()

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type": "file",
		"storage-path": tempDir,
		"debug":       "true",
	}, testCase, "test_file_"+testCase)
}

func startMinioContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ACCESS_KEY": "minioadmin",
			"MINIO_SECRET_KEY": "minioadmin",
		},
		Cmd:        []string{"server", "/data"},
		WaitingFor: wait.ForHTTP("/minio/health/ready").WithPort("9000/tcp"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func startFakeGCSContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server:latest",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-scheme", "http"},
		WaitingFor:   wait.ForHTTP("/").WithPort("4443/tcp"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func startAzuriteContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "mcr.microsoft.com/azure-storage/azurite:latest",
		ExposedPorts: []string{"10000/tcp"},
		Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0"},
		WaitingFor:   wait.ForListeningPort("10000/tcp"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func startFTPContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "fauria/vsftpd:latest",
		ExposedPorts: []string{"21/tcp"},
		Env: map[string]string{
			"FTP_USER":     "testuser",
			"FTP_PASS":     "testpass",
			"PASV_ADDRESS": "127.0.0.1",
		},
		WaitingFor: wait.ForListeningPort("21/tcp"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func startSFTPContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "panubo/sshd:latest",
		ExposedPorts: []string{"22/tcp"},
		Env: map[string]string{
			"SSH_USERS": "testuser:1001:testpass",
		},
		WaitingFor: wait.ForListeningPort("22/tcp"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func createTestTables(ctx context.Context, t *testing.T, container testcontainers.Container) error {
	queries := []string{
		`CREATE DATABASE IF NOT EXISTS test_db1`,
		`CREATE DATABASE IF NOT EXISTS test_db2`,
		`CREATE DATABASE IF NOT EXISTS test_db3`,
		`CREATE DATABASE IF NOT EXISTS logs_2023`,
		`CREATE DATABASE IF NOT EXISTS logs_2024`,
		`CREATE DATABASE IF NOT EXISTS system_db`, // Should be excluded by default

		// Tables in test_db1
		`CREATE TABLE test_db1.users (
			id UInt32,
			name String
		) ENGINE = MergeTree()
		ORDER BY id`,

		`CREATE TABLE test_db1.logs (
			id UInt32,
			message String
		) ENGINE = MergeTree()
		ORDER BY id`,

		`CREATE TABLE test_db1.audit_log (
			id UInt32,
			action String
		) ENGINE = MergeTree()
		ORDER BY id`,

		// Tables in test_db2
		`CREATE TABLE test_db2.products (
			id UInt32,
			name String
		) ENGINE = MergeTree()
		ORDER BY id`,

		`CREATE TABLE test_db2.inventory (
			id UInt32,
			item String
		) ENGINE = MergeTree()
		ORDER BY id`,

		// Tables in test_db3
		`CREATE TABLE test_db3.metrics (
			id UInt32,
			name String
		) ENGINE = MergeTree()
		ORDER BY id`,

		// Log databases
		`CREATE TABLE logs_2023.events (
			id UInt32,
			event String
		) ENGINE = MergeTree()
		ORDER BY id`,

		`CREATE TABLE logs_2024.events (
			id UInt32,
			event String
		) ENGINE = MergeTree()
		ORDER BY id`,

		// System tables (should be excluded by default)
		`CREATE TABLE system_db.settings (
			id UInt32,
			name String
		) ENGINE = MergeTree()
		ORDER BY id`,

		// Insert test data
		`INSERT INTO test_db1.users VALUES (1, 'Alice'), (2, 'Bob')`,
		`INSERT INTO test_db1.logs VALUES (1, 'log entry 1'), (2, 'log entry 2')`,
		`INSERT INTO test_db1.audit_log VALUES (1, 'login'), (2, 'logout')`,
		`INSERT INTO test_db2.products VALUES (1, 'Product A'), (2, 'Product B')`,
		`INSERT INTO test_db2.inventory VALUES (1, 'Item 1'), (2, 'Item 2')`,
		`INSERT INTO test_db3.metrics VALUES (1, 'metric1'), (2, 'metric2')`,
		`INSERT INTO logs_2023.events VALUES (1, 'event1'), (2, 'event2')`,
		`INSERT INTO logs_2024.events VALUES (1, 'event1'), (2, 'event2')`,
		`INSERT INTO system_db.settings VALUES (1, 'setting1'), (2, 'setting2')`,
	}

	for _, query := range queries {
		if err := executeTestQuery(ctx, t, container, query); err != nil {
			return err
		}
	}
	return nil
}

func clearTestTables(ctx context.Context, t *testing.T, container testcontainers.Container) error {
	queries := []string{
		"DROP TABLE IF EXISTS test_db1.users",
		"DROP TABLE IF EXISTS test_db1.logs",
		"DROP TABLE IF EXISTS test_db2.products",
		"DROP TABLE IF EXISTS test_db3.metrics",
		"DROP DATABASE IF EXISTS test_db3",
		"DROP DATABASE IF EXISTS test_db1",
		"DROP DATABASE IF EXISTS test_db2",
	}

	for _, query := range queries {
		if err := executeTestQuery(ctx, t, container, query); err != nil {
			return err
		}
	}
	return nil
}

func verifyTestData(ctx context.Context, t *testing.T, container testcontainers.Container, table string, expected string) error {
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id;", table)
	result, err := executeTestQueryWithResult(ctx, t, container, query)
	if err != nil {
		return err
	}

	if result != expected {
		return fmt.Errorf("unexpected result for table %s: got %q, want %q", table, result, expected)
	}

	return nil
}

func verifyDumpResults(tempDir string, expectedFiles []string) error {
	files, err := os.ReadDir(tempDir)
	if err != nil {
		return err
	}

	foundFiles := make(map[string]bool)
	for _, file := range files {
		foundFiles[file.Name()] = true
	}

	for _, expected := range expectedFiles {
		if !foundFiles[expected] {
			return fmt.Errorf("expected file %s not found in dump", expected)
		}
	}

	return nil
}

func executeTestQuery(ctx context.Context, t *testing.T, container testcontainers.Container, query string) error {
	host, err := container.Host(ctx)
	if err != nil {
		return err
	}

	port, err := container.MappedPort(ctx, "8123")
	if err != nil {
		return err
	}

	urlStr := fmt.Sprintf("http://%s:%s/?query=%s", host, port.Port(), url.QueryEscape(query))
	resp, err := http.Post(urlStr, "text/plain", nil)
	if err != nil {
		return err
	}
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func executeTestQueryWithResult(ctx context.Context, t *testing.T, container testcontainers.Container, query string) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}

	port, err := container.MappedPort(ctx, "8123")
	if err != nil {
		return "", err
	}

	clickhouseUrl := fmt.Sprintf("http://%s:%s/?query=%s", host, port.Port(), url.QueryEscape(query))
	resp, err := http.Get(clickhouseUrl)
	if err != nil {
		return "", err
	}
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
