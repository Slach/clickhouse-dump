package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestE2E(t *testing.T) {
	ctx := context.Background()

	// Start ClickHouse container
	clickhouseContainer, err := startClickHouseContainer(ctx)
	require.NoError(t, err, "Failed to start ClickHouse container")
	defer func() {
		require.NoError(t, clickhouseContainer.Terminate(ctx))
	}()

	// Run tests for different storage backends
	t.Run("S3", func(t *testing.T) { testS3Storage(ctx, t, clickhouseContainer) })
	t.Run("GCS", func(t *testing.T) { testGCSStorage(ctx, t, clickhouseContainer) })
	t.Run("AzureBlob", func(t *testing.T) { testAzureBlobStorage(ctx, t, clickhouseContainer) })
	t.Run("FTP", func(t *testing.T) { testFTPStorage(ctx, t, clickhouseContainer) })
	t.Run("SFTP", func(t *testing.T) { testSFTPStorage(ctx, t, clickhouseContainer) })
	t.Run("File", func(t *testing.T) { testFileStorage(ctx, t, clickhouseContainer) })
}

func startClickHouseContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:latest",
		ExposedPorts: []string{"8123/tcp"},
		WaitingFor:   wait.ForHTTP("/").WithPort("8123/tcp"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func testS3Storage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	minioContainer, err := startMinioContainer(ctx)
	require.NoError(t, err, "Failed to start Minio container")
	defer func() {
		require.NoError(t, minioContainer.Terminate(ctx))
	}()

	minioHost, err := minioContainer.Host(ctx)
	require.NoError(t, err, "Failed to get Minio host")

	minioPort, err := minioContainer.MappedPort(ctx, "9000")
	require.NoError(t, err, "Failed to get Minio port")

	runMainTestScenario(ctx, t, clickhouseContainer, []string{
		"--storage-type", "s3",
		"--storage-config", fmt.Sprintf("bucket=testbucket,region=us-east-1,endpoint=http://%s:%s", minioHost, minioPort.Port()),
	})
}

func runMainTestScenario(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, storageArgs []string) {
	// Create test tables and insert data
	require.NoError(t, createTestTables(ctx, t, clickhouseContainer))

	// Test 1: Default dump (should get all tables except system)
	err := runClickHouseDump(ctx, t, clickhouseContainer,
		append([]string{
			"dump",
			"--compress-format", "gzip",
			"--compress-level", "6",
		}, storageArgs...)...,
	)
	require.NoError(t, err, "Failed to dump data")
	
	// Clear tables
	require.NoError(t, clearTestTables(ctx, t, clickhouseContainer))

	// Restore with same filters
	err = runClickHouseDump(ctx, t, clickhouseContainer,
		append([]string{
			"restore",
		}, storageArgs...)...,
	)
	require.NoError(t, err, "Failed to restore data")

	// Verify only non-system tables were restored
	require.NoError(t, verifyTestData(ctx, t, clickhouseContainer, "test_db1.users", "1\tAlice\n2\tBob\n"))
	require.NoError(t, verifyTestData(ctx, t, clickhouseContainer, "test_db1.logs", "1\tlog entry 1\n2\tlog entry 2\n"))
	require.NoError(t, verifyTestData(ctx, t, clickhouseContainer, "test_db2.products", "1\tProduct A\n2\tProduct B\n"))
	
	// Verify system tables were NOT restored
	_, err = executeQueryWithResult(ctx, t, clickhouseContainer, "SELECT * FROM system.metrics")
	require.Error(t, err, "system.metrics should not exist after restore")
}

func testGCSStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	gcsContainer, err := startFakeGCSContainer(ctx)
	require.NoError(t, err, "Failed to start fake GCS container")
	defer func() {
		require.NoError(t, gcsContainer.Terminate(ctx))
	}()

	gcsHost, err := gcsContainer.Host(ctx)
	require.NoError(t, err, "Failed to get GCS host")

	gcsPort, err := gcsContainer.MappedPort(ctx, "4443")
	require.NoError(t, err, "Failed to get GCS port")

	runMainTestScenario(ctx, t, clickhouseContainer, []string{
		"--storage-type", "gcs",
		"--storage-config", fmt.Sprintf("bucket=testbucket,endpoint=http://%s:%s", gcsHost, gcsPort.Port()),
	})
}

func testAzureBlobStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	azuriteContainer, err := startAzuriteContainer(ctx)
	require.NoError(t, err, "Failed to start Azurite container")
	defer func() {
		require.NoError(t, azuriteContainer.Terminate(ctx))
	}()

	azuriteHost, err := azuriteContainer.Host(ctx)
	require.NoError(t, err, "Failed to get Azurite host")

	azuritePort, err := azuriteContainer.MappedPort(ctx, "10000")
	require.NoError(t, err, "Failed to get Azurite port")

	runMainTestScenario(ctx, t, clickhouseContainer, []string{
		"--storage-type", "azblob",
		"--storage-config", fmt.Sprintf("account=devstoreaccount1,key=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==,container=testcontainer,endpoint=http://%s:%s/devstoreaccount1", azuriteHost, azuritePort.Port()),
	})
}

func testFTPStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	ftpContainer, err := startFTPContainer(ctx)
	require.NoError(t, err, "Failed to start FTP container")
	defer func() {
		require.NoError(t, ftpContainer.Terminate(ctx))
	}()

	ftpHost, err := ftpContainer.Host(ctx)
	require.NoError(t, err, "Failed to get FTP host")

	ftpPort, err := ftpContainer.MappedPort(ctx, "21")
	require.NoError(t, err, "Failed to get FTP port")

	runMainTestScenario(ctx, t, clickhouseContainer, []string{
		"--storage-type", "ftp",
		"--storage-config", fmt.Sprintf("host=%s:%s,user=testuser,password=testpass", ftpHost, ftpPort.Port()),
	})
}

func testSFTPStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	sftpContainer, err := startSFTPContainer(ctx)
	require.NoError(t, err, "Failed to start SFTP container")
	defer func() {
		require.NoError(t, sftpContainer.Terminate(ctx))
	}()

	sftpHost, err := sftpContainer.Host(ctx)
	require.NoError(t, err, "Failed to get SFTP host")

	sftpPort, err := sftpContainer.MappedPort(ctx, "22")
	require.NoError(t, err, "Failed to get SFTP port")

	runMainTestScenario(ctx, t, clickhouseContainer, []string{
		"--storage-type", "sftp",
		"--storage-config", fmt.Sprintf("host=%s:%s,user=testuser,password=testpass", sftpHost, sftpPort.Port()),
	})
}

func testFileStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	// Create temp directory for test
	tempDir := t.TempDir()

	runMainTestScenario(ctx, t, clickhouseContainer, []string{
		"--storage-type", "file",
		"--storage-path", tempDir,
	})
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

func runClickHouseDump(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, args ...string) error {
	clickhouseHost, err := clickhouseContainer.Host(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse host: %v", err)
	}

	clickhousePort, err := clickhouseContainer.MappedPort(ctx, "8123")
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse port: %v", err)
	}

	allArgs := append([]string{
		"--host", clickhouseHost,
		"--port", clickhousePort.Port(),
		"--user", "default",
		"--password", "",
		"--database", "default",
	}, args...)

	cmd := exec.Command("./clickhouse-dump", allArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("clickhouse-dump failed: %v\nOutput: %s", err, output)
	}

	return nil
}

func createTestTables(ctx context.Context, t *testing.T, container testcontainers.Container) error {
	queries := []string{
		`CREATE DATABASE IF NOT EXISTS test_db1`,
		`CREATE DATABASE IF NOT EXISTS test_db2`,
		`CREATE DATABASE IF NOT EXISTS system`, // Will be excluded by default

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

		// Tables in test_db2
		`CREATE TABLE test_db2.products (
			id UInt32,
			name String
		) ENGINE = MergeTree()
		ORDER BY id`,

		// System table (should be excluded)
		`CREATE TABLE system.metrics (
			id UInt32,
			name String
		) ENGINE = MergeTree()
		ORDER BY id`,

		// Insert test data
		`INSERT INTO test_db1.users VALUES (1, 'Alice'), (2, 'Bob')`,
		`INSERT INTO test_db1.logs VALUES (1, 'log entry 1'), (2, 'log entry 2')`,
		`INSERT INTO test_db2.products VALUES (1, 'Product A'), (2, 'Product B')`,
		`INSERT INTO system.metrics VALUES (1, 'metric1'), (2, 'metric2')`,
	}

	for _, query := range queries {
		if err := executeQuery(ctx, t, container, query); err != nil {
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
		"DROP TABLE IF EXISTS system.metrics",
		"DROP DATABASE IF EXISTS test_db1",
		"DROP DATABASE IF EXISTS test_db2",
	}

	for _, query := range queries {
		if err := executeQuery(ctx, t, container, query); err != nil {
			return err
		}
	}
	return nil
}

func verifyTestData(ctx context.Context, t *testing.T, container testcontainers.Container, table string, expected string) error {
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id;", table)
	result, err := executeQueryWithResult(ctx, t, container, query)
	if err != nil {
		return err
	}

	if result != expected {
		return fmt.Errorf("unexpected result for table %s: got %q, want %q", table, result, expected)
	}

	return nil
}

func verifyDumpResults(ctx context.Context, t *testing.T, container testcontainers.Container, tempDir string, expectedFiles []string) error {
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

func executeQuery(ctx context.Context, t *testing.T, container testcontainers.Container, query string) error {
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

func executeQueryWithResult(ctx context.Context, t *testing.T, container testcontainers.Container, query string) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}

	port, err := container.MappedPort(ctx, "8123")
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("http://%s:%s/?query=%s", host, port.Port(), url.QueryEscape(query))
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer func() {
		require.NoError(t, resp.Body.Close())
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
