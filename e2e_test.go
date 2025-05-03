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

	// Create a test table and insert some data
	require.NoError(t, createTestTable(ctx, t, clickhouseContainer))

	// Run clickhouse-dump to back up the data
	err = runClickHouseDump(ctx, t, clickhouseContainer,
		"dump",
		"--storage-type", "s3",
		"--storage-config", fmt.Sprintf("bucket=testbucket,region=us-east-1,endpoint=http://%s:%s", minioHost, minioPort.Port()),
		"--compress-format", "gzip",
		"--compress-level", "6",
	)
	require.NoError(t, err, "Failed to dump data")

	// Clear the test table
	require.NoError(t, clearTestTable(ctx, t, clickhouseContainer))

	// Run clickhouse-dump to restore the data
	err = runClickHouseDump(ctx, t, clickhouseContainer,
		"restore",
		"--storage-type", "s3",
		"--storage-config", fmt.Sprintf("bucket=testbucket,region=us-east-1,endpoint=http://%s:%s", minioHost, minioPort.Port()),
	)
	require.NoError(t, err, "Failed to restore data")

	// Verify the restored data
	require.NoError(t, verifyTestData(ctx, t, clickhouseContainer))
}

func testGCSStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	gcsContainer, err := startFakeGCSContainer(ctx)
	require.NoError(t, err, "Failed to start fake GCS container")
	defer func() {
		require.NoError(t, gcsContainer.Terminate(ctx))
	}()

	// TODO: Implement GCS storage test
}

func testAzureBlobStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	azuriteContainer, err := startAzuriteContainer(ctx)
	require.NoError(t, err, "Failed to start Azurite container")
	defer func() {
		require.NoError(t, azuriteContainer.Terminate(ctx))
	}()

	// TODO: Implement Azure Blob storage test
}

func testFTPStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	ftpContainer, err := startFTPContainer(ctx)
	require.NoError(t, err, "Failed to start FTP container")
	defer func() {
		require.NoError(t, ftpContainer.Terminate(ctx))
	}()

	// TODO: Implement FTP storage test
}

func testSFTPStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	sftpContainer, err := startSFTPContainer(ctx)
	require.NoError(t, err, "Failed to start SFTP container")
	defer func() {
		require.NoError(t, sftpContainer.Terminate(ctx))
	}()

	// TODO: Implement SFTP storage test
}

func testFileStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container) {
	// Create temp directory for test
	tempDir := t.TempDir()

	// Create a test table and insert some data
	require.NoError(t, createTestTable(ctx, t, clickhouseContainer))

	// Run clickhouse-dump to back up the data
	err := runClickHouseDump(ctx, t, clickhouseContainer,
		"dump",
		"--storage-type", "file",
		"--storage-path", tempDir,
		"--compress-format", "gzip",
		"--compress-level", "6",
	)
	require.NoError(t, err, "Failed to dump data")

	// Verify files were created
	files, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Greater(t, len(files), 0, "No files were created in temp dir")

	// Clear the test table
	require.NoError(t, clearTestTable(ctx, t, clickhouseContainer))

	// Run clickhouse-dump to restore the data
	err = runClickHouseDump(ctx, t, clickhouseContainer,
		"restore",
		"--storage-type", "file",
		"--storage-path", tempDir,
	)
	require.NoError(t, err, "Failed to restore data")

	// Verify the restored data
	require.NoError(t, verifyTestData(ctx, t, clickhouseContainer))
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

func createTestTable(ctx context.Context, t *testing.T, container testcontainers.Container) error {
	query := `
		CREATE TABLE test_table (
			id UInt32,
			name String
		) ENGINE = MergeTree()
		ORDER BY id;

		INSERT INTO test_table (id, name) VALUES
		(1, 'Alice'),
		(2, 'Bob'),
		(3, 'Charlie');
	`
	return executeQuery(ctx, t, container, query)
}

func clearTestTable(ctx context.Context, t *testing.T, container testcontainers.Container) error {
	query := "TRUNCATE TABLE test_table;"
	return executeQuery(ctx, t, container, query)
}

func verifyTestData(ctx context.Context, t *testing.T, container testcontainers.Container) error {
	query := "SELECT * FROM test_table ORDER BY id;"
	result, err := executeQueryWithResult(ctx, t, container, query)
	if err != nil {
		return err
	}

	expected := "1\tAlice\n2\tBob\n3\tCharlie\n"
	if result != expected {
		return fmt.Errorf("unexpected result: got %s, want %s", result, expected)
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
