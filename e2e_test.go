package main

import (
	"context"
	"fmt"
	"github.com/Slach/clickhouse-dump/storage"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func logFailMessage(msg string) string {
	ryukDisabled := os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "true"
	if ryukDisabled == false {
		msg += " But it will be automatically destroyed by testcontainers-go. To prevent this and keep the container running for inspection, set the environment variable TESTCONTAINERS_RYUK_DISABLED=true."
	}
	return msg
}

func TestE2E(t *testing.T) {
	t.Parallel() // Enable parallel execution for all subtests

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
		storageType := storageType // Capture range variable
		for _, testCase := range testCases {
			testCase := testCase // Capture range variable
			t.Run(fmt.Sprintf("%s_%s", storageType, testCase), func(t *testing.T) {
				t.Parallel() // Enable parallel execution for this subtest
				ctx := context.Background()
				clickhouseContainer, err := startClickHouseContainer(ctx, fmt.Sprintf("clickhouse-%s-%d", t.Name(), time.Now().UnixNano()))
				require.NoError(t, err, "Failed to start ClickHouse container")
				defer func() {
					if !t.Failed() {
						require.NoError(t, clearTestTables(ctx, t, clickhouseContainer))
						require.NoError(t, clickhouseContainer.Terminate(ctx))
					} else {
						t.Log(logFailMessage("Test failed, tables are not cleared and ClickHouse container continue running."))
						host, hostErr := clickhouseContainer.Host(ctx)
						port, portErr := clickhouseContainer.MappedPort(ctx, "8123/tcp")
						if hostErr == nil && portErr == nil {
							t.Logf("ClickHouse container %s is available at %s:%s", clickhouseContainer.GetContainerID(), host, port.Port())
						}
					}
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

func runMainTestScenario(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, storageFlags map[string]string, testCase string, backupName string) {
	// Clear any existing tables first
	require.NoError(t, clearTestTables(ctx, t, clickhouseContainer))

	// Create fresh test tables and insert data
	require.NoError(t, createTestTables(ctx, t, clickhouseContainer))

	// Get ClickHouse connection details
	host, err := clickhouseContainer.Host(ctx)
	require.NoError(t, err)
	port, err := clickhouseContainer.MappedPort(ctx, "8123/tcp")
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
				fmt.Sprintf("%s/test_db1.database.sql", backupName),
				fmt.Sprintf("%s/test_db2.database.sql", backupName),
				fmt.Sprintf("%s/test_db3.database.sql", backupName),
				fmt.Sprintf("%s/logs_2023.database.sql", backupName),
				fmt.Sprintf("%s/logs_2024.database.sql", backupName),
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
				fmt.Sprintf("%s/logs_2024/events.schema.sql", backupName),
				fmt.Sprintf("%s/logs_2024/events.data.sql", backupName),
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
			excludeDatabases: "",
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
				fmt.Sprintf("%s/system_db/settings.schema.sql", backupName),
				fmt.Sprintf("%s/system_db/settings.data.sql", backupName),
			},
			expectedRestored: []string{
				"test_db1.users",
				"test_db1.audit_log",
				"test_db2.products",
				"test_db2.inventory",
				"logs_2023.events",
				"logs_2024.events",
				"system_db.settings", // Verify this is NOT restored despite being in backup
			},
			expectedMissing: []string{
				"test_db1.logs",
				"test_db3.metrics",
			},
		},
		"complex_pattern": {
			databases:        "test_db[12]|logs_202[34]",
			excludeDatabases: "logs_2023",
			tables:           "users|products|events",
			excludeTables:    "",
			expectedFiles: []string{
				fmt.Sprintf("%s/test_db1/users.schema.sql", backupName),
				fmt.Sprintf("%s/test_db1/users.data.sql", backupName),
				fmt.Sprintf("%s/test_db2/products.schema.sql", backupName),
				fmt.Sprintf("%s/test_db2/products.data.sql", backupName),
				fmt.Sprintf("%s/logs_2024/events.schema.sql", backupName),
				fmt.Sprintf("%s/logs_2024/events.data.sql", backupName),
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

	compressionFormat := []string{"gzip", "zstd"}[uint(time.Now().Nanosecond()%2)]

	// Build args from test case and storage flags
	flags := []string{
		"--host=" + host,
		"--port=" + port.Port(),
		"--user=default",
		"--databases=" + tc.databases,
		"--tables=" + tc.tables,
		"--exclude-tables=" + tc.excludeTables,
		"--batch-size=100000",
		"--compress-format=" + compressionFormat,
		"--compress-level=6",
		"--parallel=3",
	}
	if tc.excludeDatabases != "" {
		flags = append(flags, "--exclude-databases="+tc.excludeDatabases)
	}
	storageFlagsSlice := make([]string, 0)

	for paramName, paramValue := range storageFlags {
		storageFlagsSlice = append(storageFlagsSlice, fmt.Sprintf("--%s=%s", paramName, paramValue))
	}
	flags = append(flags, storageFlagsSlice...)

	if _, isDebug := os.LookupEnv("DEBUG"); isDebug {
		flags = append(flags, "--debug")
	}

	// Test 1: Dump
	dumpArgs := append([]string{"clickhouse-dump", "dump"}, flags...)
	dumpArgs = append(dumpArgs, backupName)
	t.Logf("dumpArgs=%#v", dumpArgs)
	dumpErr := app.Run(ctx, dumpArgs)
	require.NoError(t, dumpErr, "fail to execute dump command %v", dumpArgs)
	// Verify dump files were created
	if storageFlags["storage-type"] == "file" {
		require.NoError(t, verifyDumpResults(t, storageFlags["storage-path"], tc.expectedFiles))
	}

	// Clear tables before restore
	require.NoError(t, clearTestTables(ctx, t, clickhouseContainer))

	// Test 2: Restore
	restoreArgs := append([]string{"clickhouse-dump", "restore"}, flags...)
	restoreArgs = append(restoreArgs, backupName)
	restoreErr := app.Run(ctx, restoreArgs)
	t.Logf("restoreArgs=%#v", restoreArgs)
	require.NoError(t, restoreErr, "fail to execute restore command %v", restoreArgs)

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
		case "system_db.settings":
			expectedData = "1\tsetting1\n2\tsetting2\n"
		default:
			t.Fatalf("unexpected table in verification: %s", table)
		}

		verifyDataErr := verifyTestData(ctx, t, clickhouseContainer, table, expectedData)
		require.NoError(t, verifyDataErr, "table %s data verification failed", table)
	}

	// Verify excluded tables were NOT restored
	for _, table := range tc.expectedMissing {
		_, notExistsErr := executeTestQueryWithResult(ctx, t, clickhouseContainer, fmt.Sprintf("SELECT * FROM %s", table))
		require.Error(t, notExistsErr, "table %s should not exist after restore", table)
	}
}

func testS3Storage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_backup_" + testCase
	minioContainer, err := startMinioContainer(ctx, fmt.Sprintf("minio-%s-%d", t.Name(), time.Now().UnixNano()))
	require.NoError(t, err, "Failed to start Minio container")
	defer func() {
		if !t.Failed() {
			require.NoError(t, minioContainer.Terminate(ctx))
		} else {
			t.Log(logFailMessage("Test failed, Minio container continue running."))
			host, hostErr := minioContainer.Host(ctx)
			port, portErr := minioContainer.MappedPort(ctx, "9000/tcp")
			if hostErr == nil && portErr == nil {
				t.Logf("Minio container %s is available at http://%s:%s (MINIO_ROOT_USER: minioadmin, MINIO_ROOT_PASSWORD: minioadmin)", minioContainer.GetContainerID(), host, port.Port())
			}
		}
	}()

	minioHost, err := minioContainer.Host(ctx)
	require.NoError(t, err, "Failed to get Minio host")

	minioPort, err := minioContainer.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err, "Failed to get Minio port")

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":     "s3",
		"storage-bucket":   "testbucket",
		"storage-region":   "us-east-1",
		"storage-path":     "/",
		"storage-endpoint": "http://" + minioHost + ":" + minioPort.Port(),
		"storage-account":  "minioadmin",
		"storage-key":      "minioadmin",
	}, testCase, backupName)
}

func testGCSStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_gcs_" + testCase
	gcsContainer, err := startFakeGCSContainer(ctx, fmt.Sprintf("fakegcs-%s-%d", t.Name(), time.Now().UnixNano()))
	require.NoError(t, err, "Failed to start fake GCS container")
	defer func() {
		if !t.Failed() {
			require.NoError(t, gcsContainer.Terminate(ctx))
		} else {
			t.Log(logFailMessage("Test failed, fake GCS container continue running."))
			host, hostErr := gcsContainer.Host(ctx)
			port, portErr := gcsContainer.MappedPort(ctx, "4443/tcp")
			if hostErr == nil && portErr == nil {
				t.Logf("Fake GCS container %s is available at http://%s:%s", gcsContainer.GetContainerID(), host, port.Port())
			}
		}
	}()

	gcsHost, err := gcsContainer.Host(ctx)
	require.NoError(t, err, "Failed to get GCS host")

	gcsPort, err := gcsContainer.MappedPort(ctx, "4443/tcp")
	require.NoError(t, err, "Failed to get GCS port")

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":     "gcs",
		"storage-bucket":   "testbucket",
		"storage-path":     "/",
		"storage-endpoint": "http://" + gcsHost + ":" + gcsPort.Port(), // fake-gcs-server uses http
	}, testCase, backupName)
}

func testAzureBlobStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_azblob_" + testCase
	azuriteContainer, err := startAzuriteContainer(ctx, fmt.Sprintf("azurite-%s-%d", t.Name(), time.Now().UnixNano()))
	require.NoError(t, err, "Failed to start Azurite container")
	defer func() {
		if !t.Failed() {
			require.NoError(t, azuriteContainer.Terminate(ctx))
		} else {
			t.Log(logFailMessage("Test failed, Azurite container continue running."))
			host, hostErr := azuriteContainer.Host(ctx)
			port, portErr := azuriteContainer.MappedPort(ctx, "10000/tcp")
			if hostErr == nil && portErr == nil {
				t.Logf("Azurite container %s is available. Blob endpoint: http://%s:%s/devstoreaccount1", azuriteContainer.GetContainerID(), host, port.Port())
			}
		}
	}()

	azuriteHost, err := azuriteContainer.Host(ctx)
	require.NoError(t, err, "Failed to get Azurite host")

	azuritePort, err := azuriteContainer.MappedPort(ctx, "10000/tcp")
	require.NoError(t, err, "Failed to get Azurite port")

	// Get Azurite endpoint
	endpoint := fmt.Sprintf("http://%s:%s/devstoreaccount1", azuriteHost, azuritePort.Port())
	t.Logf("Using Azurite endpoint: %s", endpoint)

	// For Azurite (local testing) use special credentials and endpoint
	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":      "azblob",
		"storage-account":   "devstoreaccount1",
		"storage-key":       "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
		"storage-container": "testcontainer",
		"storage-path":      "/",
		"storage-endpoint":  endpoint,
	}, testCase, backupName)
}

func testFTPStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_ftp_" + testCase
	ftpContainer, err := startFTPContainer(ctx, t, fmt.Sprintf("ftp-%s-%d", t.Name(), time.Now().UnixNano()))
	require.NoError(t, err, "Failed to start FTP container")
	defer func() {
		if !t.Failed() {
			require.NoError(t, ftpContainer.Terminate(ctx))
		} else {
			t.Log(logFailMessage("Test failed, FTP container continue running."))
			host, hostErr := ftpContainer.Host(ctx)
			port, portErr := ftpContainer.MappedPort(ctx, "21/tcp")
			if hostErr == nil && portErr == nil {
				t.Logf("FTP container %s is available at ftp://%s:%s (user: testuser, pass: testpass)", ftpContainer.GetContainerID(), host, port.Port())
			}
		}
	}()

	ftpHost, err := ftpContainer.Host(ctx)
	require.NoError(t, err, "Failed to get FTP host")

	ftpPort, err := ftpContainer.MappedPort(ctx, "21/tcp")
	require.NoError(t, err, "Failed to get FTP port")

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":     "ftp",
		"storage-host":     ftpHost + ":" + ftpPort.Port(),
		"storage-user":     "testuser",
		"storage-password": "testpass",
		"storage-path":     "/",
	}, testCase, backupName)
}

func testSFTPStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	backupName := "test_sftp_" + testCase
	sftpContainer, err := startSFTPContainer(ctx, fmt.Sprintf("sftp-%s-%d", t.Name(), time.Now().UnixNano()))
	require.NoError(t, err, "Failed to start SFTP container")
	defer func() {
		if !t.Failed() {
			require.NoError(t, sftpContainer.Terminate(ctx))
		} else {
			t.Log(logFailMessage("Test failed, SFTP container continue running."))
			host, hostErr := sftpContainer.Host(ctx)
			port, portErr := sftpContainer.MappedPort(ctx, "22/tcp")
			if hostErr == nil && portErr == nil {
				t.Logf("SFTP container %s is available at sftp://%s:%s (user: testuser, pass: testpass)", sftpContainer.GetContainerID(), host, port.Port())
			}
		}
	}()

	sftpHost, err := sftpContainer.Host(ctx)
	require.NoError(t, err, "Failed to get SFTP host")

	sftpPort, err := sftpContainer.MappedPort(ctx, "22/tcp")
	require.NoError(t, err, "Failed to get SFTP port")

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type":     "sftp",
		"storage-host":     sftpHost + ":" + sftpPort.Port(),
		"storage-user":     "testuser",
		"storage-password": "testpass",
		"storage-path":     "upload",
	}, testCase, backupName)
}

func testFileStorage(ctx context.Context, t *testing.T, clickhouseContainer testcontainers.Container, testCase string) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "clickhouse-dump-test-file-")
	require.NoError(t, err, "Failed to create temp directory")
	defer func() {
		if !t.Failed() {
			require.NoError(t, os.RemoveAll(tempDir))
		} else {
			t.Logf("Test failed, temporary directory is not removed: %s", tempDir)
		}
	}()

	runMainTestScenario(ctx, t, clickhouseContainer, map[string]string{
		"storage-type": "file",
		"storage-path": tempDir,
	}, testCase, "test_file_"+testCase)
}

func sanitizeContainerName(name string) string {
	// Replace invalid characters with underscores
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	return replacer.Replace(name)
}

func startClickHouseContainer(ctx context.Context, containerName string) (testcontainers.Container, error) {
	clickHouseVersion := os.Getenv("CLICKHOUSE_VERSION")
	if clickHouseVersion == "" {
		clickHouseVersion = "latest"
	}
	req := testcontainers.ContainerRequest{
		Name:         sanitizeContainerName(containerName),
		Image:        "clickhouse/clickhouse-server:" + clickHouseVersion,
		ExposedPorts: []string{"8123/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_SKIP_USER_SETUP": "1",
		},
		WaitingFor: wait.ForHTTP("/").WithPort("8123/tcp").WithStartupTimeout(15 * time.Second).WithPollInterval(1 * time.Second),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
}

func startMinioContainer(ctx context.Context, containerName string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Name: sanitizeContainerName(containerName),
		// https://github.com/bitnami/containers/issues/81607
		Image:        "bitnami/minio:2025.4.22",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_DEFAULT_BUCKETS": "testbucket",
			"MINIO_ROOT_USER":       "minioadmin",
			"MINIO_ROOT_PASSWORD":   "minioadmin",
			"MINIO_SCHEME":          "http",
			"BITNAMI_DEBUG":         "true",
		},
		WaitingFor: wait.ForExec(
			[]string{
				"sh", "-c",
				"ls -la /bitnami/minio/data/testbucket/ && curl -f http://localhost:9000/minio/health/live",
			},
		).WithStartupTimeout(15 * time.Second).WithPollInterval(1 * time.Second),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
}

func startFakeGCSContainer(ctx context.Context, containerName string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Name:         sanitizeContainerName(containerName),
		Image:        "fsouza/fake-gcs-server:latest",
		ExposedPorts: []string{"4443/tcp"},
		Entrypoint:   []string{"/bin/sh"},
		Cmd:          []string{"-c", "mkdir -p /data/testbucket && fake-gcs-server -data /data -scheme http -port 4443"},
		WaitingFor: wait.ForExec([]string{"nc", "127.0.0.1", "4443", "-z"}).
			WithStartupTimeout(15 * time.Second).
			WithPollInterval(1 * time.Second),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
}

func startAzuriteContainer(ctx context.Context, containerName string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Name:         sanitizeContainerName(containerName),
		Image:        "mcr.microsoft.com/azure-storage/azurite:latest",
		ExposedPorts: []string{"10000/tcp"},
		Cmd:          []string{"azurite", "--debug", "/dev/stderr", "-l", "/data", "--blobHost", "0.0.0.0", "--blobKeepAliveTimeout", "600", "--disableTelemetry"},
		WaitingFor:   wait.ForListeningPort("10000/tcp").WithStartupTimeout(15 * time.Second),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
}

const (
	ftpMinPort = 30000
	ftpMaxPort = 39999
	rangeSize  = 20
)

var nextFtpPortOffset int32 = 0

func allocateFtpPortRange() (int, int, bool) {
	offset := int(atomic.AddInt32(&nextFtpPortOffset, rangeSize)) - rangeSize
	startPort := ftpMinPort + offset
	endPort := startPort + rangeSize - 1
	if endPort > ftpMaxPort {
		return 0, 0, false // Out of ports
	}
	return startPort, endPort, true
}

func startFTPContainer(ctx context.Context, t *testing.T, containerName string) (testcontainers.Container, error) {
	// Generate ports for PASV range based on test name hash to avoid conflicts in parallel tests
	minPort, maxPort, ok := allocateFtpPortRange()
	if !ok {
		t.Fatal("can't allocate FTP port range")
	}

	exposedPorts := []string{
		"21/tcp",
	}
	// Dynamically expose the ports
	for port := minPort; port <= maxPort; port++ {
		exposedPorts = append(exposedPorts, fmt.Sprintf("%d:%d/tcp", port, port))
	}

	req := testcontainers.ContainerRequest{
		Name:         sanitizeContainerName(containerName),
		Image:        "fauria/vsftpd:latest",
		ExposedPorts: exposedPorts,
		Env: map[string]string{
			"FTP_USER":      "testuser",
			"FTP_PASS":      "testpass",
			"PASV_ENABLE":   "YES",
			"PASV_ADDRESS":  "0.0.0.0", // Use 0.0.0.0 to allow connections from any IP
			"PASV_MIN_PORT": strconv.Itoa(minPort),
			"PASV_MAX_PORT": strconv.Itoa(maxPort),
		},
		WaitingFor: wait.ForListeningPort("21/tcp").WithStartupTimeout(15 * time.Second),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
}

func startSFTPContainer(ctx context.Context, containerName string) (testcontainers.Container, error) {
	scriptPath := filepath.Join(os.TempDir(), "generate-ssh-key.sh")
	_ = os.WriteFile(scriptPath, []byte(`
#!/bin/sh
set -e
mkdir -p /home/testuser/.ssh && ssh-keygen -t rsa -N '' -f /home/testuser/.ssh/id_rsa
cp /home/testuser/.ssh/id_rsa.pub /home/testuser/.ssh/authorized_keys
chown -R testuser:users /home/testuser/.ssh
chmod 600 /home/testuser/.ssh/authorized_keys
`), 0755)

	req := testcontainers.ContainerRequest{
		Name:         sanitizeContainerName(containerName),
		Image:        "atmoz/sftp:latest",
		ExposedPorts: []string{"22/tcp"},
		Cmd:          []string{"testuser:testpass:::upload"},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      scriptPath,
				ContainerFilePath: "/etc/sftp.d/generate-testuser-key.sh",
				FileMode:          0755,
			},
		},
		WaitingFor: wait.ForExec([]string{
			"bash", "-c",
			"sftp -i /home/testuser/.ssh/id_rsa -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -P 22 testuser@localhost <<< 'ls'",
		}).WithExitCode(0).WithStartupTimeout(15 * time.Second),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
}
func createTestTables(ctx context.Context, t *testing.T, container testcontainers.Container) error {
	queries := []string{
		`CREATE DATABASE IF NOT EXISTS test_db1`,
		`CREATE DATABASE IF NOT EXISTS test_db2`,
		`CREATE DATABASE IF NOT EXISTS test_db3`,
		`CREATE DATABASE IF NOT EXISTS logs_2023`,
		`CREATE DATABASE IF NOT EXISTS logs_2024`,
		`CREATE DATABASE IF NOT EXISTS system_db`,

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

		// preudo-System tables
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
		"DROP DATABASE IF EXISTS test_db1 SYNC",
		"DROP DATABASE IF EXISTS test_db2 SYNC",
		"DROP DATABASE IF EXISTS test_db3 SYNC",
		"DROP DATABASE IF EXISTS logs_2023 SYNC",
		"DROP DATABASE IF EXISTS logs_2024 SYNC",
		"DROP DATABASE IF EXISTS system_db SYNC",
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

func verifyDumpResults(t *testing.T, tempDir string, expectedFiles []string) error {
	// Build list of possible compressed variants for each expected file
	compressionExts := []string{"", ".gz", ".zstd"}
	expectedVariants := make(map[string]bool)

	for _, file := range expectedFiles {
		for _, ext := range compressionExts {
			expectedVariants[file+ext] = true
		}
	}

	// Use the storage's List method to find files
	remoteStorage, err := storage.NewFileStorage(tempDir, false)
	if err != nil {
		return fmt.Errorf("failed to create file remoteStorage: %w", err)
	}

	foundFiles, err := remoteStorage.List("", true)
	if err != nil {
		return fmt.Errorf("failed to walk dump directory: %w", err)
	}

	// Check each found file against expected variants
	foundCount := 0
	for _, found := range foundFiles {
		if expectedVariants[found] {
			foundCount++
			t.Logf("Found expected file: %s", found)
		}
	}

	if foundCount < len(expectedFiles) {
		return fmt.Errorf("missing files in dump, found %d/%d expected files, foundFiles=%v, expectedVariants=%v", foundCount, len(expectedFiles), foundFiles, expectedVariants)
	}

	return nil
}

func executeTestQuery(ctx context.Context, t *testing.T, container testcontainers.Container, query string) error {
	host, err := container.Host(ctx)
	if err != nil {
		return err
	}

	port, err := container.MappedPort(ctx, "8123/tcp")
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

	port, err := container.MappedPort(ctx, "8123/tcp")
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
