# Changelog

## [v1.2.0] - 2026-03-06

### Bug Fixes
- **ftp**: Close dumper storage connections to prevent FTP pool exhaustion (#133fe2d)

### CI/CD
- Add repository guard to prevent fork PR access to secrets (#4e190ea)

### Tests
- Replace fauria/vsftpd with instantlinux/vsftpd for FTP e2e tests (#c681017)
- Skip passing empty exclude-tables argument in e2e tests (#28321bd)

### Dependencies
- Update Go to 1.26
- Update Docker actions (build-push v7, metadata v6, setup-buildx v4, login v4, setup-qemu v4)
- Update GitHub Actions (checkout v6, setup-go v6, goreleaser v7)
- Update aws-sdk-go-v2 monorepo (multiple updates)
- Update cloud.google.com/go/storage to v1.60.0
- Update google.golang.org/api to v0.269.0
- Update golang.org/x/crypto to v0.48.0 (includes security fix)
- Update github.com/klauspost/compress to v1.18.4
- Update github.com/urfave/cli/v3 to v3.7.0
- Update github.com/testcontainers/testcontainers-go to v0.40.0
- Update github.com/stretchr/testify to v1.11.1
- Update github.com/aws/smithy-go to v1.24.2
- Update github.com/pkg/sftp to v1.13.10
- Update go.opentelemetry.io/otel/sdk to v1.40.0

## [v1.1.0] - 2025-05-15

### Features
- Add `--parallel` flag for parallel table processing (#435d1b2)
- Add version check requiring ClickHouse 24.10 or higher ([#32](https://github.com/Slach/clickhouse-dump/issues/32))
- Add GitHub Actions status badge to README

### Refactoring
- Migrate from `github.com/urfave/cli/v2` to `github.com/urfave/cli/v3`
  - Replace `EnvVars` with `Sources` pattern in flag definitions
  - Update `cli.App` to `cli.Command` API
  - Update `app.Run` calls to pass context
- Replace `jlaffaye/ftp` with `secsy/goftp` for thread-safe FTP operations
- Switch to zstd compression and `--parallel=4` default

### Bug Fixes
- **ftp**: Fix directory creation, path normalization, and stream close error handling
- **ftp**: Add thread-safe directory cache with mutex protection
- Fix deprecated `rand.Seed` call removal
- Fix Azure Blob Storage path handling in e2e tests
- Fix CLI argument order for urfave/cli parsing in e2e tests
- Fix default FTP storage path to root directory

### Dependencies
- Update Go to 1.25
- Update aws-sdk-go-v2 monorepo (multiple updates)
- Update google.golang.org/api to v0.251.0
- Update cloud.google.com/go/storage to v1.57.0
- Update golang.org/x/crypto to v0.42.0
- Update github.com/testcontainers/testcontainers-go to v0.39.0
- Update github.com/stretchr/testify to v1.11.1
- Update github.com/aws/smithy-go to v1.23.0
- Update GitHub Actions (checkout v5, setup-go v6)

## [v1.0.3] - 2025-10-01

## [v1.0.2] - 2025-07-19

## [v1.0.1] - 2025-06-30

## [v1.0.0] - 2025-05-14
