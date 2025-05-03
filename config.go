package main

type Config struct {
	Host           string
	Port           int
	User           string
	Password       string
	Database       string
	BatchSize      int
	StorageType    string
	StorageConfig  map[string]string
	StoragePath    string
	CompressFormat string
	CompressLevel  int
}
