package main

type Config struct {
	Host             string
	Port             int
	User             string
	Password         string
	Databases        string
	ExcludeDatabases string
	Tables           string
	ExcludeTables    string
	BatchSize        int
	StorageType      string
	StorageConfig    map[string]string
	CompressFormat   string
	CompressLevel    int
	BackupName       string
	Debug            bool
	Parallel         int
}
