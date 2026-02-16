package main

import (
	"os"

	"github.com/goccy/go-yaml"
)

type BackupTask struct {
	Name       string
	LocalPath  string
	RemotePath string
	Schedule   string
}

type Bucket struct {
	Name  string
	Tasks []BackupTask
}

type Config struct {
	Buckets []Bucket
}

func ParseConfig(path string) (*Config, error) {
	var (
		parsed Config
		err    error
	)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, &parsed)

	return &parsed, err
}
