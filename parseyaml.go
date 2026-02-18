package main

import (
	"os"

	"github.com/goccy/go-yaml"
)

type BackupTask struct {
	Name       string
	LocalPath  string `yaml:"local_path"`
	RemotePath string `yaml:"remote_path"`
	Schedule   string
}

type Bucket struct {
	Name  string
	Tasks []BackupTask
}

type Credentials struct {
	ApplicationKeyId string `yaml:"application_key_id"`
	ApplicationKey   string `yaml:"application_key"`
}

type Remote struct {
	Endpoint string
	Region   string
}

type Config struct {
	Credentials Credentials
	Remote      Remote
	Concurrency int
	Buckets     []Bucket
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
