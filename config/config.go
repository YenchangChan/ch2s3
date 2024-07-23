package config

import (
	"encoding/json"
	"io"
	"os"
	"path"
)

type S3 struct {
	Endpoint       string
	Bucket         string `json:"-"`
	Region         string
	AccessKey      string
	SecretKey      string
	CompressMethod string `json:"compress_method"` //lz4, lz4hc, zstd,deflate_qpl
	CompressLevel  int    `json:"compress_level"`
	RetryTimes     uint   `json:"retry_times"`
	CleanIfFail    bool
	UsePathStyle   bool `json:"use_path_style"`
	CheckSum       bool
	Upload         bool
}

type Ch struct {
	Cluster     string
	Hosts       [][]string
	Port        int
	User        string
	Password    string
	Database    string
	Tables      []string
	Clean       bool
	ReadTimeout int
	SshUser     string
	SshPassword string
	SshPort     int
}

type Config struct {
	ClickHouse Ch
	S3Disk     S3 `json:"s3"`
	LogLevel   string
}

func ParseConfig(cwd string) (*Config, error) {
	f, err := os.Open(path.Join(cwd, "conf/backup.json"))
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var conf Config
	setDefaults(&conf)
	err = json.Unmarshal(data, &conf)
	if err != nil {
		return nil, err
	}
	return &conf, nil
}

func setDefaults(conf *Config) {
	conf.ClickHouse.Port = 9000
	conf.ClickHouse.User = "default"
	conf.ClickHouse.Database = "default"
	conf.ClickHouse.Clean = true
	conf.ClickHouse.ReadTimeout = 21600 //6h
	conf.ClickHouse.SshPort = 22

	conf.S3Disk.CleanIfFail = false
	conf.S3Disk.CompressMethod = "lz4"
	conf.S3Disk.CompressLevel = 3
	conf.S3Disk.RetryTimes = 1 //不重试
	conf.S3Disk.UsePathStyle = true
	conf.S3Disk.CheckSum = false
	conf.S3Disk.Upload = false

	conf.LogLevel = "info"
}
