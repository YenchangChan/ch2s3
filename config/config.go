package config

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path"
)

type S3 struct {
	Endpoint       string
	Bucket         string
	Region         string
	AccessKey      string
	SecretKey      string
	CompressMethod string `json:"compress_method"` //lz4, lz4hc, zstd,deflate_qpl
	CompressLevel  int    `json:"compress_level"`
	IgnoreExists   bool   `json:"ignore_exists"` //如果S3上已存在不报错
	RetryTimes     uint   `json:"retry_times"`
	CleanIfFail    bool
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
}

type Config struct {
	ClickHouse Ch
	S3Disk     S3 `json:"s3"`
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

	conf.S3Disk.CleanIfFail = false
	conf.S3Disk.CompressMethod = "lz4"
	conf.S3Disk.CompressLevel = 3
	conf.S3Disk.IgnoreExists = true
	conf.S3Disk.RetryTimes = 0 //不重试
}

func DumpConfig(c *Config) {
	raw, err := json.MarshalIndent(c, "  ", "   ")
	if err == nil {
		log.Printf("%s", string(raw))
	}
}
