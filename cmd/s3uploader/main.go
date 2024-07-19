package main

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/log"
	"github.com/YenchangChan/ch2s3/s3client"
	"github.com/jessevdk/go-flags"
)

type CmdOptions struct {
	BucketName string `short:"b" long:"bucket" description:"S3 bucket name"`
	FolderPath string `short:"f" long:"folder" description:"Folder path"`
	AccessKey  string `short:"a" long:"access" description:"AWS access key"`
	SecretKey  string `short:"s" long:"secret" description:"AWS secret key"`
	Region     string `short:"r" long:"region" description:"AWS region"`
	EndPoint   string `short:"e" long:"endpoint" description:"S3 endpoint"`
	DryRun     bool   `short:"d" long:"dryrun" description:"Dry run mode"`
}

// ./s3uploader -b 19700101/default.test_ck_dataq_r30/192.168.101.93/data/default/test_ck_dataq_r30/19700101_0_0_0 -f /data01/clickhouse/store/3cc/3ccf8474-fa31-469f-8ace-26ece20686d6/19700101_0_0_0 -a VdmPbwvMlH8ryeqW -s 8z16tUktXpvcjjy5M4MqXvCks5MMHb63 -r zh-west-1 -e http://192.168.101.94:49000/backup
func main() {
	log.InitLogger("info", []string{"stdout"})
	var opts CmdOptions
	flags.Parse(&opts)
	conf := config.S3{
		Endpoint:       opts.EndPoint,
		CompressMethod: "lz4",
		CompressLevel:  3,
		AccessKey:      opts.AccessKey,
		SecretKey:      opts.SecretKey,
		Region:         opts.Region,
		RetryTimes:     1,
		UsePathStyle:   true,
		CleanIfFail:    true,
	}

	raw, err := json.MarshalIndent(opts, "  ", "  ")
	if err != nil {
		log.Logger.Panic(err)
		return
	}
	log.Logger.Infoln(string(raw))
	err = s3client.NewSession(&conf)
	if err != nil {
		log.Logger.Panic(err)
		return
	}
	files := strings.Split(opts.FolderPath, ",")
	for _, file := range files {
		err = s3client.Upload(conf.Bucket, file, opts.BucketName, opts.DryRun)
		if err != nil {
			if conf.CleanIfFail {
				s3client.Remove(conf.Bucket, opts.BucketName)
			}
			log.Logger.Panic(err)
			os.Exit(1)
		}
	}
}
