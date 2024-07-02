package s3client

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	svc *s3.S3
)

func NewSession(conf *config.S3) error {
	var endpoint string
	u, err := url.Parse(conf.Endpoint)
	if err != nil {
		return err
	}
	if u.Path != "" {
		endpoint = u.Scheme + "://" + u.Host
		conf.Bucket = strings.Split(u.Path, "/")[1]
	} else {
		endpoint = conf.Endpoint
	}
	if conf.Bucket == "" || conf.Region == "" {
		return fmt.Errorf("bucket and region must not be empty")
	}
	sc, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(conf.AccessKey, conf.SecretKey, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(conf.Region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(conf.UsePathStyle),
	})
	if err != nil {
		return err
	}
	svc = s3.New(sc)
	return nil
}

func Remove(bucket, key string) error {
	params := &s3.ListObjectsInput{
		Bucket: aws.String("backup"),
	}
	resp, err := svc.ListObjects(params)
	if err != nil {
		return err
	}
	for _, item := range resp.Contents {
		if strings.HasPrefix(*item.Key, key) {
			log.Logger.Debugf("%s need to delete\n", *item.Key)
			_, err := svc.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    item.Key,
			})
			if err != nil {
				log.Logger.Errorf("delete %s failed\n", *item.Key)
				return err
			} else {
				log.Logger.Debugf("%s deleted\n", *item.Key)
			}
		}
	}
	return nil
}
