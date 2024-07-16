package s3client

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/log"
	"github.com/YenchangChan/ch2s3/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	svc *s3.S3
	sc  *session.Session
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
	log.Logger.Infof("path: %s, endpoint: %s, bucket: %s\n", u.Path, endpoint, conf.Bucket)
	if conf.Bucket == "" || conf.Region == "" {
		return fmt.Errorf("bucket and region must not be empty")
	}
	sc, err = session.NewSession(&aws.Config{
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
		Bucket: aws.String(bucket),
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

func CheckSum(host string, bucket, key string, paths map[string]utils.PathInfo, conf config.S3) (map[string]utils.PathInfo, uint64, error) {
	var rsize uint64
	errPaths := make(map[string]utils.PathInfo)
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}
	resp, err := svc.ListObjects(params)
	if err != nil {
		return errPaths, rsize, err
	}
	rpaths := make(map[string]string)
	for _, item := range resp.Contents {
		if strings.HasPrefix(*item.Key, key) {
			checksum := strings.Trim(*item.ETag, "\"")
			size := *item.Size
			rsize += uint64(size)
			rpaths[*item.Key] = checksum
			log.Logger.Debugf("remote s3 path: %s, checksum: %s", *item.Key, checksum)
		}
	}
	for k, v := range paths {
		if v.Host != host {
			continue
		}
		if _, ok := rpaths[k]; ok {
			if conf.CheckSum {
				if v.MD5 != rpaths[k] {
					errPaths[k] = v
					err = fmt.Errorf("checksum mismatch for %s, expect %s, but got %s", k, v, rpaths[k])
				}
			}
		} else {
			errPaths[k] = v
			err = fmt.Errorf("file %s not found on s3", k)
		}
	}
	return errPaths, rsize, err
}

func Upload(bucket, folderPath, key string, dryrun bool) error {
	pool := utils.NewPoolDefault()
	var lastErr error
	defer pool.Close()
	cnt := 0
	start := time.Now()
	err := filepath.Walk(folderPath, func(fpath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Logger.Errorf("Error walking the path:%v", err)
			return err
		}

		log.Logger.Debugf("Visiting:%v\n", fpath)

		if !info.IsDir() {
			pool.Submit(func() {
				skey := path.Join(key, filepath.Base(fpath))
				if !dryrun {
					file, err := os.Open(fpath)
					if err != nil {
						log.Logger.Errorf("Error opening file:%v", err)
						lastErr = err
						return
					}
					defer file.Close()
					_, err = svc.PutObjectWithContext(context.Background(), &s3.PutObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(skey),
						Body:   file,
					})
					if err != nil {
						if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
							lastErr = fmt.Errorf("upload canceled due to timeout, %v", err)
							return
						}
						lastErr = fmt.Errorf("failed to upload object, %v", err)
						return
					}
					cnt++
				}
				log.Logger.Infof("Uploaded:[%s] to [%s]", fpath, skey)
			})
		}
		return lastErr
	})
	pool.Wait()

	if err != nil {
		log.Logger.Errorf("Error walking the folder: %v", err)
		return err
	}
	log.Logger.Infof("%d files upload to s3 success! Elapsed: %v sec", cnt, time.Since(start).Seconds())
	return nil
}
