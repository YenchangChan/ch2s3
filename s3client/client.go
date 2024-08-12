package s3client

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
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
	log.Logger.Infof("path: %s, endpoint: %s, bucket: %s", u.Path, endpoint, conf.Bucket)
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
		Prefix: aws.String(key),
	}
	resp, err := svc.ListObjects(params)
	if err != nil {
		return err
	}
LOOP_DEL:
	log.Logger.Infof("key %s has %d objects need to delete", key, len(resp.Contents))
	for _, item := range resp.Contents {
		log.Logger.Debugf("%s need to delete", *item.Key)
		_, err := svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    item.Key,
		})
		if err != nil {
			log.Logger.Errorf("delete %s failed", *item.Key)
			return err
		} else {
			err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    item.Key,
			})
			if err != nil {
				log.Logger.Errorf("delete %s failed", *item.Key)
				return err
			}
			log.Logger.Debugf("%s deleted", *item.Key)
		}
	}

	//由于一次最多只能删除1000个object，删除之后查一把还有没有没删除的
	resp, err = svc.ListObjects(params)
	if err != nil {
		return err
	}
	if len(resp.Contents) == 0 {
		log.Logger.Infof("object %s is empty", key)
	} else {
		//没删除干净，再来一次
		log.Logger.Infof("object %s is not empty, still %d objects remained, try again", key, len(resp.Contents))
		goto LOOP_DEL
	}
	return nil
}

func CheckSum(host string, bucket, key string, paths map[string]utils.PathInfo, conf config.S3) (map[string]utils.PathInfo, uint64, error) {
	var rsize uint64
	errPaths := make(map[string]utils.PathInfo)

	subKeys := make(map[string]struct{})
	for _, v := range paths {
		if v.Host != host {
			continue
		}
		subKey := path.Dir(v.RPath)
		if _, ok := subKeys[subKey]; ok {
			continue
		}
		subKeys[subKey] = struct{}{}
	}

	rpaths := make(map[string]string)
	cnt := 0
	for subkey := range subKeys {
		params := &s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(subkey),
		}
		resp, err := svc.ListObjects(params)
		if err != nil {
			return errPaths, rsize, err
		}

		subCnt := 0
		for _, item := range resp.Contents {
			checksum := strings.Trim(*item.ETag, "\"")
			if strings.Contains(checksum, "-") && conf.CheckSum {
				//分段上传, 由于不知道UploadId, 无法计算具体的MD5值, 需要将对象下载下来，分段计算MD5
				log.Logger.Infof("key %s is multipart upload, checksum: %s", *item.Key, checksum)
				output, err := svc.GetObject(&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    item.Key,
				})
				if err != nil {
					return errPaths, rsize, err
				}
				defer output.Body.Close()
				//一次读取32MB
				segment := make([]byte, 1048576*32)
				hash := md5.New()
				for {
					n, err := output.Body.Read(segment)
					if err != nil && err != io.EOF {
						return errPaths, rsize, err
					}
					if n == 0 {
						break
					}
					hash.Write(segment[:n])
				}
				checksum = hex.EncodeToString(hash.Sum(nil))
			}
			size := *item.Size
			rsize += uint64(size)
			subCnt++
			rpaths[*item.Key] = checksum
			log.Logger.Debugf("[%s]remote s3 path: %s, checksum: %s", host, *item.Key, checksum)
		}
		log.Logger.Infof("[%s] %s remote count: %d", host, subkey, subCnt)
		cnt += subCnt
		if subCnt == 1000 {
			log.Logger.Warnf("NOTICE: %s has more than 1000 keys, may not list all.", subkey)
		}
	}
	log.Logger.Infof("[%s] %s remote total count: %d", host, key, cnt)
	var err error
	for k, v := range paths {
		if v.Host != host {
			continue
		}
		if checksum, ok := rpaths[k]; ok {
			if conf.CheckSum {
				if v.MD5 != checksum {
					errPaths[k] = v
					err = fmt.Errorf("checksum mismatch for %s, expect %s, but got %s", k, v, rpaths[k])
					log.Logger.Warnf("[%s]%v", host, err)
				}
			}
		} else {
			errPaths[k] = v
			err = fmt.Errorf("file %s not found on s3", k)
			log.Logger.Warnf("[%s]%v", host, err)
		}
	}
	log.Logger.Infof("errPaths: %d", len(errPaths))
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

		log.Logger.Debugf("Visiting:%v", fpath)

		if !info.IsDir() {
			pool.Submit(func() {
				if err := uploadFile(key, fpath, bucket, dryrun); err != nil {
					lastErr = err
					return
				}
				cnt++
			})
			pool.Wait()
			if lastErr != nil {
				return lastErr
			}
		}

		return nil
	})

	if err != nil {
		log.Logger.Errorf("Error walking the folder: %v", err)
		return err
	}
	if lastErr != nil {
		log.Logger.Errorf("Error uploading files: %v", lastErr)
		return lastErr
	}
	log.Logger.Infof("%d files upload to s3 success! Elapsed: %v sec", cnt, time.Since(start).Seconds())
	return nil
}

func uploadFile(key, fpath, bucket string, dryrun bool) error {
	skey := path.Join(key, filepath.Base(fpath))
	if !dryrun {
		file, err := os.Open(fpath)
		if err != nil {
			log.Logger.Errorf("Error opening file:%v", err)
			return err
		}
		defer file.Close()
		_, err = svc.PutObjectWithContext(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(skey),
			Body:   file,
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				log.Logger.Error(err)
				return fmt.Errorf("upload canceled due to timeout, %v", err)
			}
			log.Logger.Error(err)
			return fmt.Errorf("failed to upload object, %v", err)
		}
	}
	log.Logger.Infof("Uploaded:[%s] to [%s]", fpath, skey)
	return nil
}
