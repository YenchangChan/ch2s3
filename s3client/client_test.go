package s3client

import (
	"fmt"
	"testing"

	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
)

func TestS3(t *testing.T) {
	log.InitLogger("debug", []string{"stdout"})
	conf := config.S3{
		Endpoint: "http://192.168.101.94:49000/backup",
		//Bucket:         "backup",
		Region:         "zh-west-1",
		AccessKey:      "VdmPbwvMlH8ryeqW",
		SecretKey:      "8z16tUktXpvcjjy5M4MqXvCks5MMHb63",
		CompressMethod: "gzip",
		CompressLevel:  1,
		IgnoreExists:   true,
		RetryTimes:     3,
		CleanIfFail:    true,
	}
	err := NewSession(&conf)
	assert.Nil(t, err)
	result, err := svc.ListBuckets(nil)
	assert.Nil(t, nil)

	fmt.Println("Bucket:", conf.Bucket)

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
	err = Remove("backup", "19700101/default.test_ck_dataq_r50/192.168.101.93")
	assert.Nil(t, err)
}
