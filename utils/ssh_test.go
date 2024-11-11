package utils

import (
	"fmt"
	"testing"

	"github.com/YenchangChan/ch2s3/log"
	"github.com/stretchr/testify/assert"
)

func TestSSH(t *testing.T) {
	log.InitLogger("debug", []string{"stdout"})
	cmd := "cd /data01/clickhouse/store/cb6/cb658542-efb3-4655-9278-a3b217cbe9c7/0_0_312004_7892_303610/; ls"
	opts := SshOptions{
		User:     "root",
		Password: "",
		Host:     "192.168.101.93",
		Port:     22,
	}
	out, err := RemoteExecute(opts, cmd)
	assert.Nil(t, err)
	fmt.Println(out)
}
