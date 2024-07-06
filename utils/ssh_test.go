package utils

import (
	"fmt"
	"testing"

	"github.com/YenchangChan/ch2s3/log"
	"github.com/stretchr/testify/assert"
)

func TestSSH(t *testing.T) {
	log.InitLogger("debug", []string{"stdout"})
	cmd := "ls -l"
	opts := SshOptions{
		User:     "root",
		Password: "123456",
		Host:     "192.168.122.101",
		Port:     22,
	}
	out, err := RemoteExecute(opts, cmd)
	assert.Nil(t, err)
	fmt.Println(out)
}
