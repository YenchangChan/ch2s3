package utils

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/YenchangChan/ch2s3/log"
	scp "github.com/bramvdbogaerde/go-scp"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

const (
	TmpWorkDirectory string = "/tmp"
)

type SshOptions struct {
	User     string
	Password string
	Host     string
	Port     int
}

func sshConnectwithPassword(user, password string) (*ssh.ClientConfig, error) {
	return &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		Timeout:         30 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}, nil
}

func sshConnectwithPublickKey(user string) (*ssh.ClientConfig, error) {
	home, _ := os.UserHomeDir()
	key, err := os.ReadFile(path.Join(home, ".ssh", "id_rsa"))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		Timeout:         30 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}, nil
}

func SSHConnect(opts SshOptions) (*ssh.Client, error) {
	var (
		addr         string
		clientConfig *ssh.ClientConfig
		client       *ssh.Client
		err          error
	)

	if opts.Password == "" {
		clientConfig, err = sshConnectwithPublickKey(opts.User)
	} else {
		clientConfig, err = sshConnectwithPassword(opts.User, opts.Password)
	}
	if err != nil {
		return nil, err
	}

	// connet to ssh
	addr = net.JoinHostPort(opts.Host, fmt.Sprintf("%d", opts.Port))
	if client, err = ssh.Dial("tcp", addr, clientConfig); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}

	return client, nil
}

func ScpConnect(opts SshOptions) (*scp.Client, *ssh.Client, error) {
	sshClient, err := SSHConnect(opts)
	if err != nil {
		return nil, nil, err
	}
	// create scp client
	var client scp.Client
	if client, err = scp.NewClientBySSH(sshClient); err != nil {
		err = errors.Wrapf(err, "")
		sshClient.Close()
		return nil, nil, err
	}

	return &client, sshClient, nil
}

// https://stackoverflow.com/questions/41259439/how-to-convert-filemode-to-int
func GetFilePerm(file string) (string, error) {
	fileInfo, err := os.Stat(file)
	if err != nil {
		return "", errors.Wrapf(err, "GetFilePerm")
	}
	perm := fileInfo.Mode().Perm()
	return fmt.Sprintf("%04o", perm), nil
}

func ScpUpload(client *scp.Client, localFilePath, remoteFilePath string) error {
	f, err := os.Open(localFilePath)
	if err != nil {
		return errors.Wrapf(err, "os.Open")
	}
	defer f.Close()
	perm, err := GetFilePerm(localFilePath)
	if err != nil {
		return err
	}
	err = client.CopyFromFile(context.Background(), *f, remoteFilePath, perm)
	if err != nil {
		err = errors.Wrapf(err, "CopyFromFile")
		return err
	}
	return nil
}

func SSHRun(client *ssh.Client, password, shell string) (result string, err error) {
	var session *ssh.Session
	var buf []byte
	// create session
	if session, err = client.NewSession(); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer session.Close()

	log.Logger.Debugf("shell: %s", shell)

	modes := ssh.TerminalModes{
		ssh.ECHO:          0,     // disable echoing
		ssh.TTY_OP_ISPEED: 14400, // input speed = 14.4kbaud
		ssh.TTY_OP_OSPEED: 14400, // output speed = 14.4kbaud
	}
	err = session.RequestPty("xterm", 80, 40, modes)
	if err != nil {
		return "", errors.Wrap(err, "")
	}
	in, err := session.StdinPipe()
	if err != nil {
		return "", errors.Wrap(err, "")
	}

	out, err := session.StdoutPipe()
	if err != nil {
		return "", errors.Wrap(err, "")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(in io.WriteCloser, out io.Reader, output *[]byte) {
		defer wg.Done()
		var (
			line string
			r    = bufio.NewReader(out)
		)

		for {
			b, err := r.ReadByte()
			if err != nil {
				break
			}
			*output = append(*output, b)
			//log.Logger.Debugf("out: %v", string(*output))
			if b == byte('\n') {
				line = ""
				continue
			}
			line += string(b)
			// TODO I have no idea to slove this problem: "xxx is not in the sudoers file.  This incident will be reported."
			if strings.HasPrefix(line, "[sudo] password for ") && strings.HasSuffix(line, ": ") {
				_, err = in.Write([]byte(password + "\n"))
				if err != nil {
					break
				}
			}
			//reg, _ := regexp.Compile(".*@.*'s password:")
			//if reg.MatchString(line) {
			if strings.HasSuffix(strings.ToLower(line), "password: ") {
				_, err = in.Write([]byte(password + "\n"))
				if err != nil {
					break
				}
			}
		}
	}(in, out, &buf)

	_, err = session.CombinedOutput(shell)
	if err != nil {
		return "", errors.Wrap(err, "")
	}
	wg.Wait()
	result = strings.TrimSpace(string(buf))
	result = result[strings.Index(result, "i love china")+12:]
	result = strings.TrimSpace(result)
	if strings.HasPrefix(result, "[sudo] password for ") {
		result = result[strings.Index(result, "\n")+1:]
	}
	log.Logger.Debugf("output:[%s]", result)
	return
}

func ScpUploadFiles(files []string, remotePath string, opts SshOptions) error {
	for _, file := range files {
		if file == "" {
			continue
		}
		remoteFile := path.Join(remotePath, path.Base(file))
		err := ScpUploadFile(file, remoteFile, opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func ScpUploadFile(localFile, remoteFile string, opts SshOptions) error {
	client, sshClient, err := ScpConnect(opts)
	if err != nil {
		return err
	}
	defer client.Close()
	defer sshClient.Close()
	// delete remote file first, beacuse maybe the remote file exists and created by root
	cmd := fmt.Sprintf("rm -rf %s", path.Join(TmpWorkDirectory, path.Base(remoteFile)))
	_, err = RemoteExecute(opts, cmd)
	if err != nil {
		return err
	}

	err = ScpUpload(client, localFile, path.Join(TmpWorkDirectory, path.Base(remoteFile)))
	if err != nil {
		return err
	}

	if path.Dir(remoteFile) != TmpWorkDirectory {
		cmd = fmt.Sprintf("cp %s %s", path.Join(TmpWorkDirectory, path.Base(remoteFile)), remoteFile)
		_, err = RemoteExecute(opts, cmd)
		if err != nil {
			return err
		}
	}

	return nil
}

func RemoteExecute(opts SshOptions, cmd string) (string, error) {
	client, err := SSHConnect(opts)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("host: %s, cmd: %s", opts.Host, cmd))
	}
	defer client.Close()

	finalScript := genFinalScript(opts.User, cmd)
	var output string
	if output, err = SSHRun(client, opts.Password, finalScript); err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("run '%s' on host %s fail: %s", cmd, opts.Host, output))
	}
	return output, nil
}

func genFinalScript(user, cmd string) string {
	var shell string
	if user != "root" && user != "clickhouse" {
		cmds := strings.Split(cmd, ";")
		for index, command := range cmds {
			cmds[index] = fmt.Sprintf("sudo %s", command)
		}
		cmd = strings.Join(cmds, ";")
		shell = fmt.Sprintf("export LANG=en_US.UTF-8; %s", cmd)
	} else {
		shell = fmt.Sprintf("export LANG=en_US.UTF-8; %s", cmd)
	}
	shell = fmt.Sprintf("echo 'i love china'; %s", shell)
	return shell
}
