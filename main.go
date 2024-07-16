package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/YenchangChan/ch2s3/backup"
	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/constant"
	"github.com/YenchangChan/ch2s3/log"
)

var (
	partition = flag.String("p", "", "which partition to backup")
	ttl       = flag.String("ttl", "", "ttl interval")
	r         = flag.Bool("restore", false, "restore table")

	op_type    string
	cwd        string
	Version    string
	BuildStamp string
	Githash    string
)

func main() {
	conf, err := config.ParseConfig(cwd)
	if err != nil {
		fmt.Printf("parse config failed:%v\n", err)
		os.Exit(-1)
	}
	log.InitLogger(conf.LogLevel, []string{"stdout", "ch2s3.log"})
	log.Logger.Infof("ch2s3, partition: %s, cwd: %s, version: %s, build timestamp: %s, git hash: %s",
		*partition, cwd, Version, BuildStamp, Githash)

	DumpConfig(conf)
	current_partition_only := false
	if *ttl == "" {
		current_partition_only = true
	}
	back := backup.NewBack(conf, op_type, *partition, cwd, current_partition_only)
	switch op_type {
	case constant.OP_TYPE_BACKUP:
		err = ch2s3(back)
	case constant.OP_TYPE_RESTORE:
		err = restore(back)
	}
	if err != nil {
		log.Logger.Panic(err)
	}
	log.Logger.Infof("backup completed, please see reporter from [%s]!", back.RepoterPath())
}

func init() {
	flag.Parse()

	op_type = constant.OP_TYPE_BACKUP
	if *r {
		op_type = constant.OP_TYPE_RESTORE
	}

	//指定TTL仅在备份时有效
	if !*r && *ttl != "" {
		//指定TTL时，默认按照toYYYYMMDD分区
		ttlExpr := strings.SplitN(*ttl, " ", 2)
		interval := ttlExpr[0]
		unit := strings.ToUpper(ttlExpr[1])
		var year, month, day int
		switch unit {
		case "DAY", "D":
			if d, err := strconv.Atoi(interval); err == nil {
				day = d * (-1)
			}
		case "WEEK", "W":
			if w, err := strconv.Atoi(interval); err == nil {
				day = w * 7 * (-1)
			}
		case "MONTH", "M", "MON":
			if m, err := strconv.Atoi(interval); err == nil {
				month = m * (-1)
			}
		case "YEAR", "Y":
			if y, err := strconv.Atoi(interval); err == nil {
				year = y * (-1)
			}
		}
		*partition = time.Now().AddDate(year, month, day).Format("20060102")
	}

	if *partition == "" {
		*partition = time.Now().Format("20060102")
	}

	exe, _ := filepath.Abs(os.Args[0])
	cwd = filepath.Dir(filepath.Dir(exe))
}

func ch2s3(back *backup.Backup) error {
	var err error
	if err = back.Init(); err != nil {
		return err
	}
	log.Logger.Infof("backup init success!")

	defer back.Stop()

	if err = back.Do(); err != nil {
		return err
	}

	log.Logger.Infof("backup to s3 success!")

	if err = back.Repoter(op_type); err != nil {
		return err
	}

	log.Logger.Infof("backup reporter success!")

	return nil
}
func restore(back *backup.Backup) error {
	var err error
	if err = back.Init(); err != nil {
		return err
	}
	log.Logger.Infof("restore init success!")

	defer back.Stop()

	if err = back.Restore(); err != nil {
		return err
	}

	log.Logger.Infof("restore from s3 success!")

	if err = back.Repoter(op_type); err != nil {
		return err
	}

	log.Logger.Infof("restore reporter success!")

	return nil
}

func DumpConfig(c *config.Config) {
	raw, err := json.MarshalIndent(c, "  ", "   ")
	if err == nil {
		log.Logger.Infof("%s", string(raw))
	}
}
