package backup

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/YenchangChan/ch2s3/ch"
	"github.com/YenchangChan/ch2s3/config"
)

type Backup struct {
	conf      *config.Config
	partition string
	states    map[string]*State
	reporter  string
}

func NewBack(conf *config.Config, partition, cwd string) *Backup {
	os.Mkdir(path.Join(cwd, "reporter"), 0644)
	return &Backup{
		conf:      conf,
		partition: partition,
		states:    make(map[string]*State),
		reporter:  fmt.Sprintf(path.Join(cwd, "reporter/backup_%s.out"), partition),
	}
}

// 初始化备份条件，创建clickhouse连接，检查S3有效性
func (this *Backup) Init() error {
	//todo: 检查S3有效性
	return ch.Connect(this.conf.ClickHouse)
}

// 具体的备份操作
func (this *Backup) Do() error {
	for _, table := range this.conf.ClickHouse.Tables {
		statekey := fmt.Sprintf("%s.%s", this.conf.ClickHouse.Database, table)
		rows, err := ch.Rows(this.conf.ClickHouse.Database, table, this.partition)
		if err != nil {
			return err
		}
		bsize, err := ch.Size(this.conf.ClickHouse.Database, table, this.partition)
		if err != nil {
			return err
		}
		this.states[statekey] = NewState(rows, bsize)
		err = ch.Ch2S3(this.conf.ClickHouse.Database, table, this.partition, this.conf.S3Disk)
		if err != nil {
			log.Printf("table %s backup failed: %v", statekey, err)
			this.states[statekey].Failure(err)
			continue
		}
		this.states[statekey].Success()
		log.Printf("backup table %s done", statekey)
	}

	return nil
}

// 出具报表
func (this *Backup) Repoter() error {
	f, err := os.OpenFile(this.reporter, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	var ok_tables, fail_tables, total_bytes uint64
	var all_costs int
	defer f.Close()
	_, err = f.WriteString(fmt.Sprintf("Backup Date: %s\n\n", this.partition))
	if err != nil {
		return err
	}
	f.WriteString("+--------------------------------+---------------+---------------+---------------+---------------+\n")
	f.WriteString("|            table               |     rows      |  size(local)  |   elapsed     |    status     |\n")
	f.WriteString("+--------------------------------+---------------+---------------+---------------+---------------+\n")
	for k, v := range this.states {
		f.WriteString(fmt.Sprintf("|%32s|%15d|%15s|%11d sec|%15s|\n", k, v.rows, formatReadableSize(v.bsize), v.elasped, status(v.extval)))
		if v.extval == BACKUP_SUCCESS {
			ok_tables++
		} else {
			fail_tables++
		}
		total_bytes += v.bsize
		all_costs += v.elasped
	}
	f.WriteString("+--------------------------------+---------------+---------------+---------------+---------------+\n")

	f.WriteString(fmt.Sprintf("\nTotal Tables: %d,  Success Tables: %d,  Failed Tables: %d,  Total Bytes: %s,  Elapsed: %d sec\n", ok_tables+fail_tables, ok_tables, fail_tables, formatReadableSize(total_bytes), all_costs))

	if fail_tables > 0 {
		f.WriteString("\nFailed Tables:\n")
		i := 1
		for k, v := range this.states {
			if v.extval == BACKUP_FAILURE {
				f.WriteString(fmt.Sprintf("[%d]%s\n", i, k))
				f.WriteString(fmt.Sprintf("\t%v\n", v.why))
				i++
			}
		}
	}
	f.WriteString("\n")
	return nil
}

func (this *Backup) RepoterPath() string {
	return this.reporter
}

// 清理备份成功的本地数据
func (this *Backup) Cleanup() error {
	if !this.conf.ClickHouse.Clean {
		//备份完成不清理本地数据
		return nil
	}
	for _, table := range this.conf.ClickHouse.Tables {
		_ = ch.Clean(this.conf.ClickHouse.Database, table, this.partition)
	}
	return nil
}

func (this *Backup) Stop() {
	ch.Close()
}
