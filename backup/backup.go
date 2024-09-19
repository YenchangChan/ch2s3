package backup

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/YenchangChan/ch2s3/ch"
	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/constant"
	"github.com/YenchangChan/ch2s3/log"
	"github.com/YenchangChan/ch2s3/s3client"
	"github.com/bndr/gotabulate"
)

type Backup struct {
	conf      *config.Config
	partition string
	cponly    bool
	states    map[string]*State
	reporter  string
	cwd       string
}

func NewBack(conf *config.Config, op_type, partition, cwd string, cponly bool) *Backup {
	os.Mkdir(path.Join(cwd, "reporter"), 0644)
	return &Backup{
		conf:      conf,
		partition: partition,
		cponly:    cponly,
		states:    make(map[string]*State),
		cwd:       cwd,
		reporter:  fmt.Sprintf(path.Join(cwd, "reporter/%s_%s.out"), op_type, time.Now().Format("20060102T15:04:05")),
	}
}

// 初始化备份条件，创建clickhouse连接，检查S3有效性
func (this *Backup) Init() error {
	err := s3client.NewSession(&this.conf.S3Disk)
	if err != nil {
		return err
	}

	return ch.Connect(this.conf.ClickHouse)
}

// 具体的备份操作
func (this *Backup) Do() error {
	for _, table := range this.conf.ClickHouse.Tables {
		statekey := fmt.Sprintf("%s.%s", this.conf.ClickHouse.Database, table)
		rows, err := ch.Rows(this.conf.ClickHouse.Database, table, this.partition, this.cponly)
		if err != nil {
			return err
		}
		buncsize, bczise, err := ch.Size(this.conf.ClickHouse.Database, table, this.partition, this.cponly)
		if err != nil {
			return err
		}
		var partitions []string
		if this.cponly {
			partitions = strings.Split(this.partition, ",")
		} else {
			partitions, err = ch.Partitions(this.conf.ClickHouse.Database, table, this.partition, this.cponly)
			if err != nil {
				return err
			}
		}
		this.states[statekey] = NewState(rows, buncsize, bczise, len(partitions))
		ok := true
		for i, p := range partitions {
			log.Logger.Infof("(%d/%d) table %s [%s] backup ", i+1, len(partitions), statekey, p)
			rsize, err := ch.Ch2S3(this.conf.ClickHouse.Database, table, p, this.conf.S3Disk, this.cwd)
			this.states[statekey].Set(constant.STATE_REMOTE_SIZE, rsize)
			if err != nil {
				log.Logger.Errorf("table %s partition %s backup failed: %v", statekey, p, err)
				this.states[statekey].Failure(err)
				ok = false
				continue
			}
			if this.conf.ClickHouse.Clean {
				err = ch.Clean(this.conf.ClickHouse.Database, table, p)
				if err != nil {
					log.Logger.Errorf("clean table %s partition %s failed: %v", statekey, p, err)
				}
			}
		}
		if ok {
			this.states[statekey].Success()
		}
		log.Logger.Infof("backup table %s done", statekey)
	}

	return nil
}

// 备份表只能一个partition一个partition的备份，因为无法查询出全量的partition了
func (this *Backup) Restore() error {
	var err error
	for _, table := range this.conf.ClickHouse.Tables {
		statekey := fmt.Sprintf("%s.%s", this.conf.ClickHouse.Database, table)
		ok := true
		partitions := strings.Split(this.partition, ",")
		this.states[statekey] = NewState(0, 0, 0, len(partitions))
		var rows, buncsize, bcsize uint64
		for i, p := range partitions {
			log.Logger.Infof("(%d/%d) table %s [%s] restore ", i+1, len(partitions), statekey, p)
			err = ch.Restore(this.conf.ClickHouse.Database, table, p, this.conf.S3Disk)
			if err != nil {
				log.Logger.Errorf("table %s restore failed: %v", statekey, err)
				this.states[statekey].Failure(err)
				ok = false
				break
			}
			row, err := ch.Rows(this.conf.ClickHouse.Database, table, p, true)
			if err != nil {
				return err
			}
			bunc, bc, err := ch.Size(this.conf.ClickHouse.Database, table, p, true)
			if err != nil {
				return err
			}
			rows += row
			buncsize += bunc
			bcsize += bc
		}

		this.states[statekey].Set(constant.STATE_ROWS, rows)
		this.states[statekey].Set(constant.STATE_UNCOMPRESSED_SIZE, buncsize)
		this.states[statekey].Set(constant.STATE_COMPRESSED_SIZE, bcsize)
		if ok {
			this.states[statekey].Success()
		}
		log.Logger.Infof("restore table %s done", statekey)
	}
	return nil
}

// 出具报表
func (this *Backup) Repoter(op_type string) error {
	f, err := os.OpenFile(this.reporter, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	var ok_tables, fail_tables, total_bytes uint64
	var all_costs int
	defer f.Close()
	_, err = f.WriteString(fmt.Sprintf("%s Date: %s\n\n", strings.Title(op_type), this.partition))
	if err != nil {
		return err
	}
	var data [][]interface{}

	data = append(data, []interface{}{"table", "rows", "size(uncompressed)", "size(compressed)", "remote_size", "partition", "elapsed", "status"})
	for k, v := range this.states {
		data = append(data, []interface{}{k, v.rows, formatReadableSize(v.buncsize), formatReadableSize(v.bcsize), formatReadableSize(v.rsize), v.partitions, v.elasped, status(v.extval)})
		if v.extval == constant.BACKUP_SUCCESS {
			ok_tables++
		} else {
			fail_tables++
		}
		total_bytes += v.buncsize
		all_costs += v.elasped
	}
	tabulate := gotabulate.Create(data)
	f.WriteString(tabulate.Render("grid"))
	f.WriteString(fmt.Sprintf("\nTotal Tables: %d,  Success Tables: %d,  Failed Tables: %d,  Total Bytes: %s,  Elapsed: %d sec\n", ok_tables+fail_tables, ok_tables, fail_tables, formatReadableSize(total_bytes), all_costs))

	if fail_tables > 0 {
		f.WriteString("\nFailed Tables:\n")
		i := 1
		for k, v := range this.states {
			if v.extval == constant.BACKUP_FAILURE {
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
		// 备份失败，不删除数据
		statekey := fmt.Sprintf("%s.%s", this.conf.ClickHouse.Database, table)
		if this.states[statekey].extval == constant.BACKUP_FAILURE {
			log.Logger.Warnf("table %s backup failed, do not clean data", statekey)
			continue
		}
		var err error
		var partitions []string
		if this.cponly {
			partitions = strings.Split(this.partition, ",")
		} else {
			partitions, err = ch.Partitions(this.conf.ClickHouse.Database, table, this.partition, this.cponly)
			if err != nil {
				return err
			}
		}
		for _, p := range partitions {
			err = ch.Clean(this.conf.ClickHouse.Database, table, p)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (this *Backup) Stop() {
	ch.Close()
}
