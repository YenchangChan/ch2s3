package ch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/log"
	"github.com/YenchangChan/ch2s3/s3client"
	"github.com/YenchangChan/ch2s3/utils"
	"github.com/avast/retry-go/v4"
)

type Conn struct {
	h    string
	c    driver.Conn
	opts utils.SshOptions
}

var (
	conns [][]Conn
)

func Connect(conf config.Ch) error {
	var lastErr error
	for _, shards := range conf.Hosts {
		var shardConns []Conn
		for _, replica := range shards {
			opts := clickhouse.Options{
				Addr: []string{fmt.Sprintf("%s:%d", replica, conf.Port)},
				Auth: clickhouse.Auth{
					Username: conf.User,
					Password: conf.Password,
					Database: conf.Database,
				},
				Compression: &clickhouse.Compression{
					Method: clickhouse.CompressionLZ4,
				},
				ReadTimeout: time.Duration(conf.ReadTimeout) * time.Second,
			}
			c, err := clickhouse.Open(&opts)
			if err != nil {
				log.Logger.Errorf("[%s]connect failed: %v", replica, err)
				lastErr = err
			}

			if err = c.Ping(context.Background()); err != nil {
				log.Logger.Errorf("[%s]ping failed: %v", replica, err)
				lastErr = err
			}
			shardConns = append(shardConns, Conn{
				h: replica,
				c: c,
				opts: utils.SshOptions{
					Host:     replica,
					Port:     conf.SshPort,
					User:     conf.SshUser,
					Password: conf.SshPassword,
				},
			})
		}
		if len(shardConns) > 0 {
			conns = append(conns, shardConns)
		} else {
			return lastErr
		}
	}

	return nil
}

func Reconnect(conf config.Ch) error {
	Close()
	return Connect(conf)
}

func GetAvaliableConn(shardNum int) (Conn, error) {
	if shardNum < 0 || shardNum >= len(conns) {
		return Conn{}, fmt.Errorf("shardNum is invalid")
	}
	var lastErr error
	for _, conn := range conns[shardNum] {
		if err := conn.c.Ping(context.Background()); err == nil {
			return conn, nil
		} else {
			lastErr = err
		}
	}
	return Conn{}, lastErr
}

func Close() {
	for _, shard := range conns {
		for _, replica := range shard {
			replica.c.Close()
		}
	}
}

func Size(database, table, partition string, cponly bool) (uint64, uint64, error) {
	var lastErr error
	var wg sync.WaitGroup
	var lock sync.Mutex
	var uncompressed_size, compressed_size uint64
	wg.Add(len(conns))
	op := "<="
	if cponly {
		op = "="
	}
	query := fmt.Sprintf("SELECT sum(data_uncompressed_bytes), sum(data_compressed_bytes) FROM system.parts WHERE partition %s '%s' AND database = '%s' AND table = '%s'",
		op, partition, database, table)
	log.Logger.Debugf("execute sql => %s", query)
	for i := range conns {
		conn, err := GetAvaliableConn(i)
		if err != nil {
			return 0, 0, err
		}
		go func(c driver.Conn) {
			defer wg.Done()
			var buncsize, bcsize uint64
			err := c.QueryRow(context.Background(), query).Scan(&buncsize, &bcsize)
			if err != nil {
				lastErr = err
				return
			}
			lock.Lock()
			uncompressed_size += buncsize
			compressed_size += bcsize
			lock.Unlock()
		}(conn.c)
	}
	wg.Wait()
	log.Logger.Debugf("execute result => %v, %v", uncompressed_size, compressed_size)
	return uncompressed_size, compressed_size, lastErr
}

func Rows(database, table, partition string, cponly bool) (uint64, error) {
	var lastErr error
	var wg sync.WaitGroup
	var lock sync.Mutex
	var count uint64
	wg.Add(len(conns))
	op := "<="
	if cponly {
		op = "="
	}
	query := fmt.Sprintf("SELECT sum(rows) FROM system.parts WHERE partition %s '%s' AND database = '%s' AND table = '%s'",
		op, partition, database, table)
	log.Logger.Debugf("execute sql => %s", query)
	for i := range conns {
		conn, err := GetAvaliableConn(i)
		if err != nil {
			return 0, err
		}
		go func(c driver.Conn) {
			defer wg.Done()
			var cnt uint64
			err := c.QueryRow(context.Background(), query).Scan(&cnt)
			if err != nil {
				lastErr = err
				return
			}
			lock.Lock()
			count += cnt
			lock.Unlock()
		}(conn.c)
	}
	wg.Wait()
	log.Logger.Debugf("execute result => %v", count)
	return count, lastErr
}

func Partitions(database, table, partition string, cponly bool) ([]string, error) {
	var lastErr error
	var wg sync.WaitGroup
	var partitions []string
	mp := make(map[string]struct{})
	var lock sync.Mutex
	wg.Add(len(conns))
	op := "<="
	if cponly {
		op = "="
	}
	query := fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE partition %s '%s' AND database = '%s' AND table = '%s' ORDER BY partition",
		op, partition, database, table)
	log.Logger.Debugf("execute sql => %s", query)
	for i := range conns {
		conn, err := GetAvaliableConn(i)
		if err != nil {
			return partitions, err
		}
		go func(c driver.Conn) {
			defer wg.Done()

			rows, err := c.Query(context.Background(), query)
			if err != nil {
				lastErr = err
				return
			}
			defer rows.Close()
			for rows.Next() {
				var partition string
				err := rows.Scan(&partition)
				if err != nil {
					lastErr = err
					return
				}
				lock.Lock()
				mp[partition] = struct{}{}
				lock.Unlock()
			}
		}(conn.c)
	}
	wg.Wait()
	for p := range mp {
		partitions = append(partitions, p)
	}
	log.Logger.Debugf("execute result => %v", partitions)
	return partitions, lastErr
}

/*
BACKUP TABLE default.test_ck_dataq_r77 PARTITION '20230731' TO S3('http://192.168.101.94:49000/backup/20230731', 'VdmPbwvMlH8ryeqW', '8z16tUktXpvcjjy5M4MqXvCks5MMHb63')
SETTINGS compression_method='lz4', compression_level=3
*/
func genBackupSql(database, table, partition, host string, conf config.S3) (string, string) {
	var key, sql string
	sql = fmt.Sprintf("BACKUP TABLE `%s`.`%s` ", database, table)
	if partition != "" {
		sql += fmt.Sprintf(" PARTITION '%s'", partition)
	}
	key = fmt.Sprintf("%s/%s.%s/%s",
		partition, database, table, host)
	sql += fmt.Sprintf(" TO S3('%s/%s', '%s', '%s')",
		conf.Endpoint, key, conf.AccessKey, conf.SecretKey)
	sql += fmt.Sprintf(" SETTINGS structure_only = 1,compression_method='%s', compression_level=%d", conf.CompressMethod, conf.CompressLevel)
	return key, sql
}

/*
RESTORE TABLE default.test_ck_dataq_r50 PARTITION  '20230731'
FROM S3('http://192.168.101.94:49000/backup/20230731/default.test_ck_dataq_r50/192.168.101.93', 'VdmPbwvMlH8ryeqW', '8z16tUktXpvcjjy5M4MqXvCks5MMHb63') SETTINGS allow_non_empty_tables = 1
*/
func genResoreSql(database, table, partition, host string, conf config.S3) string {
	var sql string
	sql = fmt.Sprintf("RESTORE TABLE `%s`.`%s` ", database, table)
	if partition != "" {
		sql += fmt.Sprintf(" PARTITION '%s'", partition)
	}
	sql += fmt.Sprintf(" FROM S3('%s/%s/%s.%s/%s', '%s', '%s')",
		conf.Endpoint, partition, database, table, host, conf.AccessKey, conf.SecretKey)
	sql += fmt.Sprintf(" SETTINGS allow_non_empty_tables=true")
	return sql
}

func Paths(database, table, partition string, conf config.S3) (map[string]utils.PathInfo, error) {
	paths := make(map[string]utils.PathInfo)

	query := fmt.Sprintf(`SELECT path FROM system.parts WHERE (database = '%s') AND (table = '%s') AND (partition = '%s')`,
		database, table, partition)
	for i := range conns {
		conn, err := GetAvaliableConn(i)
		if err != nil {
			return nil, err
		}
		log.Logger.Debugf("[%s]%s", conn.h, query)
		rows, err := conn.c.Query(context.Background(), query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var allPaths []string
		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				return nil, err
			}
			if strings.HasSuffix(path, "/") {
				path += "*"
			}
			log.Logger.Debugf("path: %s", path)
			allPaths = append(allPaths, path)
		}
		if conf.CheckSum {
			for _, p := range allPaths {
				log.Logger.Debugf("shell: md5sum %s", p)
				out, err := utils.RemoteExecute(conn.opts, fmt.Sprintf("md5sum %s", p))
				if err != nil {
					log.Logger.Errorf("md5sum %s failed: %v", p, err)
					return nil, err
				}
				log.Logger.Debugf("out: %s", out)
				for _, line := range strings.Split(out, "\n") {
					if line == "" {
						continue
					}
					fields := strings.Fields(line)
					if len(fields) != 2 {
						return nil, fmt.Errorf("md5sum output format error: %s", line)
					}
					md5sum := fields[0]
					pp := strings.Split(fields[1], "/")
					partfiles := strings.Join(pp[len(pp)-2:], "/")
					key := fmt.Sprintf("%s/%s.%s/%s/data/%s/%s/%s",
						partition, database, table, conn.h, database, table, partfiles)
					paths[key] = utils.PathInfo{
						Host:  conn.h,
						RPath: key,
						LPath: fields[1],
						MD5:   md5sum,
					}
					log.Logger.Debugf("clickhouse local path:[%s] path: %s, key: %s, checksum: %s", conn.h, fields[1], key, md5sum)
				}
			}
		} else {
			for _, p := range allPaths {
				log.Logger.Debugf("shell: ls %s", p)
				out, err := utils.RemoteExecute(conn.opts, fmt.Sprintf("ls %s", p))
				if err != nil {
					log.Logger.Errorf("ls %s failed: %v", p, err)
					return nil, err
				}
				log.Logger.Debugf("out: %s", out)
				for _, line := range strings.Split(out, "\n") {
					if line == "" {
						continue
					}
					line = strings.TrimSuffix(line, "\r")
					pp := strings.Split(line, "/")
					partfiles := strings.Join(pp[len(pp)-2:], "/")
					key := fmt.Sprintf("%s/%s.%s/%s/data/%s/%s/%s",
						partition, database, table, conn.h, database, table, partfiles)
					paths[key] = utils.PathInfo{
						Host:  conn.h,
						RPath: key,
						LPath: line,
					}
					log.Logger.Debugf("clickhouse local path:[%s] path: %s, key: %s", conn.h, line, key)
				}
			}
		}
	}

	return paths, nil
}

func Ch2S3(database, table, partition string, conf config.S3) (uint64, error) {
	var wg sync.WaitGroup
	var lastErr error
	var rsize uint64
	wg.Add(len(conns))
	for i := range conns {
		conn, err := GetAvaliableConn(i)
		if err != nil {
			return rsize, err
		}
		go func(conn Conn) {
			defer wg.Done()
			key, query := genBackupSql(database, table, partition, conn.h, conf)
			log.Logger.Infof("backup sql => [%s]%s", conn.h, query)
			if err := retry.Do(
				func() error {
					//step0: 获取表数据
					log.Logger.Infof("[%s]step0 -> init", conn.h)
					paths, err := Paths(database, table, partition, conf)
					if err != nil {
						return err
					}
					//step1: 备份表schema
					log.Logger.Infof("[%s]step1 -> backup schema", conn.h)
				AGAIN:
					err = conn.c.Exec(context.Background(), query)
					if err != nil {
						var exception *clickhouse.Exception
						if errors.As(err, &exception) {
							if exception.Code == 598 {
								err = s3client.Remove(conf.Bucket, key)
								if err != nil {
									log.Logger.Errorf("[%s] clean data %s from s3 failed:%v", conn.h, key, err)
								}
								goto AGAIN
							}
						}
						return err
					}
					//step2: 备份数据
					log.Logger.Infof("[%s]step2 -> upload data", conn.h)
					if err := Upload(conn.opts, paths, conf); err != nil {
						return err
					}
					//step3: 校验数据
					log.Logger.Infof("[%s]step3 -> check sum", conn.h)
					s3size, err := s3client.CheckSum(conn.h, conf.Bucket, key, paths, conf)
					if err != nil {
						log.Logger.Errorf("[%s] check sum %s from s3 failed:%v", conn.h, key, err)
						return err
					}
					atomic.AddUint64(&rsize, s3size)
					log.Logger.Infof("[%s]%s %s backup success", conn.h, key, partition)
					return nil
				},
				retry.LastErrorOnly(true),
				retry.Attempts(conf.RetryTimes),
				retry.Delay(10*time.Second),
			); err != nil {
				if conf.CleanIfFail {
					// 删除s3上的不完整的数据
					log.Logger.Warnf("[%s] %v, try to clean", conn.h, err)
					err = s3client.Remove(conf.Bucket, key)
					if err != nil {
						log.Logger.Errorf("[%s] clean data %s from s3 failed:%v", conn.h, key, err)
					}
				}
				lastErr = err
				return
			}
		}(conn)
	}
	wg.Wait()
	return rsize, lastErr
}

func Restore(database, table, partition string, conf config.S3) error {
	var wg sync.WaitGroup
	var lastErr error
	wg.Add(len(conns))
	for i := range conns {
		conn, err := GetAvaliableConn(i)
		if err != nil {
			return err
		}
		go func(conn Conn) {
			defer wg.Done()
			query := genResoreSql(database, table, partition, conn.h, conf)
			log.Logger.Infof("restore sql => [%s]%s", conn.h, query)
			if err := retry.Do(
				func() error {
					err := conn.c.Exec(context.Background(), query)
					if err != nil {
						return err
					}
					return nil
				},
				retry.LastErrorOnly(true),
				retry.Attempts(conf.RetryTimes),
				retry.Delay(10*time.Second),
			); err != nil {
				lastErr = err
				return
			}
		}(conn)
	}
	wg.Wait()
	return lastErr
}

func Clean(database, table, partition string) error {
	for i := range conns {
		query := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PARTITION '%s'", database, table, partition)
		conn, err := GetAvaliableConn(i)
		log.Logger.Infof("execute sql => [%s]%s", conn.h, query)
		if err != nil {
			return err
		}
		err = conn.c.Exec(context.Background(), query)
		if err != nil {
			return err
		}
	}
	return nil
}
