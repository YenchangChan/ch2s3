package ch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/YenchangChan/ch2s3/config"
	"github.com/avast/retry-go/v4"
)

type Conn struct {
	h string
	c driver.Conn
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
				ReadTimeout: 3600 * time.Second,
			}
			c, err := clickhouse.Open(&opts)
			if err != nil {
				log.Printf("[%s]connect failed: %v", replica, err)
				lastErr = err
			}

			if err = c.Ping(context.Background()); err != nil {
				log.Printf("[%s]ping failed: %v", replica, err)
				lastErr = err
			}
			shardConns = append(shardConns, Conn{
				h: replica,
				c: c,
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

func Size(database, table, partition string) (uint64, error) {
	var lastErr error
	var wg sync.WaitGroup
	var bytes uint64
	wg.Add(len(conns))
	query := fmt.Sprintf("SELECT sum(data_uncompressed_bytes) FROM system.parts WHERE partition = '%s' AND database = '%s' AND table = '%s'",
		partition, database, table)
	log.Printf("execute sql => %s", query)
	for i := range conns {
		conn, err := GetAvaliableConn(i)
		if err != nil {
			return 0, err
		}
		go func(c driver.Conn) {
			defer wg.Done()
			var bsize uint64
			err := c.QueryRow(context.Background(), query).Scan(&bsize)
			if err != nil {
				lastErr = err
				return
			}
			bytes += bsize
		}(conn.c)
	}
	wg.Wait()
	log.Printf("execute result => %v", bytes)
	return bytes, lastErr
}

func Rows(database, table, partition string) (uint64, error) {
	var lastErr error
	var wg sync.WaitGroup
	var count uint64
	wg.Add(len(conns))
	query := fmt.Sprintf("SELECT sum(rows) FROM system.parts WHERE partition = '%s' AND database = '%s' AND table = '%s'",
		partition, database, table)
	log.Printf("execute sql => %s", query)
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
			count += cnt
		}(conn.c)
	}
	wg.Wait()
	log.Printf("execute result => %v", count)
	return count, lastErr
}

/*
BACKUP TABLE default.test_ck_dataq_r77 PARTITION '20230731' TO S3('http://192.168.101.94:49000/backup/20230731', 'VdmPbwvMlH8ryeqW', '8z16tUktXpvcjjy5M4MqXvCks5MMHb63')
SETTINGS compression_method='lz4', compression_level=3
*/
func genBackupSql(database, table, partition, host string, conf config.S3) string {
	var sql string
	sql = fmt.Sprintf("BACKUP TABLE `%s`.`%s` ", database, table)
	if partition != "" {
		sql += fmt.Sprintf(" PARTITION '%s'", partition)
	}
	sql += fmt.Sprintf(" TO S3('%s/%s/%s.%s/%s', '%s', '%s')",
		conf.Endpoint, partition, database, table, host, conf.AccessKey, conf.SecretKey)
	sql += fmt.Sprintf(" SETTINGS compression_method='%s', compression_level=%d", conf.CompressMethod, conf.CompressLevel)
	log.Printf("backup sql => %s", sql)
	return sql
}

func Ch2S3(database, table, partition string, conf config.S3) error {
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
			query := genBackupSql(database, table, partition, conn.h, conf)
			if err := retry.Do(
				func() error {
					err := conn.c.Exec(context.Background(), query)
					if err != nil {
						if conf.IgnoreExists {
							var exception *clickhouse.Exception
							if errors.As(err, &exception) {
								if exception.Code == 598 {
									log.Printf("[%s] %v, ignore it", conn.h, exception.Message)
									return nil
								}
							}
						}
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
		log.Printf("execute sql => %s", query)
		conn, err := GetAvaliableConn(i)
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
