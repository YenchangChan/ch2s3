package backup

import (
	"fmt"

	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/log"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

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
	sql += fmt.Sprintf("TO S3('%s/%s/%s/%s.%s', '%s', '%s')",
		conf.Endpoint, partition, host, database, table, conf.AccessKey, conf.SecretKey)

	sql += fmt.Sprintf(" SETTINGS max_execution_time = 0, compression_method='%s', compression_level=%d", conf.CompressMethod, conf.CompressLevel)
	log.Logger.Debugf("backup sql => %s", sql)
	return sql
}

const (
	_         = iota
	KB uint64 = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

func formatReadableSize(size uint64) string {
	if size < KB {
		return fmt.Sprintf("%.2f B", float64(size)/float64(1))
	} else if size < MB {
		return fmt.Sprintf("%.2f KiB", float64(size)/float64(KB))
	} else if size < GB {
		return fmt.Sprintf("%.2f MiB", float64(size)/float64(MB))
	} else if size < TB {
		return fmt.Sprintf("%.2f GiB", float64(size)/float64(GB))
	} else if size < PB {
		return fmt.Sprintf("%.2f TiB", float64(size)/float64(TB))
	} else {
		return fmt.Sprintf("%.2f PiB", float64(size)/float64(PB))
	}

}

func formatBytes(rbytes uint64) string {
	p := message.NewPrinter(language.English)
	return p.Sprintf("%f", rbytes)
}
