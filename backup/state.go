package backup

import (
	"time"

	"github.com/YenchangChan/ch2s3/constant"
)

type State struct {
	start      time.Time
	elasped    int
	partitions int
	rows       uint64
	buncsize   uint64
	bcsize     uint64
	extval     int
	why        error
}

func NewState(rows, buncsize, bcsize uint64, partitions int) *State {
	return &State{
		start:      time.Now(),
		partitions: partitions,
		rows:       rows,
		buncsize:   buncsize,
		bcsize:     bcsize,
	}
}

func (s *State) Set(key string, value any) {
	switch key {
	case constant.STATE_ROWS:
		s.rows = value.(uint64)
	case constant.STATE_UNCOMPRESSED_SIZE:
		s.buncsize = value.(uint64)
	case constant.STATE_COMPRESSED_SIZE:
		s.bcsize = value.(uint64)
	}
}

func (s *State) Success() {
	s.elasped = int(time.Since(s.start).Seconds())
	s.extval = constant.BACKUP_SUCCESS
}

func (s *State) Failure(err error) {
	s.elasped = int(time.Since(s.start).Seconds())
	s.why = err
	s.extval = constant.BACKUP_FAILURE
}

func status(s int) string {
	if s == constant.BACKUP_SUCCESS {
		return "SUCCESS"
	} else {
		return "FAILURE"
	}
}
