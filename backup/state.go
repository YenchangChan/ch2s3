package backup

import (
	"time"
)

const (
	BACKUP_SUCCESS = 0
	BACKUP_FAILURE = 1
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

func (s *State) Success() {
	s.elasped = int(time.Since(s.start).Seconds())
	s.extval = BACKUP_SUCCESS
}

func (s *State) Failure(err error) {
	s.elasped = int(time.Since(s.start).Seconds())
	s.why = err
	s.extval = BACKUP_FAILURE
}

func status(s int) string {
	if s == BACKUP_SUCCESS {
		return "SUCCESS"
	} else {
		return "FAILURE"
	}
}
