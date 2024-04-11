package backup

import "time"

const (
	BACKUP_SUCCESS = 0
	BACKUP_FAILURE = 1
)

type State struct {
	start   time.Time
	elasped int
	rows    uint64
	bsize   uint64
	extval  int
	why     error
}

func NewState(rows, bsize uint64) *State {
	return &State{
		start: time.Now(),
		rows:  rows,
		bsize: bsize,
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
