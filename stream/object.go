package stream

import (
	consensus "github.com/anyproto/any-sync-consensusnode"
	"sync"
	"time"
)

// object is a cache entry that holds the actual log state and maintains added streams
type object struct {
	logId   string
	records []consensus.Record

	streams map[uint64]*Stream

	mu sync.Mutex
}

// AddRecords adds new records to the log and sends new records to streams
// The records source is db and called via stream.Service
func (o *object) AddRecords(recs []consensus.Record) {
	o.mu.Lock()
	if len(recs) <= len(o.records) {
		o.mu.Unlock()
		return
	}
	diff := recs[0 : len(recs)-len(o.records)]
	o.records = recs
	streams := make([]*Stream, 0, len(o.streams))
	for _, st := range o.streams {
		streams = append(streams, st)
	}
	logId := o.logId
	o.mu.Unlock()

	for _, st := range streams {
		_ = st.AddRecords(logId, diff)
	}
}

// Records returns all log records
func (o *object) Records() []consensus.Record {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.records
}

// AddStream adds stream to the object
func (o *object) AddStream(s *Stream) {
	o.mu.Lock()
	o.streams[s.id] = s
	records := o.records
	logId := o.logId
	o.mu.Unlock()
	_ = s.AddRecords(logId, records)
}

// RemoveStream remove stream from object
func (o *object) RemoveStream(id uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.streams, id)
}

func (o *object) TryClose(ttl time.Duration) (ok bool, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.streams) > 0 {
		return
	} else {
		return true, o.Close()
	}
}

func (o *object) Close() (err error) {
	return nil
}
