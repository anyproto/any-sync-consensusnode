package stream

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	consensus "github.com/anyproto/any-sync-consensusnode"
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
	defer o.mu.Unlock()

	if len(recs) <= len(o.records) {
		return
	}
	diff := recs[0 : len(recs)-len(o.records)]
	o.records = recs
	for _, st := range o.streams {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := st.AddRecords(ctx, o.logId, diff); err != nil {
			log.Warn("slow consumer: failed to add records to stream", zap.String("logId", o.logId), zap.Uint64("streamId", st.id), zap.Error(err))
		}
		cancel()
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
	defer o.mu.Unlock()
	o.streams[s.id] = s
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := s.AddRecords(ctx, o.logId, o.records); err != nil {
		log.Warn("slow consumer: failed to send initial records", zap.String("logId", o.logId), zap.Uint64("streamId", s.id), zap.Error(err))
	}
	cancel()
	return
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
