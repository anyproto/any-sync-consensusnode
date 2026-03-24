package stream

import (
	"context"
	consensus "github.com/anyproto/any-sync-consensusnode"
	"github.com/cheggaaa/mb/v3"
	"go.uber.org/zap"
	"sync"
)

// Stream is a buffer that receives updates from object and gives back to a client
type Stream struct {
	id     uint64
	logIds map[string]struct{}
	mu     sync.Mutex
	mb     *mb.MB[consensus.Log]
	s      *service
	closed bool
}

// LogIds returns watched log ids
func (s *Stream) LogIds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	logIds := make([]string, 0, len(s.logIds))
	for logId := range s.logIds {
		logIds = append(logIds, logId)
	}
	return logIds
}

// AddRecords adds new records to stream, called by objects
func (s *Stream) AddRecords(logId string, records []consensus.Record) (err error) {
	return s.mb.Add(context.TODO(), consensus.Log{Id: logId, Records: records})
}

// WaitLogs wait for new log records
// empty returned slice means that stream is closed
func (s *Stream) WaitLogs() []consensus.Log {
	logs, _ := s.mb.Wait(context.TODO())
	return logs
}

// WatchIds adds given ids to subscription
func (s *Stream) WatchIds(ctx context.Context, logIds []string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	var newIds []string
	for _, logId := range logIds {
		if _, ok := s.logIds[logId]; !ok {
			s.logIds[logId] = struct{}{}
			newIds = append(newIds, logId)
		}
	}
	s.mu.Unlock()

	for _, logId := range newIds {
		if addErr := s.s.AddStream(ctx, logId, s); addErr != nil {
			log.Info("can't add stream for log", zap.String("logId", logId), zap.Error(addErr))
			_ = s.mb.Add(ctx, consensus.Log{
				Id:  logId,
				Err: addErr,
			})
			continue
		}
		// If stream was closed while we were adding, undo the add to prevent leak
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = s.s.RemoveStream(ctx, logId, s.id)
			return
		}
		s.mu.Unlock()
	}
}

// UnwatchIds removes given ids from subscription
func (s *Stream) UnwatchIds(ctx context.Context, logIds []string) {
	s.mu.Lock()
	var removeIds []string
	for _, logId := range logIds {
		if _, ok := s.logIds[logId]; ok {
			delete(s.logIds, logId)
			removeIds = append(removeIds, logId)
		}
	}
	s.mu.Unlock()

	for _, logId := range removeIds {
		if remErr := s.s.RemoveStream(ctx, logId, s.id); remErr != nil {
			log.Warn("can't remove stream for log", zap.String("logId", logId), zap.Error(remErr))
		}
	}
}

// Close closes stream and unsubscribes all ids
func (s *Stream) Close() {
	_ = s.mb.Close()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	logIds := make([]string, 0, len(s.logIds))
	for logId := range s.logIds {
		logIds = append(logIds, logId)
	}
	s.mu.Unlock()

	for _, logId := range logIds {
		_ = s.s.RemoveStream(context.TODO(), logId, s.id)
	}
}
