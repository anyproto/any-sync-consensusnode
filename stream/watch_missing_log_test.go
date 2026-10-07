package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	consensus "github.com/anyproto/any-sync-consensusnode"
)

// A node creates a space log and watches it right away. A watch that does not find the log ends with
// an error, as clients expect, and leaves the stream able to watch the log again.
func TestStream_WatchMissingLog(t *testing.T) {
	const logId = "logId"
	existing := consensus.Log{Id: logId, Records: []consensus.Record{{Id: "1"}}}

	// fetchResults makes the db answer each lookup with the next result; the last one repeats
	fetchResults := func(fx *fixture, results ...error) *int {
		var calls int
		fx.mockDB.fetchLog = func(ctx context.Context, id string) (consensus.Log, error) {
			err := results[min(calls, len(results)-1)]
			calls++
			if err != nil {
				return consensus.Log{}, err
			}
			return existing, nil
		}
		return &calls
	}

	t.Run("a log missing on the first lookup is found on the second", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)
		calls := fetchResults(fx, consensuserr.ErrLogNotFound, nil)
		st := fx.NewStream()
		er := readEvents(st)

		st.WatchIds(ctx, []string{logId})

		assert.Equal(t, 2, *calls)
		assert.Equal(t, []string{logId}, st.LogIds())
		events := er.closeAndWait(t, st)
		require.Len(t, events, 1)
		assert.NoError(t, events[0].Err)
		assert.Equal(t, existing.Records, events[0].Records)
	})

	t.Run("a log missing on both lookups ends the watch with an error", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)
		calls := fetchResults(fx, consensuserr.ErrLogNotFound)
		st := fx.NewStream()
		er := readEvents(st)

		st.WatchIds(ctx, []string{logId})
		// the log appears and gets a record; the ended watch gets nothing
		fx.mockDB.receiver(logId, []consensus.Record{{Id: "2", PrevId: "1"}, {Id: "1"}})

		assert.Equal(t, 2, *calls)
		assert.Empty(t, st.LogIds())
		events := er.closeAndWait(t, st)
		require.Len(t, events, 1)
		assert.Equal(t, consensuserr.ErrLogNotFound, events[0].Err)
	})

	t.Run("watching again after the log is created delivers it", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)
		fetchResults(fx, consensuserr.ErrLogNotFound, consensuserr.ErrLogNotFound, nil)
		st := fx.NewStream()
		er := readEvents(st)

		st.WatchIds(ctx, []string{logId})
		st.WatchIds(ctx, []string{logId})
		fx.mockDB.receiver(logId, []consensus.Record{{Id: "2", PrevId: "1"}, {Id: "1"}})

		assert.Equal(t, []string{logId}, st.LogIds())
		events := er.closeAndWait(t, st)
		require.Len(t, events, 3)
		assert.Equal(t, consensuserr.ErrLogNotFound, events[0].Err)
		assert.Equal(t, existing.Records, events[1].Records)
		require.Len(t, events[2].Records, 1)
		assert.Equal(t, "2", events[2].Records[0].Id)
	})

	t.Run("another error is not retried and the log can be watched again", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)
		calls := fetchResults(fx, consensuserr.ErrUnexpected, nil)
		st := fx.NewStream()
		er := readEvents(st)

		st.WatchIds(ctx, []string{logId})
		assert.Equal(t, 1, *calls)
		assert.Empty(t, st.LogIds())

		st.WatchIds(ctx, []string{logId})
		assert.Equal(t, []string{logId}, st.LogIds())
		events := er.closeAndWait(t, st)
		require.Len(t, events, 2)
		assert.Equal(t, consensuserr.ErrUnexpected, events[0].Err)
		assert.Equal(t, existing.Records, events[1].Records)
	})
}

// eventReader collects the stream's events in order, as the client receives them
type eventReader struct {
	mu       sync.Mutex
	events   []consensus.Log
	finished chan struct{}
}

func readEvents(st *Stream) *eventReader {
	er := &eventReader{finished: make(chan struct{})}
	go func() {
		defer close(er.finished)
		for {
			logs := st.WaitLogs()
			if len(logs) == 0 {
				return
			}
			er.mu.Lock()
			er.events = append(er.events, logs...)
			er.mu.Unlock()
		}
	}()
	return er
}

func (er *eventReader) closeAndWait(t *testing.T, st *Stream) []consensus.Log {
	st.Close()
	select {
	case <-er.finished:
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for the stream to finish")
	}
	er.mu.Lock()
	defer er.mu.Unlock()
	return er.events
}
