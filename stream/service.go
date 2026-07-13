package stream

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/metric"
	"github.com/cheggaaa/mb/v3"
	"go.uber.org/zap"

	consensus "github.com/anyproto/any-sync-consensusnode"
	"github.com/anyproto/any-sync-consensusnode/db"
)

const CName = "consensus.stream"

var log = logger.NewNamed(CName)

var (
	cacheTTL = time.Minute
)

type ctxLog uint

const (
	ctxLogKey ctxLog = 1
)

func New() Service {
	return &service{}
}

// Service maintains a cache for logs (receive updates from db) and creates new stream objects with able to subscribe/unsubscribe to log ids
type Service interface {
	// NewStream creates new stream with able to watch and unwatch log ids
	NewStream() *Stream
	app.ComponentRunnable
}

type service struct {
	db           db.Service
	cache        ocache.OCache
	lastStreamId uint64
}

func (s *service) Init(a *app.App) (err error) {
	s.db = a.MustComponent(db.CName).(db.Service)
	cacheOpts := []ocache.Option{
		ocache.WithTTL(cacheTTL),
		ocache.WithLogger(log.Named(CName).Sugar()),
	}
	if ms := a.Component(metric.CName); ms != nil {
		cacheOpts = append(cacheOpts, ocache.WithPrometheus(ms.(metric.Metric).Registry(), "consensus", "logcache"))
	}
	s.cache = ocache.New(s.loadLog, cacheOpts...)

	if err = s.db.SetChangeReceiver(s.receiveChange); err != nil {
		return
	}
	return s.db.SetResetReceiver(s.resync)
}

func (s *service) Name() (name string) {
	return CName
}

func (s *service) Run(ctx context.Context) (err error) {
	return nil
}

func (s *service) NewStream() *Stream {
	return &Stream{
		id:     atomic.AddUint64(&s.lastStreamId, 1),
		logIds: make(map[string]struct{}),
		mb:     mb.New[consensus.Log](100),
		s:      s,
	}
}

// AddStream to object with given logId
func (s *service) AddStream(ctx context.Context, logId string, stream *Stream) (err error) {
	obj, err := s.getObject(ctx, logId)
	if err != nil {
		return err
	}
	obj.AddStream(stream)
	return
}

// RemoveStream from object with five logId
func (s *service) RemoveStream(ctx context.Context, logId string, streamId uint64) (err error) {
	obj, err := s.getObject(ctx, logId)
	if err != nil {
		return err
	}
	obj.RemoveStream(streamId)
	return
}

func (s *service) loadLog(ctx context.Context, logId string) (value ocache.Object, err error) {
	if ctxLog := ctx.Value(ctxLogKey); ctxLog != nil {
		return &object{
			logId:   ctxLog.(consensus.Log).Id,
			records: ctxLog.(consensus.Log).Records,
			streams: make(map[uint64]*Stream),
		}, nil
	}
	dbLog, err := s.db.FetchLog(ctx, logId, "")
	if err != nil {
		return nil, err
	}
	return &object{
		logId:   dbLog.Id,
		records: dbLog.Records,
		streams: make(map[uint64]*Stream),
	}, nil
}

// resync reloads every cached log from the db and sends the missed records to the subscribed streams.
// It is called when the db change stream was interrupted: the updates made while it was down are never
// replayed, so without a resync the cache would stay behind the db forever and would serve stale records
// to every new subscriber.
func (s *service) resync() {
	var objects []*object
	s.cache.ForEach(func(v ocache.Object) bool {
		objects = append(objects, v.(*object))
		return true
	})
	log.Info("resyncing logs with the db", zap.Int("count", len(objects)))
	for _, obj := range objects {
		dbLog, err := s.db.FetchLog(context.Background(), obj.logId, "")
		if err != nil {
			log.Error("resync: can't fetch log", zap.String("logId", obj.logId), zap.Error(err))
			continue
		}
		obj.AddRecords(dbLog.Records)
	}
}

func (s *service) receiveChange(logId string, records []consensus.Record) {
	ctx := context.WithValue(context.Background(), ctxLogKey, consensus.Log{Id: logId, Records: records})
	obj, err := s.getObject(ctx, logId)
	if err != nil {
		log.Error("failed load object from cache", zap.Error(err))
		return
	}
	obj.AddRecords(records)
}

func (s *service) getObject(ctx context.Context, logId string) (*object, error) {
	cacheObj, err := s.cache.Get(ctx, logId)
	if err != nil {
		return nil, err
	}
	return cacheObj.(*object), nil
}

func (s *service) Close(ctx context.Context) (err error) {
	return s.cache.Close()
}
