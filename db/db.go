//go:generate mockgen -destination mock_db/mock_db.go github.com/anyproto/any-sync-consensusnode/db Service
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	consensus "github.com/anyproto/any-sync-consensusnode"
	"github.com/anyproto/any-sync-consensusnode/config"
)

const CName = "consensus.db"

var log = logger.NewNamed(CName)

func New() Service {
	return &service{}
}

type ChangeReceiver func(logId string, records []consensus.Record)

type Service interface {
	// AddLog adds new log db
	AddLog(ctx context.Context, log consensus.Log) (err error)
	// DeleteLog deletes the log
	DeleteLog(ctx context.Context, logId string) error
	// AddRecord adds new record to existing log
	// returns consensuserr.ErrConflict if record didn't match or log not found
	AddRecord(ctx context.Context, logId string, record consensus.Record) (err error)
	// FetchLog gets log by id
	FetchLog(ctx context.Context, logId string) (log consensus.Log, err error)
	// SetChangeReceiver sets the receiver for updates, it must be called before app.Run stage
	SetChangeReceiver(receiver ChangeReceiver) (err error)
	app.ComponentRunnable
}

type service struct {
	conf           config.Mongo
	logColl        *mongo.Collection
	running        bool
	changeReceiver ChangeReceiver

	streamCtx    context.Context
	streamCancel context.CancelFunc
	listenerDone chan struct{}
}

func (s *service) Init(a *app.App) (err error) {
	s.conf = a.MustComponent(config.CName).(*config.Config).Mongo
	return nil
}

func (s *service) Name() (name string) {
	return CName
}

func (s *service) Run(ctx context.Context) (err error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(s.conf.Connect))
	if err != nil {
		return err
	}
	pingCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err = client.Ping(pingCtx, readpref.Primary()); err != nil {
		return err
	}
	s.logColl = client.Database(s.conf.Database).Collection(s.conf.LogCollection)
	s.running = true
	if s.changeReceiver != nil {
		if err = s.runStreamListener(ctx); err != nil {
			return err
		}
	}
	return
}

func (s *service) AddLog(ctx context.Context, l consensus.Log) (err error) {
	_, err = s.logColl.InsertOne(ctx, l)
	if mongo.IsDuplicateKeyError(err) {
		return consensuserr.ErrLogExists
	}
	return
}

type findLogQuery struct {
	Id string `bson:"_id"`
}

func (s *service) DeleteLog(ctx context.Context, logId string) (err error) {
	res, err := s.logColl.DeleteOne(ctx, findLogQuery{Id: logId})
	if err != nil {
		return
	}
	if res.DeletedCount == 0 {
		return consensuserr.ErrLogNotFound
	}
	return
}

type findRecordQuery struct {
	Id           string `bson:"_id"`
	LastRecordId string `bson:"records.0.id"`
}

type updateOp struct {
	Push struct {
		Records struct {
			Each []consensus.Record `bson:"$each"`
			Pos  int                `bson:"$position"`
		} `bson:"records"`
	} `bson:"$push"`
}

func (s *service) AddRecord(ctx context.Context, logId string, record consensus.Record) (err error) {
	var upd updateOp
	upd.Push.Records.Each = []consensus.Record{record}
	result, err := s.logColl.UpdateOne(ctx, findRecordQuery{
		Id:           logId,
		LastRecordId: record.PrevId,
	}, upd)
	if err != nil {
		log.Error("addRecord update error", zap.Error(err))
		err = consensuserr.ErrUnexpected
		return
	}
	if result.ModifiedCount == 0 {
		err = consensuserr.ErrConflict
		return
	}
	return
}

func (s *service) FetchLog(ctx context.Context, logId string) (l consensus.Log, err error) {
	if err = s.logColl.FindOne(ctx, findLogQuery{Id: logId}).Decode(&l); err != nil {
		if err == mongo.ErrNoDocuments {
			err = consensuserr.ErrLogNotFound
		}
		return
	}
	return
}

func (s *service) SetChangeReceiver(receiver ChangeReceiver) (err error) {
	if s.running {
		return fmt.Errorf("set receiver must be called before Run")
	}
	s.changeReceiver = receiver
	return
}

type matchPipeline struct {
	Match struct {
		OT string `bson:"operationType"`
	} `bson:"$match"`
}

func (s *service) runStreamListener(ctx context.Context) (err error) {
	var mp matchPipeline
	mp.Match.OT = "update"
	stream, err := s.logColl.Watch(ctx, []matchPipeline{mp})
	if err != nil {
		return
	}
	s.listenerDone = make(chan struct{})
	s.streamCtx, s.streamCancel = context.WithCancel(context.Background())
	go s.streamListener(stream)
	return
}

type streamResult struct {
	DocumentKey struct {
		Id string `bson:"_id"`
	} `bson:"documentKey"`
	UpdateDescription struct {
		UpdateFields struct {
			Records []consensus.Record `bson:"records"`
		} `bson:"updatedFields"`
	} `bson:"updateDescription"`
}

func (s *service) streamListener(stream *mongo.ChangeStream) {
	defer close(s.listenerDone)
	for stream.Next(s.streamCtx) {
		var res streamResult
		if err := stream.Decode(&res); err != nil {
			// mongo driver maintains connections and handles reconnects so that the stream will work as usual in these cases
			// here we have an unexpected error and should stop any operations to avoid an inconsistent state between db and cache
			log.Fatal("stream decode error:", zap.Error(err))
		}
		s.changeReceiver(res.DocumentKey.Id, res.UpdateDescription.UpdateFields.Records)
	}
}

func (s *service) Close(ctx context.Context) (err error) {
	if s.logColl != nil {
		err = s.logColl.Database().Client().Disconnect(ctx)
		s.logColl = nil
	}
	if s.listenerDone != nil {
		s.streamCancel()
		<-s.listenerDone
	}
	return
}
