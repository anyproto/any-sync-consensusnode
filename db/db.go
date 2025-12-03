//go:generate mockgen -destination mock_db/mock_db.go github.com/anyproto/any-sync-consensusnode/db Service
package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	consensus "github.com/anyproto/any-sync-consensusnode"
)

const CName = "consensus.db"

const (
	settingsColl = "settings"
	payloadColl  = "payload"
)

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
	FetchLog(ctx context.Context, logId, afterRecordId string) (log consensus.Log, err error)
	// SetChangeReceiver sets the receiver for updates, it must be called before app.Run stage
	SetChangeReceiver(receiver ChangeReceiver) (err error)
	// SetDeletionId sets the last deleted log id
	SetDeletionId(ctx context.Context, lastId string) (err error)
	// GetDeletionId gets the last deletion log id
	GetDeletionId(ctx context.Context) (lastId string, err error)
	app.ComponentRunnable
}

type service struct {
	conf         Config
	logColl      *mongo.Collection
	settingsColl *mongo.Collection
	payloadColl  *mongo.Collection
	running      bool

	changeReceiver ChangeReceiver
	streamCtx      context.Context
	streamCancel   context.CancelFunc
	listenerDone   chan struct{}
	client         *mongo.Client
}

func (s *service) Init(a *app.App) (err error) {
	s.conf = a.MustComponent("config").(configGetter).GetDB()
	return nil
}

func (s *service) Name() (name string) {
	return CName
}

func (s *service) Run(ctx context.Context) (err error) {
	s.client, err = mongo.Connect(ctx, options.Client().ApplyURI(s.conf.Connect))
	if err != nil {
		return err
	}
	pingCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err = s.client.Ping(pingCtx, readpref.Primary()); err != nil {
		return err
	}
	s.logColl = s.client.Database(s.conf.Database).Collection(s.conf.LogCollection)
	s.running = true
	if s.changeReceiver != nil {
		if err = s.runStreamListener(ctx); err != nil {
			return err
		}
	}
	s.settingsColl = s.client.Database(s.conf.Database).Collection(settingsColl)
	s.payloadColl = s.client.Database(s.conf.Database).Collection(payloadColl)
	if _, err = s.payloadColl.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "logId", Value: 1}}}); err != nil {
		return err
	}
	return s.runMigrations(ctx)
}

func (s *service) tx(ctx context.Context, f func(txCtx mongo.SessionContext) error) (err error) {
	return s.client.UseSessionWithOptions(
		ctx,
		options.Session().SetDefaultReadConcern(readconcern.Majority()),
		func(txCtx mongo.SessionContext) error {
			if err := txCtx.StartTransaction(); err != nil {
				return err
			}
			if err := f(txCtx); err != nil {
				_ = txCtx.AbortTransaction(context.Background())
				return err
			}
			return txCtx.CommitTransaction(context.Background())
		})
}

func (s *service) AddLog(ctx context.Context, l consensus.Log) (err error) {
	return s.tx(ctx, func(txCtx mongo.SessionContext) error {
		for i, record := range l.Records {
			if err := s.savePayload(txCtx, consensus.Payload{
				Id:      record.Id,
				LogId:   l.Id,
				Payload: record.Payload,
			}); err != nil {
				return err
			}
			l.Records[i].Payload = nil
		}
		_, err := s.logColl.InsertOne(txCtx, l)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return consensuserr.ErrLogExists
			} else {
				return errors.Join(consensuserr.ErrUnexpected, err)
			}
		}
		return nil
	})

}

type findLogQuery struct {
	Id string `bson:"_id"`
}

func (s *service) DeleteLog(ctx context.Context, logId string) (err error) {
	return s.tx(ctx, func(txCtx mongo.SessionContext) error {
		res, err := s.logColl.DeleteOne(txCtx, findLogQuery{Id: logId})
		if err != nil {
			return err
		}
		if res.DeletedCount == 0 {
			return consensuserr.ErrLogNotFound
		}
		res, err = s.payloadColl.DeleteMany(txCtx, bson.D{{"logId", logId}})
		if err != nil {
			return err
		}
		return nil
	})
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

func (s *service) AddRecord(ctx context.Context, logId string, record consensus.Record) error {
	return s.tx(ctx, func(txCtx mongo.SessionContext) (err error) {
		// save payload in a separate collection to avoid the one doc size limit
		if err = s.savePayload(txCtx, consensus.Payload{
			Id:      record.Id,
			LogId:   logId,
			Payload: record.Payload,
		}); err != nil {
			return err
		}

		// try to add record to the log
		var upd updateOp
		record.Payload = nil
		upd.Push.Records.Each = []consensus.Record{record}
		result, err := s.logColl.UpdateOne(txCtx, findRecordQuery{
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
	})
}

func (s *service) savePayload(ctx context.Context, payload consensus.Payload) (err error) {
	if payload.Payload == nil {
		return fmt.Errorf("payload is nil")
	}
	_, err = s.payloadColl.InsertOne(ctx, payload)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return
}

func (s *service) FetchLog(ctx context.Context, logId, afterRecordId string) (l consensus.Log, err error) {
	if err = s.logColl.FindOne(ctx, findLogQuery{Id: logId}).Decode(&l); err != nil {
		if err == mongo.ErrNoDocuments {
			err = consensuserr.ErrLogNotFound
		}
		return
	}
	return s.injectPayloads(ctx, l, afterRecordId)
}

func (s *service) injectPayloads(ctx context.Context, l consensus.Log, afterRecordId string) (res consensus.Log, err error) {
	var payloads = make(map[string]int, len(l.Records))
	res = l
	res.Records = l.Records[:0]
	var add = afterRecordId == ""
	for _, record := range l.Records {
		if add {
			res.Records = append(res.Records, record)
			payloads[record.Id] = len(res.Records) - 1
		} else if record.Id == afterRecordId {
			add = true
		}
	}

	cur, err := s.payloadColl.Find(ctx, bson.D{{"logId", l.Id}})
	if err != nil {
		return
	}
	defer func() {
		_ = cur.Close(ctx)
	}()

	for cur.Next(ctx) {
		curRecord := consensus.Payload{}
		if err = cur.Decode(&curRecord); err != nil {
			return
		}
		if idx, ok := payloads[curRecord.Id]; ok {
			res.Records[idx].Payload = curRecord.Payload
		}
	}

	for _, rec := range res.Records {
		if rec.Payload == nil {
			return res, fmt.Errorf("payload not found for record %s", rec.Id)
		}
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
		logWithRecords, err := s.injectPayloads(s.streamCtx, consensus.Log{Id: res.DocumentKey.Id, Records: res.UpdateDescription.UpdateFields.Records}, "")
		if err != nil {
			log.Error("failed to add payloads to log", zap.Error(err))
			continue
		}
		s.changeReceiver(res.DocumentKey.Id, logWithRecords.Records)
	}
}

func (s *service) SetDeletionId(ctx context.Context, lastId string) (err error) {
	updateOpts := options.Update().SetUpsert(true)
	_, err = s.settingsColl.UpdateOne(ctx, bson.D{{"_id", "settings"}}, bson.D{{"$set", bson.D{{"logId", lastId}}}}, updateOpts)
	return
}

func (s *service) GetDeletionId(ctx context.Context) (lastId string, err error) {
	var res struct {
		LogId string `bson:"logId"`
	}
	err = s.settingsColl.FindOne(ctx, bson.D{{"_id", "settings"}}).Decode(&res)
	if errors.Is(err, mongo.ErrNoDocuments) {
		err = nil
	}
	return res.LogId, err
}

func (s *service) Close(ctx context.Context) (err error) {
	if s.client != nil {
		err = s.client.Disconnect(ctx)
		s.client = nil
	}
	if s.listenerDone != nil {
		s.streamCancel()
		<-s.listenerDone
	}
	return
}
