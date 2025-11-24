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
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	consensus "github.com/anyproto/any-sync-consensusnode"
)

const CName = "consensus.db"

const settingsColl = "settings"

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
	// SetDeletionId sets the last deleted log id
	SetDeletionId(ctx context.Context, lastId string) (err error)
	// GetDeletionId gets the last deletion log id
	GetDeletionId(ctx context.Context) (lastId string, err error)
	app.ComponentRunnable
}

type service struct {
	conf           Config
	logColl        *mongo.Collection
	payloadColl    *mongo.Collection
	settingsColl   *mongo.Collection
	running        bool
	changeReceiver ChangeReceiver

	streamCtx    context.Context
	streamCancel context.CancelFunc
	listenerDone chan struct{}
}

func (s *service) Init(a *app.App) (err error) {
	s.conf = a.MustComponent("config").(configGetter).GetDB()
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
	s.settingsColl = client.Database(s.conf.Database).Collection(settingsColl)

	// Initialize payloads collection (default to "payloads" if not configured)
	payloadCollName := s.conf.PayloadCollection
	if payloadCollName == "" {
		payloadCollName = "payloads"
	}
	s.payloadColl = client.Database(s.conf.Database).Collection(payloadCollName)

	// Run migration before marking service as running
	if err = s.RunMigration(ctx); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	s.running = true
	if s.changeReceiver != nil {
		if err = s.runStreamListener(ctx); err != nil {
			return err
		}
	}
	return
}

func (s *service) AddLog(ctx context.Context, l consensus.Log) (err error) {
	// Make a copy to avoid modifying the input parameter
	logToStore := consensus.Log{
		Id:      l.Id,
		Records: make([]consensus.Record, len(l.Records)),
	}
	copy(logToStore.Records, l.Records)

	// Process records to store payloads in content-addressable storage
	for i := range logToStore.Records {
		record := &logToStore.Records[i]
		if len(record.Payload) > 0 {
			hash := computePayloadHash(record.Payload)

			// Store payload with hash as _id (upsert to handle duplicates)
			payload := consensus.Payload{
				Hash: hash,
				Data: record.Payload,
			}

			opts := options.Update().SetUpsert(true)
			_, err = s.payloadColl.UpdateOne(
				ctx,
				bson.M{"_id": hash},
				bson.M{"$setOnInsert": payload},
				opts,
			)
			if err != nil {
				log.Error("failed to store payload in AddLog", zap.Error(err))
				return consensuserr.ErrUnexpected
			}

			// Update record to use hash reference and clear embedded payload
			record.PayloadHash = hash
			record.Payload = nil
		}
	}

	_, err = s.logColl.InsertOne(ctx, logToStore)
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
	// Store payload in content-addressable storage if present
	if len(record.Payload) > 0 {
		hash := computePayloadHash(record.Payload)

		// Store payload with hash as _id (upsert to handle duplicates)
		payload := consensus.Payload{
			Hash: hash,
			Data: record.Payload,
		}

		opts := options.Update().SetUpsert(true)
		_, err = s.payloadColl.UpdateOne(
			ctx,
			bson.M{"_id": hash},
			bson.M{"$setOnInsert": payload},
			opts,
		)
		if err != nil {
			log.Error("failed to store payload", zap.Error(err))
			return consensuserr.ErrUnexpected
		}

		// Update record to use hash reference and clear embedded payload
		record.PayloadHash = hash
		record.Payload = nil
	}

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

	// Hydrate payloads for records that use content-addressable storage
	if err = s.hydratePayloads(ctx, &l); err != nil {
		return
	}

	return
}

// hydratePayloads fetches and populates payloads for all records in the log
func (s *service) hydratePayloads(ctx context.Context, l *consensus.Log) error {
	numRecords := len(l.Records)
	if numRecords == 0 {
		return nil
	}

	// Pre-allocate with capacity to avoid reallocations
	// Worst case: all records have unique payloads
	hashes := make([]string, 0, numRecords)
	hashToIndices := make(map[string][]int, numRecords)

	for i := range l.Records {
		record := &l.Records[i]
		if record.PayloadHash != "" {
			if _, exists := hashToIndices[record.PayloadHash]; !exists {
				hashes = append(hashes, record.PayloadHash)
			}
			hashToIndices[record.PayloadHash] = append(hashToIndices[record.PayloadHash], i)
		}
	}

	if len(hashes) == 0 {
		return nil
	}

	// Batch fetch all payloads
	cursor, err := s.payloadColl.Find(ctx, bson.M{"_id": bson.M{"$in": hashes}})
	if err != nil {
		return fmt.Errorf("failed to fetch payloads: %w", err)
	}
	defer cursor.Close(ctx)

	// Build hash -> payload map with known size
	payloadMap := make(map[string][]byte, len(hashes))
	for cursor.Next(ctx) {
		var payload consensus.Payload
		if err := cursor.Decode(&payload); err != nil {
			return fmt.Errorf("failed to decode payload: %w", err)
		}
		payloadMap[payload.Hash] = payload.Data
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	// Populate payloads in records
	for hash, indices := range hashToIndices {
		data, ok := payloadMap[hash]
		if !ok {
			return fmt.Errorf("payload not found for hash: %s", hash)
		}
		// Share the same []byte across all records with the same payload
		// This is safe as long as records are read-only after fetching
		for _, i := range indices {
			l.Records[i].Payload = data
		}
	}

	return nil
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
