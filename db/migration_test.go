package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	consensus "github.com/anyproto/any-sync-consensusnode"
)

func newMigrationFixture(t *testing.T) *migrationFixture {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017/?w=majority"))
	require.NoError(t, err)

	db := client.Database("consensus_test")
	logColl := db.Collection("log")
	payloadColl := db.Collection("payloads")
	settingsColl := db.Collection("settings")

	_ = logColl.Drop(ctx)
	_ = payloadColl.Drop(ctx)
	_ = settingsColl.Drop(ctx)

	return &migrationFixture{
		client:       client,
		logColl:      logColl,
		payloadColl:  payloadColl,
		settingsColl: settingsColl,
		cancel:       cancel,
		ctx:          ctx,
	}
}

type migrationFixture struct {
	client       *mongo.Client
	logColl      *mongo.Collection
	payloadColl  *mongo.Collection
	settingsColl *mongo.Collection
	cancel       context.CancelFunc
	ctx          context.Context
	service      *fixture
}

func (mfx *migrationFixture) StartService(t *testing.T) {
	mfx.service = newFixture(t, nil)
}

func (mfx *migrationFixture) Finish(t *testing.T) {
	if mfx.service != nil {
		mfx.service.Finish(t)
	} else {
		_ = mfx.logColl.Drop(mfx.ctx)
		_ = mfx.payloadColl.Drop(mfx.ctx)
		_ = mfx.settingsColl.Drop(mfx.ctx)
		_ = mfx.client.Disconnect(mfx.ctx)
	}
	if mfx.cancel != nil {
		mfx.cancel()
	}
}

func TestMigration_FromScratch(t *testing.T) {
	mfx := newMigrationFixture(t)
	defer mfx.Finish(t)

	legacyLogs := []consensus.Log{
		{
			Id: "log1",
			Records: []consensus.Record{
				{
					Id:      "rec1",
					PrevId:  "",
					Payload: []byte("payload1"),
					Created: time.Now().Truncate(time.Second).UTC(),
				},
				{
					Id:      "rec2",
					PrevId:  "rec1",
					Payload: []byte("payload2"),
					Created: time.Now().Truncate(time.Second).UTC(),
				},
			},
		},
		{
			Id: "log2",
			Records: []consensus.Record{
				{
					Id:      "rec3",
					PrevId:  "",
					Payload: []byte("payload3"),
					Created: time.Now().Truncate(time.Second).UTC(),
				},
			},
		},
	}

	for _, log := range legacyLogs {
		_, err := mfx.logColl.InsertOne(mfx.ctx, log)
		require.NoError(t, err)
	}

	mfx.StartService(t)

	var flag migrationFlag
	svc := mfx.service.Service.(*service)
	err := svc.settingsColl.FindOne(mfx.ctx, bson.M{"_id": migrationFlagKey}).Decode(&flag)
	require.NoError(t, err)
	assert.True(t, flag.Completed)

	fetchedLog1, err := mfx.service.FetchLog(mfx.ctx, "log1")
	require.NoError(t, err)
	require.Len(t, fetchedLog1.Records, 2)
	assert.Equal(t, []byte("payload1"), fetchedLog1.Records[0].Payload)
	assert.Equal(t, []byte("payload2"), fetchedLog1.Records[1].Payload)

	fetchedLog2, err := mfx.service.FetchLog(mfx.ctx, "log2")
	require.NoError(t, err)
	require.Len(t, fetchedLog2.Records, 1)
	assert.Equal(t, []byte("payload3"), fetchedLog2.Records[0].Payload)

	cursor, err := svc.payloadColl.Find(mfx.ctx, bson.M{})
	require.NoError(t, err)
	defer cursor.Close(mfx.ctx)

	var payloads []consensus.Payload
	err = cursor.All(mfx.ctx, &payloads)
	require.NoError(t, err)
	assert.Len(t, payloads, 3)
}

func TestMigration_Idempotent(t *testing.T) {
	mfx := newMigrationFixture(t)
	defer mfx.Finish(t)

	log := consensus.Log{
		Id: "log1",
		Records: []consensus.Record{
			{
				Id:      "rec1",
				PrevId:  "",
				Payload: []byte("payload1"),
				Created: time.Now().Truncate(time.Second).UTC(),
			},
		},
	}

	_, err := mfx.logColl.InsertOne(mfx.ctx, log)
	require.NoError(t, err)

	mfx.StartService(t)
	svc := mfx.service.Service.(*service)

	count1, err := svc.payloadColl.CountDocuments(mfx.ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count1)

	err = svc.RunMigration(mfx.ctx)
	require.NoError(t, err)

	count2, err := svc.payloadColl.CountDocuments(mfx.ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, count1, count2)

	fetchedLog, err := mfx.service.FetchLog(mfx.ctx, "log1")
	require.NoError(t, err)
	require.Len(t, fetchedLog.Records, 1)
	assert.Equal(t, []byte("payload1"), fetchedLog.Records[0].Payload)
}

func TestMigration_PayloadDeduplication(t *testing.T) {
	mfx := newMigrationFixture(t)
	defer mfx.Finish(t)

	duplicatePayload := []byte("duplicate-payload")
	log := consensus.Log{
		Id: "log1",
		Records: []consensus.Record{
			{
				Id:      "rec1",
				PrevId:  "",
				Payload: duplicatePayload,
				Created: time.Now().Truncate(time.Second).UTC(),
			},
			{
				Id:      "rec2",
				PrevId:  "rec1",
				Payload: duplicatePayload,
				Created: time.Now().Truncate(time.Second).UTC(),
			},
			{
				Id:      "rec3",
				PrevId:  "rec2",
				Payload: []byte("unique-payload"),
				Created: time.Now().Truncate(time.Second).UTC(),
			},
		},
	}

	_, err := mfx.logColl.InsertOne(mfx.ctx, log)
	require.NoError(t, err)

	mfx.StartService(t)
	svc := mfx.service.Service.(*service)

	count, err := svc.payloadColl.CountDocuments(mfx.ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	fetchedLog, err := mfx.service.FetchLog(mfx.ctx, "log1")
	require.NoError(t, err)
	require.Len(t, fetchedLog.Records, 3)
	assert.Equal(t, duplicatePayload, fetchedLog.Records[0].Payload)
	assert.Equal(t, duplicatePayload, fetchedLog.Records[1].Payload)
	assert.Equal(t, []byte("unique-payload"), fetchedLog.Records[2].Payload)
}

func TestMigration_RemovesEmbeddedPayloads(t *testing.T) {
	mfx := newMigrationFixture(t)
	defer mfx.Finish(t)

	payload := []byte("test-payload")
	expectedHash := computePayloadHash(payload)

	log := consensus.Log{
		Id: "log1",
		Records: []consensus.Record{
			{
				Id:      "rec1",
				PrevId:  "",
				Payload: payload,
				Created: time.Now().Truncate(time.Second).UTC(),
			},
		},
	}

	_, err := mfx.logColl.InsertOne(mfx.ctx, log)
	require.NoError(t, err)

	mfx.StartService(t)
	svc := mfx.service.Service.(*service)

	var rawLog consensus.Log
	err = svc.logColl.FindOne(mfx.ctx, bson.M{"_id": "log1"}).Decode(&rawLog)
	require.NoError(t, err)
	require.Len(t, rawLog.Records, 1)

	assert.Empty(t, rawLog.Records[0].Payload)
	assert.Equal(t, expectedHash, rawLog.Records[0].PayloadHash)

	var storedPayload consensus.Payload
	err = svc.payloadColl.FindOne(mfx.ctx, bson.M{"_id": expectedHash}).Decode(&storedPayload)
	require.NoError(t, err)
	assert.Equal(t, expectedHash, storedPayload.Hash)
	assert.Equal(t, payload, storedPayload.Data)
}

func TestMigration_EmptyDatabase(t *testing.T) {
	fx := newFixture(t, nil)
	defer fx.Finish(t)

	svc := fx.Service.(*service)

	err := svc.RunMigration(ctx)
	require.NoError(t, err)

	var flag migrationFlag
	err = svc.settingsColl.FindOne(ctx, bson.M{"_id": migrationFlagKey}).Decode(&flag)
	require.NoError(t, err)
	assert.True(t, flag.Completed)
}

func TestAddRecord_AfterMigration(t *testing.T) {
	fx := newFixture(t, nil)
	defer fx.Finish(t)

	log := consensus.Log{
		Id: "log1",
		Records: []consensus.Record{
			{
				Id:      "rec1",
				PrevId:  "",
				Payload: []byte("payload1"),
				Created: time.Now().Truncate(time.Second).UTC(),
			},
		},
	}

	require.NoError(t, fx.AddLog(ctx, log))

	newRecord := consensus.Record{
		Id:      "rec2",
		PrevId:  "rec1",
		Payload: []byte("payload2"),
		Created: time.Now().Truncate(time.Second).UTC(),
	}

	require.NoError(t, fx.AddRecord(ctx, "log1", newRecord))

	fetchedLog, err := fx.FetchLog(ctx, "log1")
	require.NoError(t, err)
	require.Len(t, fetchedLog.Records, 2)
	assert.Equal(t, []byte("payload2"), fetchedLog.Records[0].Payload)
	assert.Equal(t, []byte("payload1"), fetchedLog.Records[1].Payload)

	svc := fx.Service.(*service)
	hash := computePayloadHash([]byte("payload2"))
	var payload consensus.Payload
	err = svc.payloadColl.FindOne(ctx, bson.M{"_id": hash}).Decode(&payload)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload2"), payload.Data)
}

func TestPayloadDeduplication_OnAddRecord(t *testing.T) {
	fx := newFixture(t, nil)
	defer fx.Finish(t)

	log := consensus.Log{
		Id: "log1",
		Records: []consensus.Record{
			{
				Id:     "rec1",
				PrevId: "",
			},
		},
	}
	require.NoError(t, fx.AddLog(ctx, log))

	duplicatePayload := []byte("duplicate-payload")

	rec2 := consensus.Record{
		Id:      "rec2",
		PrevId:  "rec1",
		Payload: duplicatePayload,
		Created: time.Now().Truncate(time.Second).UTC(),
	}
	require.NoError(t, fx.AddRecord(ctx, "log1", rec2))

	rec3 := consensus.Record{
		Id:      "rec3",
		PrevId:  "rec2",
		Payload: duplicatePayload,
		Created: time.Now().Truncate(time.Second).UTC(),
	}
	require.NoError(t, fx.AddRecord(ctx, "log1", rec3))

	svc := fx.Service.(*service)
	hash := computePayloadHash(duplicatePayload)
	count, err := svc.payloadColl.CountDocuments(ctx, bson.M{"_id": hash})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	fetchedLog, err := fx.FetchLog(ctx, "log1")
	require.NoError(t, err)
	assert.Equal(t, duplicatePayload, fetchedLog.Records[0].Payload)
	assert.Equal(t, duplicatePayload, fetchedLog.Records[1].Payload)
}
