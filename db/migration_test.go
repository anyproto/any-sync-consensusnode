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

// newMigrationFixture creates a fixture with direct MongoDB access before migration runs
func newMigrationFixture(t *testing.T) *migrationFixture {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	// Connect to MongoDB directly
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017/?w=majority"))
	require.NoError(t, err)

	db := client.Database("consensus_test")
	logColl := db.Collection("log")
	payloadColl := db.Collection("payloads")
	settingsColl := db.Collection("settings")

	// Clean up collections
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
	// Now start the regular service which will run migration
	mfx.service = newFixture(t, nil)
}

func (mfx *migrationFixture) Finish(t *testing.T) {
	if mfx.service != nil {
		mfx.service.Finish(t)
	} else {
		// Clean up if service never started
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

	// Create logs with embedded payloads (legacy format)
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

	// Insert legacy logs directly to MongoDB BEFORE service starts
	for _, log := range legacyLogs {
		_, err := mfx.logColl.InsertOne(mfx.ctx, log)
		require.NoError(t, err)
	}

	// Now start the service - this will run the migration
	mfx.StartService(t)

	// Verify migration flag is set
	var flag migrationFlag
	svc := mfx.service.Service.(*service)
	err := svc.settingsColl.FindOne(mfx.ctx, bson.M{"_id": migrationFlagKey}).Decode(&flag)
	require.NoError(t, err)
	assert.True(t, flag.Completed)

	// Fetch logs and verify payloads are hydrated correctly
	fetchedLog1, err := mfx.service.FetchLog(mfx.ctx, "log1")
	require.NoError(t, err)
	require.Len(t, fetchedLog1.Records, 2)
	assert.Equal(t, []byte("payload1"), fetchedLog1.Records[0].Payload)
	assert.Equal(t, []byte("payload2"), fetchedLog1.Records[1].Payload)

	fetchedLog2, err := mfx.service.FetchLog(mfx.ctx, "log2")
	require.NoError(t, err)
	require.Len(t, fetchedLog2.Records, 1)
	assert.Equal(t, []byte("payload3"), fetchedLog2.Records[0].Payload)

	// Verify payloads are stored separately in payloads collection
	cursor, err := svc.payloadColl.Find(mfx.ctx, bson.M{})
	require.NoError(t, err)
	defer cursor.Close(mfx.ctx)

	var payloads []consensus.Payload
	err = cursor.All(mfx.ctx, &payloads)
	require.NoError(t, err)
	assert.Len(t, payloads, 3) // 3 unique payloads
}

func TestMigration_Idempotent(t *testing.T) {
	mfx := newMigrationFixture(t)
	defer mfx.Finish(t)

	// Create a log with embedded payload
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

	// Start service - runs migration first time
	mfx.StartService(t)
	svc := mfx.service.Service.(*service)

	// Count payloads after first migration
	count1, err := svc.payloadColl.CountDocuments(mfx.ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count1)

	// Run migration second time
	err = svc.RunMigration(mfx.ctx)
	require.NoError(t, err)

	// Count payloads after second migration - should be the same
	count2, err := svc.payloadColl.CountDocuments(mfx.ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, count1, count2)

	// Verify data is still correct
	fetchedLog, err := mfx.service.FetchLog(mfx.ctx, "log1")
	require.NoError(t, err)
	require.Len(t, fetchedLog.Records, 1)
	assert.Equal(t, []byte("payload1"), fetchedLog.Records[0].Payload)
}

func TestMigration_PayloadDeduplication(t *testing.T) {
	mfx := newMigrationFixture(t)
	defer mfx.Finish(t)

	// Create logs with duplicate payloads
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

	// Start service - runs migration
	mfx.StartService(t)
	svc := mfx.service.Service.(*service)

	// Verify only 2 payloads stored (duplicate only stored once)
	count, err := svc.payloadColl.CountDocuments(mfx.ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Verify all records still have correct payloads
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

	// Create a log with embedded payload
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

	// Start service - runs migration
	mfx.StartService(t)
	svc := mfx.service.Service.(*service)

	// Fetch raw document from MongoDB to verify Payload field is empty
	var rawLog consensus.Log
	err = svc.logColl.FindOne(mfx.ctx, bson.M{"_id": "log1"}).Decode(&rawLog)
	require.NoError(t, err)
	require.Len(t, rawLog.Records, 1)

	// Verify embedded payload is removed and hash is set
	assert.Empty(t, rawLog.Records[0].Payload, "embedded payload should be removed")
	assert.NotEmpty(t, rawLog.Records[0].PayloadHash, "PayloadHash should be set")
}

func TestMigration_SetsHashes(t *testing.T) {
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

	// Start service - runs migration
	mfx.StartService(t)
	svc := mfx.service.Service.(*service)

	// Fetch raw document
	var rawLog consensus.Log
	err = svc.logColl.FindOne(mfx.ctx, bson.M{"_id": "log1"}).Decode(&rawLog)
	require.NoError(t, err)
	require.Len(t, rawLog.Records, 1)

	// Verify hash is correctly computed
	assert.Equal(t, expectedHash, rawLog.Records[0].PayloadHash)

	// Verify payload is stored with correct hash as _id
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

	// Run migration on empty database
	err := svc.RunMigration(ctx)
	require.NoError(t, err)

	// Verify migration flag is set
	var flag migrationFlag
	err = svc.settingsColl.FindOne(ctx, bson.M{"_id": migrationFlagKey}).Decode(&flag)
	require.NoError(t, err)
	assert.True(t, flag.Completed)
}

func TestMigration_MultipleLogsProgress(t *testing.T) {
	mfx := newMigrationFixture(t)
	defer mfx.Finish(t)

	// Create many logs to test progress logging
	numLogs := 50

	for i := 0; i < numLogs; i++ {
		log := consensus.Log{
			Id: string(rune('a'+i%26)) + string(rune('0'+i/26)),
			Records: []consensus.Record{
				{
					Id:      "rec" + string(rune('0'+i)),
					PrevId:  "",
					Payload: []byte("payload" + string(rune('0'+i))),
					Created: time.Now().Truncate(time.Second).UTC(),
				},
			},
		}
		_, err := mfx.logColl.InsertOne(mfx.ctx, log)
		require.NoError(t, err)
	}

	// Start service - runs migration
	mfx.StartService(t)
	svc := mfx.service.Service.(*service)

	// Verify all logs migrated
	count, err := svc.logColl.CountDocuments(mfx.ctx, bson.M{})
	require.NoError(t, err)
	assert.Equal(t, int64(numLogs), count)

	// Verify migration flag set
	var flag migrationFlag
	err = svc.settingsColl.FindOne(mfx.ctx, bson.M{"_id": migrationFlagKey}).Decode(&flag)
	require.NoError(t, err)
	assert.True(t, flag.Completed)
}

func TestAddRecord_AfterMigration(t *testing.T) {
	fx := newFixture(t, nil)
	defer fx.Finish(t)

	// Create initial log
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

	// Add new record using AddRecord (should use new format)
	newRecord := consensus.Record{
		Id:      "rec2",
		PrevId:  "rec1",
		Payload: []byte("payload2"),
		Created: time.Now().Truncate(time.Second).UTC(),
	}

	require.NoError(t, fx.AddRecord(ctx, "log1", newRecord))

	// Fetch and verify
	fetchedLog, err := fx.FetchLog(ctx, "log1")
	require.NoError(t, err)
	require.Len(t, fetchedLog.Records, 2)
	assert.Equal(t, []byte("payload2"), fetchedLog.Records[0].Payload) // Records are prepended
	assert.Equal(t, []byte("payload1"), fetchedLog.Records[1].Payload)

	// Verify payload stored in payloads collection
	svc := fx.Service.(*service)
	hash := computePayloadHash([]byte("payload2"))
	var payload consensus.Payload
	err = svc.payloadColl.FindOne(ctx, bson.M{"_id": hash}).Decode(&payload)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload2"), payload.Data)
}

func TestFetchLog_AfterMigration(t *testing.T) {
	fx := newFixture(t, nil)
	defer fx.Finish(t)

	// Create log and verify hydration works
	log := consensus.Log{
		Id: "log1",
		Records: []consensus.Record{
			{
				Id:      "rec1",
				PrevId:  "",
				Payload: []byte("test-payload"),
				Created: time.Now().Truncate(time.Second).UTC(),
			},
		},
	}

	require.NoError(t, fx.AddLog(ctx, log))

	// Fetch should hydrate payloads
	fetched, err := fx.FetchLog(ctx, "log1")
	require.NoError(t, err)
	require.Len(t, fetched.Records, 1)
	assert.Equal(t, []byte("test-payload"), fetched.Records[0].Payload)
	assert.NotEmpty(t, fetched.Records[0].PayloadHash)
}

func TestPayloadDeduplication_OnAddRecord(t *testing.T) {
	fx := newFixture(t, nil)
	defer fx.Finish(t)

	// Create initial log
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

	// Add two records with same payload
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

	// Verify only one payload stored
	svc := fx.Service.(*service)
	hash := computePayloadHash(duplicatePayload)
	count, err := svc.payloadColl.CountDocuments(ctx, bson.M{"_id": hash})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Verify both records have correct payload when fetched
	fetchedLog, err := fx.FetchLog(ctx, "log1")
	require.NoError(t, err)
	assert.Equal(t, duplicatePayload, fetchedLog.Records[0].Payload)
	assert.Equal(t, duplicatePayload, fetchedLog.Records[1].Payload)
}
