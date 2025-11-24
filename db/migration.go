package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	consensus "github.com/anyproto/any-sync-consensusnode"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	migrationFlagKey = "migration_v2_completed"
)

type migrationFlag struct {
	ID        string `bson:"_id"`
	Completed bool   `bson:"completed"`
	Timestamp time.Time `bson:"timestamp"`
}

// RunMigration executes the one-time migration to content-addressable storage
func (s *service) RunMigration(ctx context.Context) error {
	// Check if migration already completed
	var flag migrationFlag
	err := s.settingsColl.FindOne(ctx, bson.M{"_id": migrationFlagKey}).Decode(&flag)
	if err == nil && flag.Completed {
		log.Info("migration already completed, skipping")
		return nil
	}
	if err != nil && err != mongo.ErrNoDocuments {
		return fmt.Errorf("failed to check migration status: %w", err)
	}

	log.Info("starting migration to content-addressable storage")
	startTime := time.Now()

	// Count total logs for progress reporting
	totalLogs, err := s.logColl.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to count logs: %w", err)
	}

	log.Info("found logs to migrate", zap.Int("count", int(totalLogs)))

	if totalLogs == 0 {
		// No logs to migrate, just set the flag
		return s.setMigrationFlag(ctx)
	}

	// Stream logs one at a time to avoid loading everything into memory
	cursor, err := s.logColl.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to fetch logs for migration: %w", err)
	}
	defer cursor.Close(ctx)

	// Migrate each log
	migratedCount := 0
	payloadCount := 0
	for cursor.Next(ctx) {
		var logDoc consensus.Log
		if err := cursor.Decode(&logDoc); err != nil {
			return fmt.Errorf("failed to decode log: %w", err)
		}

		if err := s.migrateLog(ctx, &logDoc); err != nil {
			return fmt.Errorf("failed to migrate log %s: %w", logDoc.Id, err)
		}

		migratedCount++
		payloadCount += len(logDoc.Records)

		// Log progress every 100 logs
		if migratedCount%100 == 0 || migratedCount == int(totalLogs) {
			log.Info("migration progress",
				zap.Int("migrated", migratedCount),
				zap.Int("total", int(totalLogs)),
				zap.Int("payloads", payloadCount))
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor iteration error: %w", err)
	}

	// Set migration flag
	if err := s.setMigrationFlag(ctx); err != nil {
		return err
	}

	duration := time.Since(startTime)
	log.Info("migration completed successfully",
		zap.Int("logs", migratedCount),
		zap.Int("payloads", payloadCount),
		zap.Duration("duration", duration))

	return nil
}

// migrateLog migrates a single log document
func (s *service) migrateLog(ctx context.Context, logDoc *consensus.Log) error {
	if len(logDoc.Records) == 0 {
		return nil
	}

	// Process each record
	modified := false
	for i := range logDoc.Records {
		record := &logDoc.Records[i]

		// Skip if already migrated (has PayloadHash but no Payload)
		if record.PayloadHash != "" && len(record.Payload) == 0 {
			continue
		}

		// Skip if no payload to migrate
		if len(record.Payload) == 0 {
			continue
		}

		// Compute hash
		hash := computePayloadHash(record.Payload)

		// Store payload in payloads collection (upsert to handle duplicates)
		payload := consensus.Payload{
			Hash: hash,
			Data: record.Payload,
		}

		opts := options.Update().SetUpsert(true)
		_, err := s.payloadColl.UpdateOne(
			ctx,
			bson.M{"_id": hash},
			bson.M{"$setOnInsert": payload},
			opts,
		)
		if err != nil {
			return fmt.Errorf("failed to store payload %s: %w", hash, err)
		}

		// Update record
		record.PayloadHash = hash
		record.Payload = nil
		modified = true
	}

	// Update the log document if any records were modified
	if modified {
		_, err := s.logColl.ReplaceOne(
			ctx,
			bson.M{"_id": logDoc.Id},
			logDoc,
		)
		if err != nil {
			return fmt.Errorf("failed to update log document: %w", err)
		}
	}

	return nil
}

// setMigrationFlag marks the migration as completed
func (s *service) setMigrationFlag(ctx context.Context) error {
	flag := migrationFlag{
		ID:        migrationFlagKey,
		Completed: true,
		Timestamp: time.Now(),
	}

	opts := options.Update().SetUpsert(true)
	_, err := s.settingsColl.UpdateOne(
		ctx,
		bson.M{"_id": migrationFlagKey},
		bson.M{"$set": flag},
		opts,
	)
	if err != nil {
		return fmt.Errorf("failed to set migration flag: %w", err)
	}

	return nil
}

// computePayloadHash computes SHA256 hash of payload data
func computePayloadHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
