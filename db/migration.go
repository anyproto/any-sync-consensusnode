package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	consensus "github.com/anyproto/any-sync-consensusnode"
)

const (
	// Version2 - payloads moved to the separate collection
	Version2 = 2
)

func (s *service) runMigrations(ctx context.Context) (err error) {
	return s.tx(ctx, func(txCtx mongo.SessionContext) error {
		version, err := s.getDbVersion(txCtx)
		if err != nil {
			return fmt.Errorf("failed to get db version: %w", err)
		}
		if version < Version2 {
			st := time.Now()
			log.Info("migrate", zap.Int("version", version))
			err = s.migrateV2(txCtx)
			if err != nil {
				log.Warn("migration failed", zap.Duration("dur", time.Since(st)), zap.Error(err))
			} else {
				log.Info("migration done", zap.Duration("dur", time.Since(st)))
			}
		}
		if err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
		if err = s.setDbVersion(txCtx, Version2); err != nil {
			return fmt.Errorf("failed to set db version: %w", err)
		}
		return nil
	})
}

func (s *service) migrateV2(ctx context.Context) (err error) {
	cur, err := s.logColl.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to find logs: %w", err)
	}
	defer func() {
		_ = cur.Close(ctx)
	}()
	for cur.Next(ctx) {
		var l consensus.Log
		if err := cur.Decode(&l); err != nil {
			return fmt.Errorf("failed to decode log: %w", err)
		}
		for i, rec := range l.Records {
			if err = s.savePayload(ctx, consensus.Payload{Id: rec.Id, LogId: l.Id, Payload: rec.Payload}); err != nil {
				return fmt.Errorf("failed to save payload for record %s: %w", rec.Id, err)
			}
			l.Records[i].Payload = nil
		}
		_, err = s.logColl.UpdateOne(ctx, bson.D{{"_id", l.Id}}, bson.D{{"$set", bson.D{{"records", l.Records}}}})
		if err != nil {
			return fmt.Errorf("failed to update log %s: %w", l.Id, err)
		}
	}
	return
}

func (s *service) getDbVersion(ctx context.Context) (version int, err error) {
	type versionDoc struct {
		Id      string `bson:"_id"`
		Version int    `bson:"version"`
	}
	var ver versionDoc
	err = s.settingsColl.FindOne(ctx, bson.D{{"_id", "version"}}).Decode(&ver)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return 0, nil
		}
		return 0, err
	}
	return ver.Version, nil
}

func (s *service) setDbVersion(ctx context.Context, version int) (err error) {
	_, err = s.settingsColl.UpdateOne(ctx, bson.D{{"_id", "version"}}, bson.D{{"$set", bson.D{{"version", version}}}}, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("failed to set db version: %w", err)
	}
	return
}
