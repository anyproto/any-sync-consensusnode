package deletelog

import (
	"context"
	"errors"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/periodicsync"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-consensusnode/db"
)

const CName = "consensus.deletionLog"

const recordsLimit = 1000

var log = logger.NewNamed(CName)

func New() app.ComponentRunnable {
	return new(deleteLog)
}

type deleteLog struct {
	coordinatorClient coordinatorclient.CoordinatorClient
	ticker            periodicsync.PeriodicSync
	db                db.Service
	enable            bool
}

func (d *deleteLog) Init(a *app.App) (err error) {
	d.coordinatorClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	d.enable = a.MustComponent("config").(configGetter).GetDeletion().Enable
	d.db = a.MustComponent(db.CName).(db.Service)
	return
}

func (d *deleteLog) Name() (name string) {
	return CName
}

func (d *deleteLog) Run(ctx context.Context) (err error) {
	if d.enable {
		d.ticker = periodicsync.NewPeriodicSync(30, time.Hour, d.checkLog, log)
		d.ticker.Run()
	}
	return
}

func (d *deleteLog) checkLog(ctx context.Context) (err error) {
	st := time.Now()
	lastId, err := d.db.GetDeletionId(ctx)
	if err != nil {
		return
	}
	recs, err := d.coordinatorClient.DeletionLog(ctx, lastId, recordsLimit)
	if err != nil {
		return
	}
	var handledCount, deletedCount int
	for _, rec := range recs {
		if rec.Status == coordinatorproto.DeletionLogRecordStatus_Remove {
			if err = d.db.DeleteLog(ctx, rec.SpaceId); err != nil {
				if errors.Is(err, consensuserr.ErrLogNotFound) {
					continue
				}
				return err
			}
			handledCount++
		}
		if err = d.db.SetDeletionId(ctx, rec.Id); err != nil {
			return err
		}
	}
	log.Info("processing deletion log",
		zap.Int("records", len(recs)),
		zap.Int("handled", handledCount),
		zap.Int("deleted", deletedCount),
		zap.Duration("dur", time.Since(st)),
	)
	return
}

func (d *deleteLog) Close(ctx context.Context) (err error) {
	if d.ticker != nil {
		d.ticker.Close()
	}
	return
}
