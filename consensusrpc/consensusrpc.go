package consensusrpc

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/cidutil"
	"go.uber.org/zap"
	"storj.io/drpc/drpcerr"

	consensus "github.com/anyproto/any-sync-consensusnode"
	"github.com/anyproto/any-sync-consensusnode/db"
	"github.com/anyproto/any-sync-consensusnode/stream"
)

const CName = "consensus.consensusrpc"

var log = logger.NewNamed(CName)

func New() app.Component {
	return &consensusRpc{}
}

var okResp = &consensusproto.Ok{}

// consensusRpc implements consensus rpc server
type consensusRpc struct {
	db       db.Service
	stream   stream.Service
	nodeconf nodeconf.Service
	account  accountservice.Service
	metric   metric.Metric
}

func (c *consensusRpc) Init(a *app.App) (err error) {
	c.db = a.MustComponent(db.CName).(db.Service)
	c.stream = a.MustComponent(stream.CName).(stream.Service)
	c.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	c.account = a.MustComponent(accountservice.CName).(accountservice.Service)
	c.metric = a.MustComponent(metric.CName).(metric.Metric)
	return consensusproto.DRPCRegisterConsensus(a.MustComponent(server.CName).(server.DRPCServer), c)
}

func (c *consensusRpc) Name() (name string) {
	return CName
}

func (c *consensusRpc) LogAdd(ctx context.Context, req *consensusproto.LogAddRequest) (resp *consensusproto.Ok, err error) {
	st := time.Now()
	defer func() {
		c.metric.RequestLog(ctx, "consensus.logAdd",
			metric.TotalDur(time.Since(st)),
			zap.String("logId", req.LogId),
			zap.Error(err),
		)
	}()

	if err = c.checkWrite(ctx); err != nil {
		return
	}

	if req.GetRecord() == nil || !cidutil.VerifyCid(req.Record.Payload, req.Record.Id) {
		err = consensuserr.ErrInvalidPayload
		return
	}

	// we don't sign the first record because it affects the id, but we sign the following records as a confirmation that the chain is valid and the record added from a valid source
	l := consensus.Log{
		Id: req.LogId,
		Records: []consensus.Record{
			{
				Id:      req.Record.Id,
				Payload: req.Record.Payload,
				Created: time.Now(),
			},
		},
	}
	if err = c.db.AddLog(ctx, l); err != nil {
		return
	}
	return okResp, nil
}

func (c *consensusRpc) LogDelete(ctx context.Context, req *consensusproto.LogDeleteRequest) (resp *consensusproto.Ok, err error) {
	st := time.Now()
	defer func() {
		c.metric.RequestLog(ctx, "consensus.logDelete",
			metric.TotalDur(time.Since(st)),
			zap.String("logId", req.LogId),
			zap.Error(err),
		)
	}()

	if err = c.checkWrite(ctx); err != nil {
		return
	}

	if err = c.db.DeleteLog(ctx, req.LogId); err != nil {
		return
	}
	return okResp, nil
}

func (c *consensusRpc) RecordAdd(ctx context.Context, req *consensusproto.RecordAddRequest) (resp *consensusproto.RawRecordWithId, err error) {
	st := time.Now()
	defer func() {
		c.metric.RequestLog(ctx, "consensus.recordAdd",
			metric.TotalDur(time.Since(st)),
			zap.String("logId", req.LogId),
			zap.Error(err),
		)
	}()

	if err = c.checkWrite(ctx); err != nil {
		return
	}

	// unmarshal payload as a consensus record
	rec := &consensusproto.Record{}
	if e := rec.UnmarshalVT(req.Record.Payload); e != nil {
		err = consensuserr.ErrInvalidPayload
		return
	}

	// set an accept time
	createdTime := time.Now()
	req.Record.AcceptorTimestamp = createdTime.Unix()

	// sign a record
	req.Record.AcceptorIdentity = c.account.Account().SignKey.GetPublic().Storage()
	if req.Record.AcceptorSignature, err = c.account.Account().SignKey.Sign(req.Record.Payload); err != nil {
		log.Warn("can't sign payload", zap.Error(err))
		err = consensuserr.ErrUnexpected
		return
	}
	// marshal with identity and sign
	payload, err := req.Record.MarshalVT()
	if err != nil {
		log.Warn("can't marshal payload", zap.Error(err))
		err = consensuserr.ErrUnexpected
		return
	}

	// create id
	id, err := cidutil.NewCidFromBytes(payload)
	if err != nil {
		log.Warn("can't make payload cid", zap.Error(err))
		err = consensuserr.ErrUnexpected
		return
	}

	// add to db
	if err = c.db.AddRecord(ctx, req.LogId, consensus.Record{
		Id:      id,
		PrevId:  rec.PrevId,
		Payload: payload,
		Created: createdTime,
	}); err != nil {
		return
	}
	return &consensusproto.RawRecordWithId{
		Payload: payload,
		Id:      id,
	}, nil
}

func (c *consensusRpc) LogWatch(rpcStream consensusproto.DRPCConsensus_LogWatchStream) error {
	if err := c.checkRead(rpcStream.Context()); err != nil {
		return err
	}

	stream := c.stream.NewStream()
	defer stream.Close()
	go c.readStream(stream, rpcStream)
	for {
		recs := stream.WaitLogs()
		if len(recs) == 0 {
			return rpcStream.Close()
		}
		for _, rec := range recs {
			if rec.Err == nil {
				if err := rpcStream.Send(&consensusproto.LogWatchEvent{
					LogId:   rec.Id,
					Records: recordsToProto(rec.Records),
				}); err != nil {
					return err
				}
			} else {
				errCode := consensusproto.ErrCodes(drpcerr.Code(rec.Err))
				if errCode == 0 {
					errCode = consensusproto.ErrCodes(drpcerr.Code(consensuserr.ErrUnexpected))
				}
				if err := rpcStream.Send(&consensusproto.LogWatchEvent{
					LogId: rec.Id,
					Error: &consensusproto.Err{
						Error: errCode,
					},
				}); err != nil {
					return err
				}
			}
		}
	}
}

func (c *consensusRpc) readStream(st *stream.Stream, rpcStream consensusproto.DRPCConsensus_LogWatchStream) {
	defer st.Close()
	for {
		req, err := rpcStream.Recv()
		if err != nil {
			return
		}
		st.UnwatchIds(rpcStream.Context(), req.UnwatchIds)
		st.WatchIds(rpcStream.Context(), req.WatchIds)
	}
}

func (c *consensusRpc) checkRead(ctx context.Context) (err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return consensuserr.ErrForbidden
	}
	nodeTypes := c.nodeconf.NodeTypes(peerId)
	for _, nodeType := range nodeTypes {
		switch nodeType {
		case nodeconf.NodeTypeCoordinator,
			nodeconf.NodeTypeTree,
			nodeconf.NodeTypeFile:
			return nil
		}
	}
	return consensuserr.ErrForbidden
}

func (c *consensusRpc) checkWrite(ctx context.Context) (err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return consensuserr.ErrForbidden
	}
	nodeTypes := c.nodeconf.NodeTypes(peerId)
	for _, nodeType := range nodeTypes {
		switch nodeType {
		case nodeconf.NodeTypeCoordinator,
			nodeconf.NodeTypeTree:
			return nil
		}
	}
	return consensuserr.ErrForbidden
}

func recordsToProto(recs []consensus.Record) []*consensusproto.RawRecordWithId {
	res := make([]*consensusproto.RawRecordWithId, len(recs))
	for i, rec := range recs {
		res[i] = &consensusproto.RawRecordWithId{
			Payload: rec.Payload,
			Id:      rec.Id,
		}
	}
	return res
}
