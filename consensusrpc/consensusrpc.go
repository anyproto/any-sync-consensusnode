package consensusrpc

import (
	"context"
	consensus "github.com/anyproto/any-sync-consensusnode"
	"github.com/anyproto/any-sync-consensusnode/db"
	"github.com/anyproto/any-sync-consensusnode/stream"
	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/util/cidutil"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"storj.io/drpc/drpcerr"
	"time"
)

const CName = "consensus.consensusrpc"

var log = logger.NewNamed(CName)

func New() app.Component {
	return &consensusRpc{}
}

// consensusRpc implements consensus rpc server
type consensusRpc struct {
	db       db.Service
	stream   stream.Service
	nodeconf nodeconf.Service
	account  accountservice.Service
}

func (c *consensusRpc) Init(a *app.App) (err error) {
	c.db = a.MustComponent(db.CName).(db.Service)
	c.stream = a.MustComponent(stream.CName).(stream.Service)
	c.nodeconf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	c.account = a.MustComponent(accountservice.CName).(accountservice.Service)
	return consensusproto.DRPCRegisterConsensus(a.MustComponent(server.CName).(server.DRPCServer), c)
}

func (c *consensusRpc) Name() (name string) {
	return CName
}

func (c *consensusRpc) LogAdd(ctx context.Context, req *consensusproto.LogAddRequest) (*consensusproto.Ok, error) {
	if err := c.checkClient(ctx); err != nil {
		return nil, err
	}

	if !cidutil.VerifyCid(req.Record.Payload, req.Record.Id) {
		return nil, consensuserr.ErrInvalidPayload
	}

	// we don't sign the first record because it affects the id, but we sign the following records as a confirmation that the chain is valid and the record added from a valid source
	l := consensus.Log{
		Id: req.Record.Id,
		Records: []consensus.Record{
			{
				Id:      req.Record.Id,
				Payload: req.Record.Payload,
				Created: time.Now(),
			},
		},
	}
	if err := c.db.AddLog(ctx, l); err != nil {
		return nil, err
	}
	return &consensusproto.Ok{}, nil
}

func (c *consensusRpc) RecordAdd(ctx context.Context, req *consensusproto.RecordAddRequest) (res *consensusproto.RawRecordWithId, err error) {
	if err = c.checkClient(ctx); err != nil {
		return
	}

	// unmarshal payload as a consensus record
	rec := &consensusproto.Record{}
	if e := rec.Unmarshal(req.Record.Payload); e != nil {
		return nil, consensuserr.ErrInvalidPayload
	}

	// sign a record
	req.Record.AcceptorIdentity = c.account.Account().SignKey.GetPublic().Storage()
	if req.Record.AcceptorSignature, err = c.account.Account().SignKey.Sign(req.Record.Payload); err != nil {
		log.Warn("can't sign payload", zap.Error(err))
		return nil, consensuserr.ErrUnexpected
	}

	// marshal with identity and sign
	payload, err := req.Record.Marshal()
	if err != nil {
		log.Warn("can't marshal payload", zap.Error(err))
		return nil, consensuserr.ErrUnexpected
	}

	// create id
	id, err := cidutil.NewCidFromBytes(payload)
	if err != nil {
		log.Warn("can't make payload cid", zap.Error(err))
		return nil, consensuserr.ErrUnexpected
	}

	// add to db
	if err = c.db.AddRecord(ctx, req.LogId, consensus.Record{
		Id:      id,
		PrevId:  rec.PrevId,
		Payload: payload,
		Created: time.Now(),
	}); err != nil {
		return
	}
	return &consensusproto.RawRecordWithId{
		Payload: payload,
		Id:      id,
	}, nil
}

func (c *consensusRpc) LogWatch(rpcStream consensusproto.DRPCConsensus_LogWatchStream) error {
	if err := c.checkClient(rpcStream.Context()); err != nil {
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

func (c *consensusRpc) checkClient(ctx context.Context) (err error) {
	peerId, err := peer.CtxPeerId(ctx)
	if err != nil {
		return consensuserr.ErrForbidden
	}
	if !slices.Contains(c.nodeconf.NodeTypes(peerId), nodeconf.NodeTypeTree) {
		return consensuserr.ErrForbidden
	}
	return
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
