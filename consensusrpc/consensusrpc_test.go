package consensusrpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/metric"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/mock_nodeconf"
	"github.com/anyproto/any-sync/testutil/accounttest"
	"github.com/anyproto/any-sync/util/cidutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"storj.io/drpc/drpcconn"

	consensus "github.com/anyproto/any-sync-consensusnode"
	"github.com/anyproto/any-sync-consensusnode/config"
	"github.com/anyproto/any-sync-consensusnode/db"
	"github.com/anyproto/any-sync-consensusnode/db/mock_db"
	"github.com/anyproto/any-sync-consensusnode/stream"
)

var ctx = context.Background()

func testRecord(prevId string) (rId *consensusproto.RawRecordWithId, rawRec *consensusproto.RawRecord) {
	rec := &consensusproto.Record{
		PrevId:    prevId,
		Identity:  []byte("identity"),
		Data:      []byte("data"),
		Timestamp: time.Now().Unix(),
	}

	recPayload, _ := rec.MarshalVT()
	rawRec = &consensusproto.RawRecord{
		Payload: recPayload,
	}
	payload, _ := rawRec.MarshalVT()
	id, _ := cidutil.NewCidFromBytes(payload)
	return &consensusproto.RawRecordWithId{
		Payload: payload,
		Id:      id,
	}, rawRec
}

func TestConsensusRpc_LogAdd(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rec, _ := testRecord("")

		pctx := peer.CtxWithPeerId(ctx, "peerId")

		fx.nodeconf.EXPECT().NodeTypes("peerId").Return([]nodeconf.NodeType{
			nodeconf.NodeTypeCoordinator,
		})

		fx.db.EXPECT().AddLog(pctx, gomock.Any())

		resp, err := fx.LogAdd(pctx, &consensusproto.LogAddRequest{
			LogId:  "testLog.id",
			Record: rec,
		})
		require.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("invalid peer", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rec, _ := testRecord("")

		pctx := peer.CtxWithPeerId(ctx, "peerId")
		fx.nodeconf.EXPECT().NodeTypes("peerId").Return(nil)
		_, err := fx.LogAdd(pctx, &consensusproto.LogAddRequest{
			LogId:  "testLog",
			Record: rec,
		})
		require.EqualError(t, err, consensuserr.ErrForbidden.Error())
	})

	t.Run("invalid payload", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		rec, _ := testRecord("")
		rec.Id += "1"
		pctx := peer.CtxWithPeerId(ctx, "peerId")
		fx.nodeconf.EXPECT().NodeTypes("peerId").Return([]nodeconf.NodeType{
			nodeconf.NodeTypeCoordinator,
		})
		_, err := fx.LogAdd(pctx, &consensusproto.LogAddRequest{
			LogId:  "testLogId",
			Record: rec,
		})
		require.EqualError(t, err, consensuserr.ErrInvalidPayload.Error())
	})
}

func TestConsensusRpc_RecordAdd(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)

		_, rec := testRecord("prevId")
		pctx := peer.CtxWithPeerId(ctx, "peerId")
		fx.nodeconf.EXPECT().NodeTypes("peerId").Return([]nodeconf.NodeType{
			nodeconf.NodeTypeCoordinator,
		})

		fx.db.EXPECT().AddRecord(pctx, "logId", gomock.Any())

		resp, err := fx.RecordAdd(pctx, &consensusproto.RecordAddRequest{
			LogId:  "logId",
			Record: rec,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp)
		assert.NotEmpty(t, resp.Id)
		assert.NotEmpty(t, resp.Payload)

		var result = &consensusproto.RawRecord{}
		require.NoError(t, result.UnmarshalVT(resp.Payload))
		assert.NotEmpty(t, result.AcceptorTimestamp)
	})
	t.Run("invalid peer", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		_, rec := testRecord("")
		_, err := fx.RecordAdd(ctx, &consensusproto.RecordAddRequest{
			Record: rec,
		})
		require.EqualError(t, err, consensuserr.ErrForbidden.Error())
	})
}

func TestConsensusRpc_LogWatch(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	clConn, srConn := net.Pipe()

	go func() {
		pctx := peer.CtxWithPeerId(ctx, "peerId")
		_ = fx.ts.ServeConn(pctx, srConn)
	}()

	fx.nodeconf.EXPECT().NodeTypes("peerId").Return([]nodeconf.NodeType{
		nodeconf.NodeTypeFile,
	})

	cl := consensusproto.NewDRPCConsensusClient(drpcconn.New(clConn))

	clStream, err := cl.LogWatch(ctx)
	require.NoError(t, err)
	defer clStream.Close()

	rec, _ := testRecord("")
	fx.db.EXPECT().FetchLog(gomock.Any(), gomock.Any(), "").DoAndReturn(func(ctx context.Context, logId, afterRecordId string) (consensus.Log, error) {
		if logId == "logId" {
			return consensus.Log{
				Id: "logId",
				Records: []consensus.Record{
					{
						Id:      rec.Id,
						Payload: rec.Payload,
					},
				},
			}, nil
		} else {
			return consensus.Log{}, consensuserr.ErrLogNotFound
		}
	}).AnyTimes()

	require.NoError(t, clStream.Send(&consensusproto.LogWatchRequest{
		WatchIds: []string{"logId"},
	}))

	ev, err := clStream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "logId", ev.LogId)

	go func() {
		fx.dbRecv("logId", []consensus.Record{
			{
				Id:      rec.Id + "1",
				PrevId:  rec.Id,
				Payload: rec.Payload,
			},
			{
				Id:      rec.Id,
				Payload: rec.Payload,
			},
		})
	}()
	ev, err = clStream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "logId", ev.LogId)

	// log not found
	require.NoError(t, clStream.Send(&consensusproto.LogWatchRequest{WatchIds: []string{"logId1"}}))

	ev, err = clStream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "logId1", ev.LogId)
	assert.Equal(t, consensusproto.ErrCodes(502), ev.Error.Error)
}

func TestConsensusRpc_LogDelete(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish(t)

	var logId = "logId"

	pctx := peer.CtxWithPeerId(ctx, "peerId")
	fx.nodeconf.EXPECT().NodeTypes("peerId").Return([]nodeconf.NodeType{
		nodeconf.NodeTypeCoordinator,
	})
	fx.db.EXPECT().DeleteLog(pctx, logId)
	resp, err := fx.LogDelete(pctx, &consensusproto.LogDeleteRequest{LogId: logId})
	assert.NotNil(t, resp)
	assert.NoError(t, err)
}

type fixture struct {
	a        *app.App
	ctrl     *gomock.Controller
	ts       *rpctest.TestServer
	db       *mock_db.MockService
	nodeconf *mock_nodeconf.MockService
	stream   stream.Service
	dbRecv   db.ChangeReceiver
	*consensusRpc
}

func newFixture(t *testing.T) *fixture {
	fx := &fixture{
		a:            new(app.App),
		ctrl:         gomock.NewController(t),
		ts:           rpctest.NewTestServer(),
		consensusRpc: New().(*consensusRpc),
	}
	fx.db = mock_db.NewMockService(fx.ctrl)
	fx.db.EXPECT().Name().Return(db.CName).AnyTimes()
	fx.db.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.db.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.db.EXPECT().Close(gomock.Any()).AnyTimes()

	fx.nodeconf = mock_nodeconf.NewMockService(fx.ctrl)
	fx.nodeconf.EXPECT().Name().Return(nodeconf.CName).AnyTimes()
	fx.nodeconf.EXPECT().Init(gomock.Any()).AnyTimes()
	fx.nodeconf.EXPECT().Run(gomock.Any()).AnyTimes()
	fx.nodeconf.EXPECT().Close(gomock.Any()).AnyTimes()

	fx.stream = stream.New()

	fx.a.Register(fx.nodeconf).
		Register(fx.db).
		Register(fx.ts).
		Register(fx.stream).
		Register(&accounttest.AccountTestService{}).
		Register(metric.New()).
		Register(new(config.Config)).
		Register(fx.consensusRpc)

	recvCh := make(chan db.ChangeReceiver, 1)
	fx.db.EXPECT().SetChangeReceiver(gomock.Any()).Do(func(recv db.ChangeReceiver) {
		recvCh <- recv
	})

	require.NoError(t, fx.a.Start(ctx))
	fx.dbRecv = <-recvCh
	return fx
}

func (fx *fixture) finish(t *testing.T) {
	assert.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
}
