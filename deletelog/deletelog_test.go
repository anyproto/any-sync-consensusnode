package deletelog

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient/mock_coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/testutil/anymock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/anyproto/any-sync-consensusnode/db"
	"github.com/anyproto/any-sync-consensusnode/db/mock_db"
)

var ctx = context.Background()

func TestDeleteLog_checkLog(t *testing.T) {
	t.Run("no records", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		fx.db.EXPECT().GetDeletionId(ctx).Return("42", nil)

		fx.coord.EXPECT().DeletionLog(ctx, "42", recordsLimit).Return(nil, nil)
		require.NoError(t, fx.checkLog(ctx))
	})
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish(t)
		now := time.Now().Unix()
		fx.db.EXPECT().GetDeletionId(ctx).Return("1", nil)

		fx.coord.EXPECT().DeletionLog(ctx, "1", recordsLimit).Return([]*coordinatorproto.DeletionLogRecord{
			{
				Id:        "1",
				SpaceId:   "s1",
				Status:    coordinatorproto.DeletionLogRecordStatus_Ok,
				Timestamp: now,
				FileGroup: "f1",
			},
			{
				Id:        "2",
				SpaceId:   "s2",
				Status:    coordinatorproto.DeletionLogRecordStatus_Remove,
				Timestamp: now,
				FileGroup: "f2",
			},
		}, nil)
		fx.db.EXPECT().DeleteLog(ctx, "s2").Return(nil)
		fx.db.EXPECT().SetDeletionId(ctx, "1").Return(nil)
		fx.db.EXPECT().SetDeletionId(ctx, "2").Return(nil)
		require.NoError(t, fx.checkLog(ctx))
	})

}

func newFixture(t *testing.T) *fixture {
	ctrl := gomock.NewController(t)
	fx := &fixture{
		ctrl:      ctrl,
		a:         new(app.App),
		coord:     mock_coordinatorclient.NewMockCoordinatorClient(ctrl),
		db:        mock_db.NewMockService(ctrl),
		deleteLog: New().(*deleteLog),
	}

	anymock.ExpectComp(fx.coord.EXPECT(), coordinatorclient.CName)
	anymock.ExpectComp(fx.db.EXPECT(), db.CName)

	fx.a.Register(fx.coord).Register(fx.db).Register(fx.deleteLog).Register(&testConfig{})
	require.NoError(t, fx.a.Start(ctx))
	return fx
}

type fixture struct {
	ctrl  *gomock.Controller
	a     *app.App
	coord *mock_coordinatorclient.MockCoordinatorClient
	db    *mock_db.MockService
	*deleteLog
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.a.Close(ctx))
	fx.ctrl.Finish()
}

type testConfig struct{}

func (c *testConfig) Init(a *app.App) error { return nil }

func (c *testConfig) Name() string { return "config" }

func (c *testConfig) GetDeletion() Config { return Config{} }
