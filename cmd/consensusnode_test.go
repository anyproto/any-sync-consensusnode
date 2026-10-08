package main

import (
	"slices"
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/net/peerservice"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-consensusnode/db"
)

// Components run in registration order. A request that reached the db before db.Run connected to mongo
// found nil collections and crashed the node, so no connection may exist before the db runs.
func TestBootstrap_Order(t *testing.T) {
	a := new(app.App)
	Bootstrap(a)
	names := a.ComponentNames()
	position := func(name string) int {
		i := slices.Index(names, name)
		require.NotEqual(t, -1, i, "%s is not registered", name)
		return i
	}

	t.Run("the transports run last", func(t *testing.T) {
		// they accept connections as soon as they run
		require.GreaterOrEqual(t, len(names), 2)
		assert.ElementsMatch(t, []string{yamux.CName, quic.CName}, names[len(names)-2:])
	})
	t.Run("the db runs before anything that dials", func(t *testing.T) {
		// an outgoing connection accepts streams too, and nodeconf dials the coordinator as soon as it runs
		for _, dialer := range []string{nodeconf.CName, nodeconf.CNameSource, coordinatorclient.CName, pool.CName, peerservice.CName} {
			assert.Less(t, position(db.CName), position(dialer), "%s runs before %s", dialer, db.CName)
		}
	})
}
