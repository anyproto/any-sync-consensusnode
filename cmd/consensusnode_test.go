package main

import (
	"slices"
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-consensusnode/consensusrpc"
	"github.com/anyproto/any-sync-consensusnode/db"
	"github.com/anyproto/any-sync-consensusnode/deletelog"
	"github.com/anyproto/any-sync-consensusnode/stream"
)

// Components run in registration order, and a transport accepts connections as soon as it runs.
// A request that arrived before db.Run connected to mongo found nil collections and crashed the node,
// so the transports run after every service that handles requests.
func TestBootstrap_TransportsRunLast(t *testing.T) {
	a := new(app.App)
	Bootstrap(a)
	names := a.ComponentNames()

	position := func(name string) int {
		i := slices.Index(names, name)
		require.NotEqual(t, -1, i, "%s is not registered", name)
		return i
	}
	for _, transport := range []string{yamux.CName, quic.CName} {
		for _, service := range []string{db.CName, stream.CName, consensusrpc.CName, deletelog.CName} {
			assert.Greater(t, position(transport), position(service), "%s runs before %s", transport, service)
		}
	}
}
