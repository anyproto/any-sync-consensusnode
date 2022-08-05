package dialer

import (
	"context"
	"errors"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/app"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/app/logger"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/config"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/service/net/peer"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/service/net/rpc"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/service/net/secure"
	"github.com/libp2p/go-libp2p-core/sec"
	"go.uber.org/zap"
	"net"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"sync"
)

const CName = "net/dialer"

var ErrArrdsNotFound = errors.New("addrs for peer not found")

var log = logger.NewNamed(CName)

func New() Dialer {
	return &dialer{}
}

type Dialer interface {
	Dial(ctx context.Context, peerId string) (peer peer.Peer, err error)
	UpdateAddrs(addrs map[string][]string)
	app.Component
}

type dialer struct {
	transport secure.Service
	peerAddrs map[string][]string

	mu sync.RWMutex
}

func (d *dialer) Init(ctx context.Context, a *app.App) (err error) {
	d.transport = a.MustComponent(secure.CName).(secure.Service)
	peerConf := a.MustComponent(config.CName).(*config.Config).PeerList.Remote
	d.peerAddrs = map[string][]string{}
	for _, rp := range peerConf {
		d.peerAddrs[rp.PeerId] = []string{rp.Addr}
	}
	return
}

func (d *dialer) Name() (name string) {
	return CName
}

func (d *dialer) UpdateAddrs(addrs map[string][]string) {
	d.mu.Lock()
	d.peerAddrs = addrs
	d.mu.Unlock()
}

func (d *dialer) Dial(ctx context.Context, peerId string) (peer peer.Peer, err error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	addrs, ok := d.peerAddrs[peerId]
	if !ok || len(addrs) == 0 {
		return nil, ErrArrdsNotFound
	}
	var (
		stream drpc.Stream
		sc     sec.SecureConn
	)
	for _, addr := range addrs {
		stream, sc, err = d.makeStream(ctx, addr)
		if err != nil {
			log.Info("can't connect to host", zap.String("addr", addr))
		} else {
			err = nil
			break
		}
	}
	if err != nil {
		return
	}
	return rpc.PeerFromStream(sc, stream, false), nil
}

func (d *dialer) makeStream(ctx context.Context, addr string) (stream drpc.Stream, sc sec.SecureConn, err error) {
	tcpConn, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	sc, err = d.transport.TLSConn(ctx, tcpConn)
	if err != nil {
		return
	}
	log.Info("connected with remote host", zap.String("serverPeer", sc.RemotePeer().String()), zap.String("per", sc.LocalPeer().String()))
	stream, err = drpcconn.New(sc).NewStream(ctx, "", rpc.Encoding)
	if err != nil {
		return
	}
	return stream, sc, err
}