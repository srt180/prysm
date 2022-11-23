package track

import (
	"net"
	"os"
	"time"

	eth "github.com/prysmaticlabs/prysm/v3/proto/prysm/v1alpha1"
	communicate "github.com/prysmaticlabs/prysm/v3/track/proto"

	"google.golang.org/grpc"
)

type Type uint8

const (
	Unknown Type = iota
	TransactionBroadcast
	BlockBroadcast
	MainnetPeer
	BlockHeadersResponse
	Connection
	BeaconBlockBroadcast
	AggregateAndProof
	BeaconPeer
)

func (t Type) String() string {
	switch t {
	case TransactionBroadcast:
		return "TransactionBroadcast"
	case BlockBroadcast:
		return "BlockBroadcast"
	case MainnetPeer:
		return "MainnetPeer"
	case BlockHeadersResponse:
		return "BlockHeadersResponse"
	case Connection:
		return "Connection"
	case BeaconBlockBroadcast:
		return "BeaconBlockBroadcast"
	case AggregateAndProof:
		return "AggregateAndProof"
	case BeaconPeer:
		return "BeaconPeer"
	default:
		return "Unknown"
	}
}

type (
	BeaconBlock struct {
		Slot      uint64
		Graffiti  []byte
		BlockHash []byte
		Proposer  uint64
		FromPeer  string
	}
	Aggregate struct {
		AggregatorIndex uint64
		BeaconBlockRoot []byte
		Slot            uint64
		CommitteeIndex  uint64
		Source          *eth.Checkpoint
		Target          *eth.Checkpoint
		FromPeer        string
	}
	//	MainnetPeerMessage struct {
	//		Timestamp int64    `json:"timestamp" bson:"-"`
	//		Type      uint8    `json:"type" bson:"-"`
	//		Pubkey    string   `json:"pubkey" bson:"pubkey"`
	//		Info      PeerInfo `json:"info" bson:",inline"`
	//		IP        string   `json:"IP" bson:"ip"`
	//		TCP       int      `json:"TCP" bson:"tcp"`
	//		UDP       int      `json:"UDP" bson:"udp"`
	//	}

	BeaconPeerMessage struct {
		Type      uint8
		MultiAddr string
		Pubkey    string
		Direction uint8
	}
)

var (
	t *tracker
)

type tracker struct {
	ch chan *message // emit channel
}

type message struct {
	topic     Type
	timestamp int64 // unix timestamp, milliseconds
	data      interface{}
}

func EmitTrack(topic Type, ts int64, data interface{}) {
	if t == nil {
		return
	}
	msg := message{topic, ts, data}
	t.ch <- &msg
}

func ServerInit() {
	const sockUrl = "/opt/geth/beacon_track.sock"

	server := Server{
		subs: make(map[string]*clientSubContext),
	}

	t = &tracker{
		ch: make(chan *message, 1),
	}
	go func() {
		ticker := time.NewTicker(time.Minute)

		for {
			select {
			case message := <-t.ch:
				server.m.Lock()
				for _, sub := range server.subs {
					select {
					case sub.msgCh <- *message:
					case <-sub.ctx.Done():
						continue
					}
				}
				server.m.Unlock()
			case <-ticker.C:
				server.m.Lock()
				for sub, ctx := range server.subs {
					log.Info("[TrackServer] subscriber", "sub", sub, "chSize", len(ctx.msgCh))
				}
				server.m.Unlock()

			}
		}
	}()

	if err := os.Remove(sockUrl); err != nil {
		log.Error("clean socket", "err", err)
	}

	lis, err := net.Listen("unix", sockUrl)
	if err != nil {
		log.Error("failed to listen", "err", err)
		panic(err)
	}
	var opts []grpc.ServerOption
	//if *tls {
	//	if *certFile == "" {
	//		*certFile = data.Path("x509/server_cert.pem")
	//	}
	//	if *keyFile == "" {
	//		*keyFile = data.Path("x509/server_key.pem")
	//	}
	//	creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
	//	if err != nil {
	//		log.Fatalf("Failed to generate credentials %v", err)
	//	}
	//	opts = []grpc.ServerOption{grpc.Creds(creds)}
	//}
	grpcServer := grpc.NewServer(opts...)
	communicate.RegisterTrackPuberServer(grpcServer, &server)
	go grpcServer.Serve(lis)
}
