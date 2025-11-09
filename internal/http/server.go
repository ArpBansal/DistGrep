package http

import "sync"

type ServerOpts struct {
	ID   string
	Port int
}

type Server struct {
	opts     ServerOpts
	peerLock sync.Mutex
	peers    map[string]string
}

func NewServer(opts ServerOpts) *Server {
	return &Server{
		opts:  opts,
		peers: make(map[string]string),
	}
}

type RPC struct {
	From    string // net.Addr to_check_1
	Payload []byte
	Stream  bool
}

type Client interface {
	Close() error
	Call(RPC) (RPC, error)
}

type Transport interface {
	Addr() string
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
	Dial(string) (Client, error)
}
