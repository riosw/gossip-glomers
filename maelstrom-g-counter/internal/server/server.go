package server

import (
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	Node         *maelstrom.Node
	kv           *maelstrom.KV
	localCounter int
}

func New() *Server {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	return &Server{
		Node:         node,
		kv:           kv,
		localCounter: 0,
	}
}

func (s *Server) Add(delta int) error {
	ctx := context.TODO()
	oldLocalCounter := s.localCounter
	s.localCounter += delta
	err := s.kv.CompareAndSwap(ctx, s.Node.ID(), oldLocalCounter, s.localCounter, true)
	if err != nil {
		if err.(*maelstrom.RPCError).Code == maelstrom.PreconditionFailed {
			s.Add(delta)
		}
		return err
	}
	return nil
}

func (s *Server) Read() (int, error) {
	globalCounter, err := s.kv.ReadInt(context.TODO(), s.Node.ID())
	if err != nil {
		return 0, err
	}
	return globalCounter, nil
}
