package server

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	Node *maelstrom.Node
	kv   *maelstrom.KV
}

func New() *Server {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	return &Server{
		Node: node,
		kv:   kv,
	}
}

func (s *Server) Add(delta int) error {
	return nil
}

func (s *Server) Read() (int, error) {
	return 42, nil
}
