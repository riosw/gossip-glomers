package server

import (
	"errors"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	Node         *maelstrom.Node
	kv           *maelstrom.KV
	localCounter int
	// nodeIDs      []string
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

func (s *Server) Send(key string, msg int) (offset int, err error) {
	return -1, errors.New("not implemented")
}

func (s *Server) Poll(offsets map[string]int) (msgs map[string]([][2]int), err error) {
	return msgs, errors.New("not implemented")
}

func (s *Server) CommitOffsets(offsets map[string]int) (err error) {
	return errors.New("not implemented")
}

func (s *Server) ListCommittedOffsets(keys map[string]string) (offsets map[string]int, err error) {
	return offsets, errors.New("not implemented")
}
