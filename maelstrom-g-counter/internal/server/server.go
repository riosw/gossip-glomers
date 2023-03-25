package server

import (
	"context"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	Node         *maelstrom.Node
	kv           *maelstrom.KV
	localCounter int
	nodeIDs      []string
	localMutex   *sync.Mutex
}

func New() *Server {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	return &Server{
		Node:         node,
		kv:           kv,
		localCounter: 0,
		localMutex:   &sync.Mutex{},
	}
}

func (s *Server) Add(delta int) error {
	ctx := context.TODO()
	s.localMutex.Lock()
	oldLocalCounter := s.localCounter
	s.localCounter += delta
	err := s.kv.CompareAndSwap(ctx, s.Node.ID(), oldLocalCounter, s.localCounter, true)
	s.localMutex.Unlock()
	if err != nil {
		if err.(*maelstrom.RPCError).Code == maelstrom.PreconditionFailed {
			s.Add(delta)
		}
		return err
	}
	return nil
}

func (s *Server) Read() (int, error) {
	var globalCounter int = 0
	for _, nodeID := range s.nodeIDs {
		val, err := s.kv.ReadInt(context.TODO(), nodeID)
		if err != nil {
			if err.(*maelstrom.RPCError).Code == maelstrom.KeyDoesNotExist {
				val = 0
			} else {
				return 0, err
			}
		}

		globalCounter += val
	}

	return globalCounter, nil
}

func (s *Server) SetNodeIDs(nodeIDs []interface{}) error {
	s.nodeIDs = make([]string, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		s.nodeIDs[i] = nodeID.(string)
	}
	return nil
}
