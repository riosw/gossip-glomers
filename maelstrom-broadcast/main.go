package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	aq "github.com/emirpasic/gods/queues/arrayqueue"
	"github.com/emirpasic/gods/sets/hashset"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var retryTimeout = 250 * time.Millisecond
var firstTryTimeout = 500 * time.Millisecond

type Node struct {
	server    *maelstrom.Node
	neighbors []string
	mapRQ     map[string]*RetryQueue
	state     *State
}

type State struct {
	set  *hashset.Set
	rwmu *sync.RWMutex
}

type RetryQueue struct {
	nodeID    string
	queue     *aq.Queue
	mu        *sync.Mutex
	server    *maelstrom.Node
	ackedMsgs *AckedMsgs
	// having ackedMsgs here is structurally improper
	// as this is used outside RetryQueue, but have to do for now
}

// This lists all seen messages that has been acked by a certain neighboring node
type AckedMsgs struct {
	mu  *sync.Mutex
	set map[int]struct{}
}

// Returns whether the queue was empty before enqueueing data
func (rq *RetryQueue) AddRetry(msgBody interface{}) {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	isEmpty := rq.queue.Empty()

	rq.queue.Enqueue(msgBody)

	if isEmpty {
		go rq.RunRetries()
	}
}

// Run Retry sending message to node rq.nodeID.
//
// Called by AddRetry when the queue starts as empty
func (rq *RetryQueue) RunRetries() {
	for {
		rq.mu.Lock()
		v, ok := rq.queue.Dequeue()
		rq.mu.Unlock()

		if !ok {
			fmt.Fprintf(os.Stderr, "RetryQueue is empty, stopping retry loop\n")
			break
		}

		if err := rq.SyncRPCWithRetries(v); err != nil {
			panic(err)
		}
	}
}

// Retries infinitely, assumes eventually message will get through
func (rq *RetryQueue) SyncRPCWithRetries(msgBody interface{}) error {
	t := time.Now()
	defer func() {
		fmt.Fprintf(os.Stderr, "Sending msg %v to %s took %s\n", msgBody, rq.nodeID, time.Now().Sub(t))
	}()

	var message int = extractMessageFromBody(msgBody.(map[string]any))

	for {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, retryTimeout)

		defer cancel()

		resp, err := rq.server.SyncRPC(ctx, rq.nodeID, msgBody)
		// Keep retrying until successful
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return err
		}
		var body map[string]any

		if err := json.Unmarshal(resp.Body, &body); err != nil {
			return fmt.Errorf("Unmarshal returns err: %s\n", err)
		}

		if val, ok := body["type"]; ok {
			if val == "broadcast_ok" {
				rq.addAcked(message)
				break
			} else {
				return fmt.Errorf("WARN: Unexpected response type, got: %s\n", val)
			}
		} else {
			return fmt.Errorf("`type` not found in response body\n")
		}
	}
	return nil
}

// Check if msg has been acked by rq.nodeID
//
// Returns true if was Acked, false if yet to be acked
func (rq *RetryQueue) isAcked(msg int) bool {
	rq.ackedMsgs.mu.Lock()
	defer rq.ackedMsgs.mu.Unlock()
	_, ok := rq.ackedMsgs.set[msg]
	return ok
}

func (rq *RetryQueue) addAcked(msg int) {
	rq.ackedMsgs.mu.Lock()
	rq.ackedMsgs.set[msg] = struct{}{}
	defer rq.ackedMsgs.mu.Unlock()
}

func main() {
	n := Node{
		server:    maelstrom.NewNode(),
		neighbors: []string{},
		state: &State{
			set:  hashset.New(),
			rwmu: &sync.RWMutex{},
		},
		mapRQ: make(map[string]*RetryQueue),
	}

	// TODO: add something like this
	// accumulated_time_to_check_state
	// Everytime isInState called, sum them
	// defer so that on application exit, logs the time lost for deduplication

	n.server.Handle("broadcast", n.broadcastHandler)
	n.server.Handle("read", n.readHandler)
	n.server.Handle("topology", n.topologyHandler)

	if err := n.server.Run(); err != nil {
		log.Fatal(err)
	}
}

func (n *Node) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	var message int = extractMessageFromBody(body)

	if ok := n.state.addIfNotExist(message); !ok {
		fmt.Fprintf(os.Stderr, "Message %d already exist inside state\n", message)
	}

	susceptible := make([]string, len(n.neighbors))
	copy(susceptible, n.neighbors)

	susceptible = removeElement(susceptible, msg.Src)

	go func() {
		for _, dest := range susceptible {
			go func(n *Node, dest string, msgBody map[string]any) {
				if ok := n.mapRQ[dest].isAcked(message); ok {
					// Skip if this message has been acked previously
					return
				}
				ctx := context.Background()
				ctx, cancel := context.WithTimeout(ctx, firstTryTimeout)

				defer cancel()
				// The point of this is to avoid waiting for the queue
				resp, err := n.server.SyncRPC(ctx, dest, msgBody)
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						n.mapRQ[dest].AddRetry(msgBody)
						return
					} else {
						fmt.Fprintf(os.Stderr, "SyncRPC returns unexpected err: %s\n", err)
					}
				}
				var body map[string]any

				if err := json.Unmarshal(resp.Body, &body); err != nil {
					fmt.Fprintf(os.Stderr, "Unmarshal returns err: %s\n", err)
				}

				if val, ok := body["type"]; ok {
					if val == "broadcast_ok" {
						n.mapRQ[dest].addAcked(message)
					} else {
						fmt.Fprintf(os.Stderr, "WARN: Unexpected response type, got: %s\n", val)
					}
				} else {
					fmt.Fprintf(os.Stderr, "`type` not found in response body\n")
				}
			}(n, dest, body)
		}
	}()

	return n.server.Reply(msg, map[string]string{"type": "broadcast_ok"})
}

func (n *Node) readHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	body["messages"] = n.getStateValues()

	return n.server.Reply(msg, body)
}

func (n *Node) topologyHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var topology = body["topology"].(map[string]interface{})
	n.neighbors = getNeighborsFromTopology(n.server.ID(), topology)

	for _, node := range n.neighbors {
		n.mapRQ[node] = &RetryQueue{
			nodeID: node,
			queue:  aq.New(),
			server: n.server,
			mu:     &sync.Mutex{},
			ackedMsgs: &AckedMsgs{
				&sync.Mutex{},
				make(map[int]struct{}),
			},
		}
	}

	return n.server.Reply(msg, map[string]string{"type": "topology_ok"})
}

func (n *Node) getStateValues() []interface{} {
	n.state.rwmu.RLock()
	defer n.state.rwmu.RUnlock()

	return n.state.set.Values()
}

func (s *State) addIfNotExist(msg int) bool {
	s.rwmu.Lock()
	defer s.rwmu.Unlock()
	if !s.set.Contains(msg) {
		s.set.Add(msg)
		return true
	} else {
		return false
	}
}

// The topology type of map[string]interface{} comes from `json.Unmarshal`.
//
// This function correctly handles the topology type parsing and extracts
// the neighbors of nodeID.
func getNeighborsFromTopology(nodeID string, topology map[string]interface{}) []string {
	dirtyNodeNeighbors := topology[nodeID].([]interface{})

	// Conversion from []interface{} to []string is not so trivial
	nodeNeighbors := make([]string, len(dirtyNodeNeighbors))
	for i, v := range dirtyNodeNeighbors {
		nodeNeighbors[i] = fmt.Sprint(v)
	}

	fmt.Fprintln(os.Stderr, "Received topology: ", topology)
	fmt.Fprintf(os.Stderr, "Neighbors of node %s are: %v\n", nodeID, nodeNeighbors)

	return nodeNeighbors
}

func removeElement(slice []string, element string) []string {
	index := -1
	for i, val := range slice {
		if val == element {
			index = i
			break
		}
	}
	if index == -1 {
		return slice // Element not found
	}
	return append(slice[:index], slice[index+1:]...)
}

func extractMessageFromBody(body map[string]interface{}) int {
	return int(body["message"].(float64))
}
