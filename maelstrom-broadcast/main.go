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

	"github.com/emirpasic/gods/sets/hashset"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var rpcSleepTime = 1000 * time.Millisecond
var timeoutDur = 500 * time.Millisecond

type Node struct {
	server    *maelstrom.Node
	neighbors []string
	state     *State
}

type State struct {
	set  hashset.Set
	rwmu sync.RWMutex
}

func main() {
	n := Node{
		server:    maelstrom.NewNode(),
		neighbors: []string{},
		state: &State{
			set: *hashset.New(),
		},
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
	var message int = int(body["message"].(float64))

	if ok := n.state.appendIfNotExist(message); !ok {
		fmt.Fprintf(os.Stderr, "Message %d already exist inside state\n", message)
	}

	unacked := make([]string, len(n.neighbors))
	copy(unacked, n.neighbors)

	var muUnacked sync.Mutex
	unacked = removeElement(unacked, msg.Src, &muUnacked)

	go func() {
		for len(unacked) > 0 {
			for _, dest := range unacked {
				go func(dest string, msgBody map[string]any) {
					ctx := context.Background()
					ctx, cancel := context.WithTimeout(ctx, timeoutDur)

					defer cancel()
					resp, err := n.server.SyncRPC(ctx, dest, msgBody)
					if err != nil {
						if errors.Is(err, context.DeadlineExceeded) {
							log.Fatalln("TODO: ADD TO QUEUE")
						}
						fmt.Fprintf(os.Stderr, "SyncRPC returns unexpected err: %s\n", err)
					}
					var body map[string]any

					if err := json.Unmarshal(resp.Body, &body); err != nil {
						fmt.Fprintf(os.Stderr, "Unmarshal returns err: %s\n", err)
					}

					if val, ok := body["type"]; ok {
						if val != "broadcast_ok" {
							fmt.Fprintf(os.Stderr, "WARN: Unexpected type value, got: %s\n", val)
						} else {
							// Don't retry this anymore
							unacked = removeElement(unacked, dest, &muUnacked)

						}
					}
				}(dest, body)
			}
			time.Sleep(rpcSleepTime)
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

	return n.server.Reply(msg, map[string]string{"type": "topology_ok"})
}

func (n *Node) getStateValues() []interface{} {
	n.state.rwmu.RLock()
	defer n.state.rwmu.RUnlock()

	return n.state.set.Values()
}

func (s *State) appendIfNotExist(msg int) bool {
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

func removeElement(slice []string, element string, mu *sync.Mutex) []string {
	mu.Lock()
	defer mu.Unlock()
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
