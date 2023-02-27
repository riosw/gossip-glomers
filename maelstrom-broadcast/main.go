package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/hashset"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var neighbors []string
var state = hashset.New()
var rwmu sync.RWMutex
var rpcSleepTime = 10000 * time.Millisecond

func main() {
	n := maelstrom.NewNode()

	// TODO: add something like this
	// accumulated_time_to_check_state
	// Everytime isInState called, sum them
	// defer so that on application exit, logs the time lost for deduplication

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var message int = int(body["message"].(float64))

		if ok := appendIfNotInState(message); !ok {
			fmt.Fprintf(os.Stderr, "Message %d already exist inside state", message)
		}
		unacked := make([]string, len(neighbors))

		copy(unacked, neighbors)

		var muUnacked sync.Mutex

		unacked = removeElement(unacked, msg.Src, &muUnacked)

		// log.Default().Printf("unacked: %v", unacked)

		go func() {
			// log.Default().Printf("Trying sending message %d ... to nodes %v", message, unacked)
			for len(unacked) > 0 {
				for _, dest := range unacked {
					err := n.RPC(dest, body, func(msg maelstrom.Message) error {
						var body map[string]any

						if err := json.Unmarshal(msg.Body, &body); err != nil {
							return err
						}

						if val, ok := body["type"]; ok {
							if val != "broadcast_ok" {
								return fmt.Errorf("WARN: Unexpected type value, got: %s", val)
							} else {
								// Don't retry this anymore
								unacked = removeElement(unacked, dest, &muUnacked)

							}
						} else {
							return fmt.Errorf("WARN: `type` not found on message body")
						}

						return nil
					})
					if err != nil {
						log.Fatalf("Unexpected Error on RPC: %v", err)
					}
				}
				time.Sleep(rpcSleepTime)
			}
		}()

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		rwmu.RLock()
		body["messages"] = state.Values()
		rwmu.RUnlock()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var topology = body["topology"].(map[string]interface{})
		neighbors = getNeighborsFromTopology(n.ID(), topology)

		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func isInState(msg int) bool {
	return state.Contains(msg)
}

func appendToState(msg int) {
	state.Add(msg)
}

func appendIfNotInState(msg int) bool {
	rwmu.Lock()
	defer rwmu.Unlock()
	if !isInState(msg) {
		appendToState(msg)
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
	fmt.Fprintf(os.Stderr, "Neighbors of node %s are: %v", nodeID, nodeNeighbors)

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
