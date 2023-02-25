package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var neighbors []string
var state []int

func main() {
	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var message int = int(body["message"].(float64))

		if isInState(message) {
			log.Printf("Message %d already exist inside state", message)
		} else {
			state = append(state, message)

			for _, neighbor := range neighbors {
				n.Send(neighbor, body)
			}
		}

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = state

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

func isInState(message int) bool {
	for _, v := range state {
		if v == message {
			return true
		}
	}
	return false
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

	log.Default().Println("Received topology: ", topology)
	log.Default().Printf("Neighbors of node %s are: %v", nodeID, nodeNeighbors)

	return nodeNeighbors
}
