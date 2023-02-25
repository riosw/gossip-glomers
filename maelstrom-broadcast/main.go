package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Topology map[string]any

func main() {
	n := maelstrom.NewNode()
	var topology Topology
	var state []int

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state = append(state, int(body["message"].(float64)))

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

		topology = body["topology"].(map[string]interface{})

		log.Default().Println("Topology has been set: ", topology)
		// for k, v := range topology {
		// 	log.Default().Printf("%s: %v", k, v)
		// }

		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
