package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	log.Printf("Node with id %s created\n", n.ID())

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "generate_ok"
		body["id"] = GenerateUID()
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func GenerateUID() (uid string) {
	return uuid.NewString()
}

func DebugGetNodeID() {
	n := maelstrom.NewNode()

	log.Printf("Node with id %s created\n", n.ID())
}
