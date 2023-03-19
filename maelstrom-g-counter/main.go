package main

import (
	"encoding/json"
	"log"

	"maelstrom-g-counter/internal/server"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	s := server.New()

	s.Node.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var delta int = body["delta"].(int)
		err := s.Add(delta)
		if err != nil {
			return err
		}

		return s.Node.Reply(msg, map[string]string{"type": "add_ok"})
	})

	s.Node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value, err := s.Read()
		if err != nil {
			return err
		}

		return s.Node.Reply(
			msg,
			map[string]any{
				"type":  "read_ok",
				"value": value})
	})

	if err := s.Node.Run(); err != nil {
		log.Fatal(err)
	}
}
