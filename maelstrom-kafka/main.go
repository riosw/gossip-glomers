package main

import (
	"encoding/json"
	"log"

	"maelstrom-kafka/server"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	s := server.New()

	s.Node.Handle("send", func(reqMsg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(reqMsg.Body, &body); err != nil {
			return err
		}

		var key string = body["key"].(string)
		var msg int = int(body["msg"].(float64))
		offset, err := s.Send(key, msg)
		if err != nil {
			return err
		}

		return s.Node.Reply(reqMsg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	s.Node.Handle("poll", func(reqMsg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(reqMsg.Body, &body); err != nil {
			return err
		}

		var offsets map[string]int = body["offsets"].(map[string]int)
		msgs, err := s.Poll(offsets)
		if err != nil {
			return err
		}

		return s.Node.Reply(
			reqMsg,
			map[string]any{
				"type": "poll_ok",
				"msgs": msgs,
			})
	})

	s.Node.Handle("commit_offsets", func(reqMsg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(reqMsg.Body, &body); err != nil {
			return err
		}

		var offsets map[string]int = body["offsets"].(map[string]int)
		err := s.CommitOffsets(offsets)
		if err != nil {
			return err
		}

		return s.Node.Reply(
			reqMsg,
			map[string]any{
				"type": "commit_offsets_ok",
			})
	})

	s.Node.Handle("list_committed_offsets", func(reqMsg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(reqMsg.Body, &body); err != nil {
			return err
		}

		var keys map[string]string = body["keys"].(map[string]string)
		offsets, err := s.ListCommittedOffsets(keys)
		if err != nil {
			return err
		}

		return s.Node.Reply(
			reqMsg,
			map[string]any{
				"type":    "list_committed_offsets_ok",
				"offsets": offsets,
			})
	})

	// Initialize NodeIDs to determine nodes to be read from maelstrom.KV
	// s.Node.Handle("init", func(reqMsg maelstrom.Message) error {
	// 	var body map[string]any
	// 	if err := json.Unmarshal(reqMsg.Body, &body); err != nil {
	// 		return err
	// 	}

	// 	s.SetNodeIDs(body["node_ids"].([]interface{}))

	// 	return nil
	// })

	if err := s.Node.Run(); err != nil {
		log.Fatal(err)
	}
}
