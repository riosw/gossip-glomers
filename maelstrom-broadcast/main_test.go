package main

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
)

func TestTopologyType(t *testing.T) {
	var topologyJSON = []byte(`{"type":"topology","topology":{"n0":["n3","n1"],"n1":["n4","n2","n0"],"n2":["n1"],"n3":["n0","n4"],"n4":["n1","n3"]},"msg_id":1}`)
	var body map[string]any

	if err := json.Unmarshal(topologyJSON, &body); err != nil {
		t.Fatalf(err.Error())
	}

	var topology = body["topology"].(map[string]interface{})

	log.Default().Printf("Type of topology[\"n0\"] is: %T\n", topology["n0"])

	n0Topology := topology["n0"].([]interface{})

	s := make([]string, len(n0Topology))
	for i, v := range n0Topology {
		s[i] = fmt.Sprint(v)
	}

	var neighbors []string = s

	log.Default().Println("Topology has been set: ", topology)
	log.Default().Println("Neighbors has been set: ", neighbors)

	for i, v := range neighbors {
		log.Default().Printf("Neighbor #%d is: %s\n", i, v)
	}

}
