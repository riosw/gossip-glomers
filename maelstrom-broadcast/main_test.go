package main

import (
	"encoding/json"
	"fmt"

	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopologyType(t *testing.T) {
	var topologyJSON = []byte(`{"type":"topology","topology":{"n0":["n3","n1"],"n1":["n4","n2","n0"],"n2":["n1"],"n3":["n0","n4"],"n4":["n1","n3"]},"msg_id":1}`)
	var body map[string]any

	if err := json.Unmarshal(topologyJSON, &body); err != nil {
		t.Fatalf(err.Error())
	}

	var topology = body["topology"].(map[string]interface{})

	neighbors = getNeighborsFromTopology("n0", topology)

	assert.Equal(t, []string{"n3", "n1"}, neighbors)

}

func TestSlices(t *testing.T) {
	neighbors := []string{"n0", "n1"}
	unacked := make([]string, len(neighbors))

	if len := copy(unacked, neighbors); len != 2 {
		t.Fatalf("wrong length. expected:%d, got:%d", 2, len)
	}

	fmt.Printf("unacked: %v\n", unacked)
	assert.Equal(t, []string{"n0", "n1"}, neighbors)

}
