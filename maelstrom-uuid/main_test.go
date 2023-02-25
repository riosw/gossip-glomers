package main

import (
	"testing"
)

func TestGenerateUID(t *testing.T) {
	t.Log(GenerateUID())
}

func TestGetNodeiD(t *testing.T) {
	DebugGetNodeID()
}
