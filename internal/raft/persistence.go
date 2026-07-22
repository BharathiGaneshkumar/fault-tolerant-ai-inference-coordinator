package raft

import (
	"encoding/json"
	"os"
)

type PersistentState struct {
	Term     int        `json:"term"`
	VotedFor int        `json:"voted_for"`
	Log      []LogEntry `json:"log"`
}

func SaveState(n *Node, path string) error {
	n.mu.Lock()
	state := PersistentState{
		Term:     n.Term,
		VotedFor: n.VotedFor,
		Log:      n.Log,
	}
	n.mu.Unlock()

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func LoadState(path string) (*PersistentState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no prior state, fresh start is fine
		}
		return nil, err
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}
