package raft

import "testing"

// t -> test assistant object
func TestBecomeCandidate(t *testing.T) {
	n := NewNode(1)
	n.BecomeCandidate()
	if n.State != Candidate {
		t.Errorf("expected state Candidate, got %v", n.State)
	}
	if n.Term != 1 {
		t.Errorf("expected term 1, got %v", n.Term)
	}
}
