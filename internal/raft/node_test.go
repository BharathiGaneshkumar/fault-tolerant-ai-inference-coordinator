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

func TestBecomeLeader(t *testing.T) {
	n := NewNode(1)
	n.BecomeCandidate() // term becomes 1
	n.BecomeLeader()

	if n.State != Leader {
		t.Errorf("expected state Leader, got %v", n.State)
	}
	if n.Term != 1 {
		t.Errorf("expected term to stay 1, got %v", n.Term)
	}
}

func TestBecomeFollower(t *testing.T) {
	n := NewNode(1)
	n.BecomeCandidate() // term becomes 1
	n.BecomeFollower(5) // simulate seeing a higher term

	if n.State != Follower {
		t.Errorf("expected state Follower, got %v", n.State)
	}
	if n.Term != 5 {
		t.Errorf("expected term 5, got %v", n.Term)
	}
}
