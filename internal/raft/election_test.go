package raft

import (
	"testing"
	"time"
)

func TestRandomElectionTimeout(t *testing.T) {
	t1 := randomElectionTimeout()
	t2 := randomElectionTimeout()

	if t1 < 150*time.Millisecond || t1 > 300*time.Millisecond {
		t.Errorf("timeout out of range: %v", t1)
	}
	if t1 == t2 {
		t.Logf("warning: got same value twice, possible but unlikely: %v", t1)
	}
}

func TestRunFollowerLoop_MultipleHeartbeatsThenTimeout(t *testing.T) {
	n := NewNode(1, 5)
	inbox := make(chan string, 2)
	inbox <- "heartbeat 1"
	inbox <- "heartbeat 2"

	RunFollowerLoop(n, inbox)

	if n.State != Candidate {
		t.Errorf("expected state Candidate after heartbeats + eventual timeout, got %v", n.State)
	}
}

func TestRunFollowerLoop_TimeoutFires(t *testing.T) {
	n := NewNode(1, 5)
	inbox := make(chan string) // empty, nothing sent

	RunFollowerLoop(n, inbox)

	if n.State != Candidate {
		t.Errorf("expected state to become Candidate, got %v", n.State)
	}
}
