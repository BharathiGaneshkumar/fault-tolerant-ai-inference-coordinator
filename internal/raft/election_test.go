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
