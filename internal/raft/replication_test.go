package raft

import (
	"testing"
	"time"
)

func TestHandleAppendEntries_AcceptsFirstEntry(t *testing.T) {
	n := NewNode(1, 5)
	replyChan := make(chan AppendEntriesReply, 1)

	msg := AppendEntriesMsg{
		LeaderID:     2,
		Term:         1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 1, Command: "x=1"}},
		LeaderCommit: 0,
		ReplyChan:    replyChan,
	}

	n.HandleAppendEntries(msg)
	reply := <-replyChan

	if !reply.Success {
		t.Errorf("expected success, got failure")
	}
	if len(n.Log) != 1 {
		t.Errorf("expected log length 1, got %v", len(n.Log))
	}
}
func TestSendAppendEntries_ReplicatesAndCommits(t *testing.T) {
	leader := NewNode(1, 3) // 3-node cluster, majority = 2

	var peers []Peer

	for id := 2; id <= 3; id++ {
		peerNode := NewNode(id, 3)
		appendInbox := make(chan AppendEntriesMsg)

		go func(pn *Node, inbox chan AppendEntriesMsg) {
			msg := <-inbox
			pn.HandleAppendEntries(msg)
		}(peerNode, appendInbox)

		peers = append(peers, Peer{ID: id, AppendInbox: appendInbox})
	}

	entries := []LogEntry{{Term: leader.Term, Command: "x=1"}}
	success := SendAppendEntries(leader, peers, entries)

	if !success {
		t.Errorf("expected replication to succeed with majority")
	}
	if leader.CommitIndex != 1 {
		t.Errorf("expected CommitIndex 1, got %v", leader.CommitIndex)
	}
	if len(leader.Log) != 1 {
		t.Errorf("expected leader log length 1, got %v", len(leader.Log))
	}
}

func TestHandleAppendEntries_RejectsStaleTerm(t *testing.T) {
	n := NewNode(1, 5)
	n.BecomeFollower(5) // we're already at term 5

	replyChan := make(chan AppendEntriesReply, 1)
	msg := AppendEntriesMsg{
		LeaderID:  2,
		Term:      3, // stale, lower than ours
		ReplyChan: replyChan,
	}

	n.HandleAppendEntries(msg)
	reply := <-replyChan

	if reply.Success {
		t.Errorf("expected rejection of stale term, got success")
	}
}

func TestHandleAppendEntries_TruncatesConflictingTail(t *testing.T) {
	n := NewNode(1, 5)
	n.Log = []LogEntry{
		{Term: 1, Command: "a"},
		{Term: 1, Command: "b"},
		{Term: 2, Command: "stale-c"},
		{Term: 2, Command: "stale-d"},
	}

	replyChan := make(chan AppendEntriesReply, 1)
	msg := AppendEntriesMsg{
		LeaderID:     2,
		Term:         3,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 3, Command: "new-c"}},
		LeaderCommit: 0,
		ReplyChan:    replyChan,
	}

	n.HandleAppendEntries(msg)
	reply := <-replyChan

	if !reply.Success {
		t.Errorf("expected success, got failure")
	}
	if len(n.Log) != 3 {
		t.Errorf("expected log length 3 after truncate+append, got %v", len(n.Log))
	}
	if n.Log[2].Command != "new-c" {
		t.Errorf("expected entry 3 to be new-c, got %v", n.Log[2].Command)
	}
}
func TestRunLeaderHeartbeatLoop_SendsHeartbeatsAndStops(t *testing.T) {
	leader := NewNode(1, 3)

	var peers []Peer
	for id := 2; id <= 3; id++ {
		peerNode := NewNode(id, 3)
		appendInbox := make(chan AppendEntriesMsg)

		go func(pn *Node, inbox chan AppendEntriesMsg) {
			for {
				msg, ok := <-inbox
				if !ok {
					return
				}
				pn.HandleAppendEntries(msg)
			}
		}(peerNode, appendInbox)

		peers = append(peers, Peer{ID: id, AppendInbox: appendInbox})
	}

	stop := make(chan bool)
	done := make(chan bool)

	go func() {
		RunLeaderHeartbeatLoop(leader, peers, stop)
		done <- true
	}()

	time.Sleep(160 * time.Millisecond) // let it tick ~3 times
	stop <- true

	select {
	case <-done:
		// loop exited cleanly
	case <-time.After(1 * time.Second):
		t.Errorf("heartbeat loop did not stop in time")
	}
}
