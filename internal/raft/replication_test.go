package raft

import (
	"testing"
	"time"
)

func TestHandleAppendEntries_AcceptsFirstEntry(t *testing.T) {
	n := NewNode(1, 5)

	msg := AppendEntriesMsg{
		LeaderID:     2,
		Term:         1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 1, Command: "x=1"}},
		LeaderCommit: 0,
	}

	reply := n.HandleAppendEntries(msg)

	if !reply.Success {
		t.Errorf("expected success, got failure")
	}
	if len(n.Log) != 1 {
		t.Errorf("expected log length 1, got %v", len(n.Log))
	}
}

func TestSendAppendEntries_ReplicatesAndCommits(t *testing.T) {
	leader := NewNode(1, 3) // 3-node cluster, majority = 2

	peersMap := make(map[int]ChannelPeer)
	var peerIDs []int

	for id := 2; id <= 3; id++ {
		peerNode := NewNode(id, 3)
		appendInbox := make(chan appendRequestEnvelope)

		go func(pn *Node, inbox chan appendRequestEnvelope) {
			envelope := <-inbox
			reply := pn.HandleAppendEntries(envelope.msg)
			envelope.replyChan <- reply
		}(peerNode, appendInbox)

		peersMap[id] = ChannelPeer{AppendInbox: appendInbox}
		peerIDs = append(peerIDs, id)
	}

	transport := &ChannelTransport{Peers: peersMap}

	entries := []LogEntry{{Term: leader.Term, Command: "x=1"}}
	success := SendAppendEntries(leader, transport, peerIDs, entries)

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

	msg := AppendEntriesMsg{
		LeaderID: 2,
		Term:     3, // stale, lower than ours
	}

	reply := n.HandleAppendEntries(msg)

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

	msg := AppendEntriesMsg{
		LeaderID:     2,
		Term:         3,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 3, Command: "new-c"}},
		LeaderCommit: 0,
	}

	reply := n.HandleAppendEntries(msg)

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

	peersMap := make(map[int]ChannelPeer)
	var peerIDs []int

	for id := 2; id <= 3; id++ {
		peerNode := NewNode(id, 3)
		appendInbox := make(chan appendRequestEnvelope, 10)

		go func(pn *Node, inbox chan appendRequestEnvelope) {
			for {
				envelope, ok := <-inbox
				if !ok {
					return
				}
				reply := pn.HandleAppendEntries(envelope.msg)
				envelope.replyChan <- reply
			}
		}(peerNode, appendInbox)

		peersMap[id] = ChannelPeer{AppendInbox: appendInbox}
		peerIDs = append(peerIDs, id)
	}
	leader.BecomeLeader(peerIDs)

	transport := &ChannelTransport{Peers: peersMap}

	stop := make(chan bool)
	done := make(chan bool)

	go func() {
		RunLeaderHeartbeatLoop(leader, transport, peerIDs, stop)
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

func TestSendAppendEntries_CatchesUpLaggingFollower(t *testing.T) {
	leader := NewNode(1, 2)
	leader.Log = []LogEntry{
		{Term: 1, Command: "a"},
		{Term: 1, Command: "b"},
		{Term: 1, Command: "c"},
	}
	leader.NextIndex[2] = 4 // leader optimistically thinks peer has all 3 entries

	peerNode := NewNode(2, 2)
	peerNode.Log = []LogEntry{
		{Term: 1, Command: "a"},
	} // peer is actually only at entry 1

	appendInbox := make(chan appendRequestEnvelope)
	go func(pn *Node, inbox chan appendRequestEnvelope) {
		for {
			envelope, ok := <-inbox
			if !ok {
				return
			}
			reply := pn.HandleAppendEntries(envelope.msg)
			envelope.replyChan <- reply
		}
	}(peerNode, appendInbox)

	transport := &ChannelTransport{
		Peers: map[int]ChannelPeer{
			2: {AppendInbox: appendInbox},
		},
	}

	success := SendAppendEntries(leader, transport, []int{2}, []LogEntry{{Term: 1, Command: "d"}})

	if !success {
		t.Errorf("expected eventual success after backing up")
	}
	if len(peerNode.Log) != 4 {
		t.Errorf("expected peer log length 4 after catch-up, got %v", len(peerNode.Log))
	}
}
