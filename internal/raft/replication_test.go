package raft

import "testing"

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
