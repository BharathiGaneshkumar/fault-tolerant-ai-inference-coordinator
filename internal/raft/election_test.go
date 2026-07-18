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
func TestHandleRequestVote_GrantsWhenUnvoted(t *testing.T) {
	n := NewNode(1, 5)
	replyChan := make(chan RequestVoteReply, 1)

	msg := RequestVoteMsg{
		CandidateID: 2,
		Term:        1,
		ReplyChan:   replyChan,
	}

	n.HandleRequestVote(msg)
	reply := <-replyChan

	if !reply.VoteGranted {
		t.Errorf("expected vote granted, got denied")
	}
	if n.VotedFor != 2 {
		t.Errorf("expected VotedFor = 2, got %v", n.VotedFor)
	}
}

func TestHandleRequestVote_RefusesSecondDifferentCandidate(t *testing.T) {
	n := NewNode(1, 5)
	replyChan1 := make(chan RequestVoteReply, 1)
	replyChan2 := make(chan RequestVoteReply, 1)

	n.HandleRequestVote(RequestVoteMsg{CandidateID: 2, Term: 1, ReplyChan: replyChan1})
	<-replyChan1 // drain first reply

	n.HandleRequestVote(RequestVoteMsg{CandidateID: 3, Term: 1, ReplyChan: replyChan2})
	reply2 := <-replyChan2

	if reply2.VoteGranted {
		t.Errorf("expected vote denied for second different candidate same term")
	}
}

func TestHandleRequestVote_StepsDownOnHigherTerm(t *testing.T) {
	n := NewNode(1, 5)
	n.BecomeCandidate() // term becomes 1, state Candidate

	replyChan := make(chan RequestVoteReply, 1)
	msg := RequestVoteMsg{CandidateID: 2, Term: 5, ReplyChan: replyChan}

	n.HandleRequestVote(msg)
	reply := <-replyChan

	if n.State != Follower {
		t.Errorf("expected state Follower after seeing higher term, got %v", n.State)
	}
	if n.Term != 5 {
		t.Errorf("expected term 5, got %v", n.Term)
	}
	if !reply.VoteGranted {
		t.Errorf("expected vote granted after stepping down")
	}
}
func TestStartElection_WinsWithSinglePeerVote(t *testing.T) {
	candidate := NewNode(1, 2) // 2-node cluster, majority = 2

	peerNode := NewNode(2, 2)
	peerInbox := make(chan RequestVoteMsg)

	go func() {
		msg := <-peerInbox
		peerNode.HandleRequestVote(msg)
	}()

	peers := []Peer{
		{ID: 2, Inbox: peerInbox},
	}

	won := StartElection(candidate, peers)

	if !won {
		t.Errorf("expected candidate to win election")
	}
	if candidate.State != Leader {
		t.Errorf("expected candidate state Leader, got %v", candidate.State)
	}
}
