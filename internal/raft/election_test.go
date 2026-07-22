package raft

import (
	"testing"
	"time"
)

func TestRandomElectionTimeout(t *testing.T) {
	t1 := randomElectionTimeout()
	t2 := randomElectionTimeout()

	if t1 < 500*time.Millisecond || t1 > 1000*time.Millisecond {
		t.Errorf("timeout out of range: %v", t1)
	}
	if t1 == t2 {
		t.Logf("warning: got same value twice, possible but unlikely: %v", t1)
	}
}

func TestHandleRequestVote_GrantsWhenUnvoted(t *testing.T) {
	n := NewNode(1, 5)

	msg := RequestVoteMsg{
		CandidateID: 2,
		Term:        1,
	}

	reply := n.HandleRequestVote(msg)

	if !reply.VoteGranted {
		t.Errorf("expected vote granted, got denied")
	}
	if n.VotedFor != 2 {
		t.Errorf("expected VotedFor = 2, got %v", n.VotedFor)
	}
}

func TestHandleRequestVote_RefusesSecondDifferentCandidate(t *testing.T) {
	n := NewNode(1, 5)

	n.HandleRequestVote(RequestVoteMsg{CandidateID: 2, Term: 1})
	reply2 := n.HandleRequestVote(RequestVoteMsg{CandidateID: 3, Term: 1})

	if reply2.VoteGranted {
		t.Errorf("expected vote denied for second different candidate same term")
	}
}
func TestHandleRequestVote_StepsDownOnHigherTerm(t *testing.T) {
	n := NewNode(1, 5)
	n.BecomeCandidate() // term becomes 1, state Candidate
	msg := RequestVoteMsg{CandidateID: 2, Term: 5}
	reply := n.HandleRequestVote(msg)
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

	voteInbox := make(chan voteRequestEnvelope)

	go func() {
		envelope := <-voteInbox
		reply := peerNode.HandleRequestVote(envelope.msg)
		envelope.replyChan <- reply
	}()

	transport := &ChannelTransport{
		Peers: map[int]ChannelPeer{
			2: {VoteInbox: voteInbox},
		},
	}

	won := StartElection(candidate, transport, []int{2})

	if !won {
		t.Errorf("expected candidate to win election")
	}
	if candidate.State != Leader {
		t.Errorf("expected candidate state Leader, got %v", candidate.State)
	}
}
func TestStartElection_WinsWithMultiplePeers(t *testing.T) {
	candidate := NewNode(1, 5) // 5-node cluster, majority = 3

	peersMap := make(map[int]ChannelPeer)
	var peerIDs []int

	for id := 2; id <= 5; id++ {
		peerNode := NewNode(id, 5)
		voteInbox := make(chan voteRequestEnvelope)

		go func(pn *Node, inbox chan voteRequestEnvelope) {
			envelope := <-inbox
			reply := pn.HandleRequestVote(envelope.msg)
			envelope.replyChan <- reply
		}(peerNode, voteInbox)

		peersMap[id] = ChannelPeer{VoteInbox: voteInbox}
		peerIDs = append(peerIDs, id)
	}

	transport := &ChannelTransport{Peers: peersMap}

	won := StartElection(candidate, transport, peerIDs)

	if !won {
		t.Errorf("expected candidate to win election with 4 peers")
	}
	if candidate.State != Leader {
		t.Errorf("expected candidate state Leader, got %v", candidate.State)
	}
	if candidate.VotesReceived < 3 {
		t.Errorf("expected at least 3 votes (majority), got %v", candidate.VotesReceived)
	}
}
