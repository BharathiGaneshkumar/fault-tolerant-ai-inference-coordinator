package raft

import (
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type Node struct {
	ID            int
	State         NodeState
	Term          int
	ClusterSize   int
	VotesReceived int
	VotedFor      int
	Log           []LogEntry
	CommitIndex   int
	NextIndex     map[int]int
	MatchIndex    map[int]int
	mu            sync.Mutex
	LastHeartbeat time.Time
}

// every node that we create nneds to first be a follower and start with term 0 and size of the whole cluster
func NewNode(id int, clusterSize int) *Node {
	return &Node{
		ID:          id,
		State:       Follower,
		Term:        0,
		ClusterSize: clusterSize,
		NextIndex:   make(map[int]int),
		MatchIndex:  make(map[int]int),
	}
}

// to trigger election become a candidate
// basically a candidate can be lected a leader only if its term is greater than the previous timed out leader
// candidate votes for itself
func (n *Node) BecomeCandidate() {
	n.State = Candidate
	n.Term++
	n.VotesReceived = 1 // votes for itself
}

// a node/candidate will become leader in same trem, so no bump
func (n *Node) BecomeLeader(peerIDs []int) {
	n.State = Leader
	for _, id := range peerIDs {
		n.NextIndex[id] = len(n.Log) + 1
		n.MatchIndex[id] = 0
	}
}

// a flwr needs to update its term number to current term number proposed
func (n *Node) BecomeFollower(term int) {
	n.State = Follower
	n.Term = term
	n.VotedFor = 0
}

func (n *Node) ReceiveVote() bool {
	n.VotesReceived++
	majority := n.ClusterSize/2 + 1
	return n.VotesReceived >= majority
}
