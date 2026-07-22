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
	PersistPath   string
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
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = Candidate
	n.Term++
	n.VotesReceived = 1
	n.VotedFor = n.ID
}
func (n *Node) BecomeLeader(peerIDs []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = Leader
	for _, id := range peerIDs {
		n.NextIndex[id] = len(n.Log) + 1
		n.MatchIndex[id] = 0
	}
	if n.PersistPath != "" {
		SaveState(n, n.PersistPath)
	}
}

func (n *Node) BecomeFollower(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = Follower
	n.Term = term
	n.VotedFor = 0
	n.LastHeartbeat = time.Now()
	if n.PersistPath != "" {
		SaveState(n, n.PersistPath)
	}
}

func (n *Node) GetState() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.State
}

func (n *Node) ReceiveVote() bool {
	n.VotesReceived++
	majority := n.ClusterSize/2 + 1
	return n.VotesReceived >= majority
}
