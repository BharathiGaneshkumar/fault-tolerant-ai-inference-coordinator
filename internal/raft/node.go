package raft

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type Node struct {
	ID    int
	State NodeState
	Term  int
}

// every node that we create nneds to first be a follower and start with term 0
func NewNode(id int) *Node {
	return &Node{
		ID:    id,
		State: Follower,
		Term:  0,
	}
}

// to trigger election become a candidate
// basically a candidate can be lected a leader only if its term is greater than the previous timed out leader
func (n *Node) BecomeCandidate() {
	n.State = Candidate
	n.Term++
}
