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
