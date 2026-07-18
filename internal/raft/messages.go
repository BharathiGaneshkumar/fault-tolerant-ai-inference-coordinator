package raft

type RequestVoteMsg struct {
	CandidateID int
	Term        int
	ReplyChan   chan RequestVoteReply
}

type RequestVoteReply struct {
	VoterID     int
	Term        int
	VoteGranted bool
}
type Peer struct {
	ID    int
	Inbox chan RequestVoteMsg
}
