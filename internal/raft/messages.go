package raft

type RequestVoteMsg struct {
	CandidateID  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	VoterID     int
	Term        int
	VoteGranted bool
}
type Peer struct {
	ID          int
	VoteInbox   chan RequestVoteMsg
	AppendInbox chan AppendEntriesMsg
}
type LogEntry struct {
	Term    int
	Command string
}

type AppendEntriesMsg struct {
	LeaderID     int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	FollowerID int
	Term       int
	Success    bool
}
type PreVoteMsg struct {
	CandidateID  int
	Term         int // the term it WOULD use if it proceeds
	LastLogIndex int
	LastLogTerm  int
}

type PreVoteReply struct {
	VoteGranted bool
}
