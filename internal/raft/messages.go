package raft

type RequestVoteMsg struct {
	CandidateID  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
	ReplyChan    chan RequestVoteReply
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
	ReplyChan    chan AppendEntriesReply
}

type AppendEntriesReply struct {
	FollowerID int
	Term       int
	Success    bool
}
