package raft

type Transport interface {
	SendRequestVote(peerID int, msg RequestVoteMsg) RequestVoteReply
	SendAppendEntries(peerID int, msg AppendEntriesMsg) AppendEntriesReply
}
