package raft

type Transport interface {
	SendRequestVote(peerID int, msg RequestVoteMsg) RequestVoteReply
	SendAppendEntries(peerID int, msg AppendEntriesMsg) AppendEntriesReply
	SendPreVote(peerID int, msg PreVoteMsg) PreVoteReply
}
