package raft

type voteRequestEnvelope struct {
	msg       RequestVoteMsg
	replyChan chan RequestVoteReply
}

type appendRequestEnvelope struct {
	msg       AppendEntriesMsg
	replyChan chan AppendEntriesReply
}

type ChannelPeer struct {
	VoteInbox   chan voteRequestEnvelope
	AppendInbox chan appendRequestEnvelope
}

type ChannelTransport struct {
	Peers map[int]ChannelPeer
}

func (t *ChannelTransport) SendRequestVote(peerID int, msg RequestVoteMsg) RequestVoteReply {
	replyChan := make(chan RequestVoteReply, 1)
	t.Peers[peerID].VoteInbox <- voteRequestEnvelope{msg: msg, replyChan: replyChan}
	return <-replyChan
}

func (t *ChannelTransport) SendAppendEntries(peerID int, msg AppendEntriesMsg) AppendEntriesReply {
	replyChan := make(chan AppendEntriesReply, 1)
	t.Peers[peerID].AppendInbox <- appendRequestEnvelope{msg: msg, replyChan: replyChan}
	return <-replyChan
}
func (t *ChannelTransport) SendPreVote(peerID int, msg PreVoteMsg) PreVoteReply {
	return PreVoteReply{VoteGranted: false} // simplified stub for channel transport, not used in gRPC path
}
