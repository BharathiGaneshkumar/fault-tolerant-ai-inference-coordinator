package transport

import (
	"context"

	"raft-inference-coordinator/internal/raft"
	pb "raft-inference-coordinator/proto"
)

type GRPCTransport struct {
	Clients map[int]pb.RaftServiceClient
}

func (t *GRPCTransport) SendRequestVote(peerID int, msg raft.RequestVoteMsg) raft.RequestVoteReply {
	client := t.Clients[peerID]

	req := &pb.RequestVoteRequest{
		CandidateId:  int32(msg.CandidateID),
		Term:         int32(msg.Term),
		LastLogIndex: int32(msg.LastLogIndex),
		LastLogTerm:  int32(msg.LastLogTerm),
	}

	resp, err := client.RequestVote(context.Background(), req)
	if err != nil {
		return raft.RequestVoteReply{VoterID: peerID, Term: msg.Term, VoteGranted: false}
	}

	return raft.RequestVoteReply{
		VoterID:     int(resp.VoterId),
		Term:        int(resp.Term),
		VoteGranted: resp.VoteGranted,
	}
}
func (t *GRPCTransport) SendAppendEntries(peerID int, msg raft.AppendEntriesMsg) raft.AppendEntriesReply {
	client := t.Clients[peerID]

	var entries []*pb.LogEntry
	for _, e := range msg.Entries {
		entries = append(entries, &pb.LogEntry{
			Term:    int32(e.Term),
			Command: e.Command,
		})
	}

	req := &pb.AppendEntriesRequest{
		LeaderId:     int32(msg.LeaderID),
		Term:         int32(msg.Term),
		PrevLogIndex: int32(msg.PrevLogIndex),
		PrevLogTerm:  int32(msg.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int32(msg.LeaderCommit),
	}

	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		return raft.AppendEntriesReply{FollowerID: peerID, Term: msg.Term, Success: false}
	}

	return raft.AppendEntriesReply{
		FollowerID: int(resp.FollowerId),
		Term:       int(resp.Term),
		Success:    resp.Success,
	}
}
