package transport

import (
	"context"

	"raft-inference-coordinator/internal/raft"
	pb "raft-inference-coordinator/proto"
)

type RaftGRPCServer struct {
	pb.UnimplementedRaftServiceServer
	Node *raft.Node
}

func (s *RaftGRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	replyChan := make(chan raft.RequestVoteReply, 1)

	msg := raft.RequestVoteMsg{
		CandidateID:  int(req.CandidateId),
		Term:         int(req.Term),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
		ReplyChan:    replyChan,
	}

	s.Node.HandleRequestVote(msg)
	reply := <-replyChan

	return &pb.RequestVoteResponse{
		VoterId:     int32(reply.VoterID),
		Term:        int32(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}
func (s *RaftGRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	replyChan := make(chan raft.AppendEntriesReply, 1)

	var entries []raft.LogEntry
	for _, e := range req.Entries {
		entries = append(entries, raft.LogEntry{
			Term:    int(e.Term),
			Command: e.Command,
		})
	}

	msg := raft.AppendEntriesMsg{
		LeaderID:     int(req.LeaderId),
		Term:         int(req.Term),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(req.LeaderCommit),
		ReplyChan:    replyChan,
	}

	s.Node.HandleAppendEntries(msg)
	reply := <-replyChan

	return &pb.AppendEntriesResponse{
		FollowerId: int32(reply.FollowerID),
		Term:       int32(reply.Term),
		Success:    reply.Success,
	}, nil
}
