package transport

import (
	"context"
	"net"
	"testing"

	"raft-inference-coordinator/internal/raft"
	pb "raft-inference-coordinator/proto"

	"google.golang.org/grpc"
)

func TestGRPCRequestVote_EndToEnd(t *testing.T) {
	node := raft.NewNode(1, 3)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, &RaftGRPCServer{Node: node})

	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := NewRaftClient(lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	resp, err := client.RequestVote(context.Background(), &pb.RequestVoteRequest{
		CandidateId: 2,
		Term:        1,
	})

	if err != nil {
		t.Fatalf("RequestVote call failed: %v", err)
	}
	if !resp.VoteGranted {
		t.Errorf("expected vote granted, got denied")
	}
}
