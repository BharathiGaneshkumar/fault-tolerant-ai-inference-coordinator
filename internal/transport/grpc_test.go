package transport

import (
	"context"
	"net"
	"testing"
	"time"

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
func TestGRPCCluster_RealElection(t *testing.T) {
	size := 3
	nodes := make(map[int]*raft.Node)
	listeners := make(map[int]net.Listener)
	servers := make(map[int]*grpc.Server)

	for id := 1; id <= size; id++ {
		n := raft.NewNode(id, size)
		nodes[id] = n

		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		listeners[id] = lis

		gs := grpc.NewServer()
		pb.RegisterRaftServiceServer(gs, &RaftGRPCServer{Node: n})
		servers[id] = gs
		go gs.Serve(lis)
	}
	defer func() {
		for _, gs := range servers {
			gs.Stop()
		}
	}()

	clients := make(map[int]pb.RaftServiceClient)
	for id, lis := range listeners {
		c, err := NewRaftClient(lis.Addr().String())
		if err != nil {
			t.Fatalf("client creation failed: %v", err)
		}
		clients[id] = c
	}

	stop := make(chan bool)
	for id, n := range nodes {
		var peerIDs []int
		peerClients := make(map[int]pb.RaftServiceClient)
		for otherID, c := range clients {
			if otherID != id {
				peerIDs = append(peerIDs, otherID)
				peerClients[otherID] = c
			}
		}
		transport := &GRPCTransport{Clients: peerClients}
		voteInbox := make(chan raft.RequestVoteMsg) // unused placeholder, real routing is via gRPC server
		appendInbox := make(chan raft.AppendEntriesMsg)
		_ = voteInbox
		_ = appendInbox
		go raft.RunNodeLifecycleGRPC(n, transport, peerIDs, stop)
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)

	leaderCount := 0
	for _, n := range nodes {
		if n.State == raft.Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %v", leaderCount)
	}
}
