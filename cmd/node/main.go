package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"raft-inference-coordinator/internal/raft"
	"raft-inference-coordinator/internal/transport"
	pb "raft-inference-coordinator/proto"

	"google.golang.org/grpc"
)

func main() {
	id := flag.Int("id", 0, "node ID")
	port := flag.String("port", "50051", "port to listen on")
	peersFlag := flag.String("peers", "", "comma-separated list of id=host:port for other nodes")
	clusterSize := flag.Int("clustersize", 3, "total cluster size")
	flag.Parse()

	if *id == 0 {
		log.Fatal("must provide -id")
	}

	n := raft.NewNode(*id, *clusterSize)
	persistPath := fmt.Sprintf("node%d_state.json", *id)
	n.PersistPath = persistPath

	if state, err := raft.LoadState(persistPath); err == nil && state != nil {
		n.Term = state.Term
		n.VotedFor = state.VotedFor
		n.Log = state.Log
		fmt.Println("node", *id, "restored state: term", state.Term)
	}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, &transport.RaftGRPCServer{Node: n})

	go func() {
		fmt.Println("node", *id, "listening on port", *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve failed: %v", err)
		}
	}()

	peerClients := make(map[int]pb.RaftServiceClient)
	var peerIDs []int

	if *peersFlag != "" {
		pairs := strings.Split(*peersFlag, ",")
		for _, pair := range pairs {
			parts := strings.Split(pair, "=")
			peerID, _ := strconv.Atoi(parts[0])
			address := parts[1]

			client, err := transport.NewRaftClient(address)
			if err != nil {
				log.Fatalf("failed to connect to peer %d: %v", peerID, err)
			}
			peerClients[peerID] = client
			peerIDs = append(peerIDs, peerID)
		}
	}

	rt := &transport.GRPCTransport{Clients: peerClients}
	stop := make(chan bool)

	raft.RunNodeLifecycleGRPC(n, rt, peerIDs, stop)
}
