package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"raft-inference-coordinator/internal/coordinator"
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
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			command := scanner.Text()
			if n.GetState() == raft.Leader {
				success := raft.SendAppendEntries(n, rt, peerIDs, []raft.LogEntry{{Term: n.Term, Command: command}})
				fmt.Println("submitted entry:", command, "success:", success)
			} else {
				fmt.Println("not leader, ignoring input")
			}
		}
	}()

	tracker := coordinator.NewHealthTracker()
	tracker.RegisterReplica(1, "localhost:9001")
	tracker.RegisterReplica(2, "localhost:9002")
	tracker.RegisterReplica(3, "localhost:9003")

	pingerStop := make(chan bool)

	go func() {
		wasLeader := false
		for {
			isLeader := n.GetState() == raft.Leader
			if isLeader && !wasLeader {
				fmt.Println("node", *id, "became leader, starting health pinger")
				go coordinator.RunHealthPinger(tracker, 1*time.Second, pingerStop)
			}
			if !isLeader && wasLeader {
				fmt.Println("node", *id, "no longer leader, stopping health pinger")
				pingerStop <- true
				pingerStop = make(chan bool)
			}
			wasLeader = isLeader
			time.Sleep(200 * time.Millisecond)
		}
	}()

	http.HandleFunc("/infer", func(w http.ResponseWriter, r *http.Request) {
		if n.GetState() != raft.Leader {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		replica, err := tracker.PickLeastLoadedReplica()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		body, _ := io.ReadAll(r.Body)

		resp, err := http.Post("http://"+replica.Address+"/generate", "application/json", bytes.NewReader(body))
		if err != nil {
			http.Error(w, "replica unreachable: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)
		w.Write(respBody)
	})

	go func() {
		httpPort := fmt.Sprintf(":%d", 8000+*id)
		fmt.Println("node", *id, "HTTP API listening on", httpPort)
		if err := http.ListenAndServe(httpPort, nil); err != nil {
			fmt.Println("HTTP server error:", err)
		}
	}()
	go raft.RunNodeLifecycleGRPC(n, rt, peerIDs, stop)
	select {}

}
