package raft

import (
	"testing"
	"time"
)

type TestCluster struct {
	Nodes         []*Node
	VoteInboxes   map[int]chan voteRequestEnvelope
	AppendInboxes map[int]chan appendRequestEnvelope
}

func setupTestCluster(size int) *TestCluster {
	cluster := &TestCluster{
		VoteInboxes:   make(map[int]chan voteRequestEnvelope),
		AppendInboxes: make(map[int]chan appendRequestEnvelope),
	}

	for id := 1; id <= size; id++ {
		n := NewNode(id, size)
		cluster.Nodes = append(cluster.Nodes, n)
		cluster.VoteInboxes[id] = make(chan voteRequestEnvelope, 10)
		cluster.AppendInboxes[id] = make(chan appendRequestEnvelope, 10)
	}

	return cluster
}

func TestFullClusterElection(t *testing.T) {
	cluster := setupTestCluster(3)
	stop := make(chan bool)

	for _, n := range cluster.Nodes {
		peersMap := make(map[int]ChannelPeer)
		var peerIDs []int
		for _, other := range cluster.Nodes {
			if other.ID != n.ID {
				peersMap[other.ID] = ChannelPeer{
					VoteInbox:   cluster.VoteInboxes[other.ID],
					AppendInbox: cluster.AppendInboxes[other.ID],
				}
				peerIDs = append(peerIDs, other.ID)
			}
		}

		transport := &ChannelTransport{Peers: peersMap}

		go RunNodeLifecycle(n, cluster.VoteInboxes[n.ID], cluster.AppendInboxes[n.ID], transport, peerIDs, stop)
	}

	time.Sleep(1500 * time.Millisecond)

	leaderCount := 0
	for _, n := range cluster.Nodes {
		if n.State == Leader {
			leaderCount++
		}
	}

	close(stop)

	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %v", leaderCount)
	}
}
