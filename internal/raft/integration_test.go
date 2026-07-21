package raft

import (
	"testing"
	"time"
)

type TestCluster struct {
	Nodes         []*Node
	VoteInboxes   map[int]chan RequestVoteMsg
	AppendInboxes map[int]chan AppendEntriesMsg
}

func setupTestCluster(size int) *TestCluster {
	cluster := &TestCluster{
		VoteInboxes:   make(map[int]chan RequestVoteMsg),
		AppendInboxes: make(map[int]chan AppendEntriesMsg),
	}

	for id := 1; id <= size; id++ {
		n := NewNode(id, size)
		cluster.Nodes = append(cluster.Nodes, n)
		cluster.VoteInboxes[id] = make(chan RequestVoteMsg, 10)
		cluster.AppendInboxes[id] = make(chan AppendEntriesMsg, 10)
	}

	return cluster
}
func runNodeMessageLoop(n *Node, voteInbox chan RequestVoteMsg, appendInbox chan AppendEntriesMsg, stop chan bool) {
	for {
		select {
		case msg := <-voteInbox:
			n.HandleRequestVote(msg)
		case msg := <-appendInbox:
			n.HandleAppendEntries(msg)
		case <-stop:
			return
		}
	}
}
func TestFullClusterElection(t *testing.T) {
	cluster := setupTestCluster(3)
	stop := make(chan bool)

	for _, n := range cluster.Nodes {
		var peers []Peer
		for _, other := range cluster.Nodes {
			if other.ID != n.ID {
				peers = append(peers, Peer{
					ID:          other.ID,
					VoteInbox:   cluster.VoteInboxes[other.ID],
					AppendInbox: cluster.AppendInboxes[other.ID],
				})
			}
		}

		go RunNodeLifecycle(n, cluster.VoteInboxes[n.ID], cluster.AppendInboxes[n.ID], peers, stop)
	}

	time.Sleep(500 * time.Millisecond)

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
