package raft

import "time"

func (n *Node) HandleAppendEntries(msg AppendEntriesMsg) {
	success := false

	if msg.Term < n.Term {
		reply := AppendEntriesReply{FollowerID: n.ID, Term: n.Term, Success: false}
		msg.ReplyChan <- reply
		return
	}

	if msg.Term > n.Term {
		n.BecomeFollower(msg.Term)
	}

	if msg.PrevLogIndex == 0 || (msg.PrevLogIndex <= len(n.Log) && n.Log[msg.PrevLogIndex-1].Term == msg.PrevLogTerm) {
		success = true
		n.Log = n.Log[:msg.PrevLogIndex]
		n.Log = append(n.Log, msg.Entries...)

		if msg.LeaderCommit > n.CommitIndex {
			if msg.LeaderCommit < len(n.Log) {
				n.CommitIndex = msg.LeaderCommit
			} else {
				n.CommitIndex = len(n.Log)
			}
		}
	}

	reply := AppendEntriesReply{FollowerID: n.ID, Term: n.Term, Success: success}
	msg.ReplyChan <- reply
}
func SendAppendEntries(n *Node, peers []Peer, entries []LogEntry) bool {
	n.Log = append(n.Log, entries...)

	prevLogIndex := len(n.Log) - len(entries)
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = n.Log[prevLogIndex-1].Term
	}

	replies := make(chan AppendEntriesReply, len(peers))

	for _, peer := range peers {
		go func(p Peer) {
			replyChan := make(chan AppendEntriesReply, 1)
			msg := AppendEntriesMsg{
				LeaderID:     n.ID,
				Term:         n.Term,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.CommitIndex,
				ReplyChan:    replyChan,
			}
			p.AppendInbox <- msg
			reply := <-replyChan
			replies <- reply
		}(peer)
	}

	successCount := 1 // leader itself counts
	for i := 0; i < len(peers); i++ {
		reply := <-replies
		if reply.Success {
			successCount++
		}
	}

	majority := n.ClusterSize/2 + 1
	if successCount >= majority {
		n.CommitIndex = len(n.Log)
		return true
	}

	return false
}
func RunLeaderHeartbeatLoop(n *Node, peers []Peer, stop chan bool) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			SendAppendEntries(n, peers, []LogEntry{})
		case <-stop:
			return
		}
	}
}
