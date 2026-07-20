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

	replies := make(chan AppendEntriesReply, len(peers))

	for _, peer := range peers {
		go func(p Peer) {
			for {
				nextIdx := n.NextIndex[p.ID]
				if nextIdx == 0 {
					nextIdx = 1
				}
				prevLogIndex := nextIdx - 1
				prevLogTerm := 0
				if prevLogIndex > 0 {
					prevLogTerm = n.Log[prevLogIndex-1].Term
				}
				entriesToSend := n.Log[prevLogIndex:]

				replyChan := make(chan AppendEntriesReply, 1)
				msg := AppendEntriesMsg{
					LeaderID:     n.ID,
					Term:         n.Term,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entriesToSend,
					LeaderCommit: n.CommitIndex,
					ReplyChan:    replyChan,
				}
				p.AppendInbox <- msg
				reply := <-replyChan

				if reply.Success {
					n.MatchIndex[p.ID] = len(n.Log)
					n.NextIndex[p.ID] = len(n.Log) + 1
					replies <- reply
					return
				}

				n.NextIndex[p.ID]--
				if n.NextIndex[p.ID] < 1 {
					n.NextIndex[p.ID] = 1
				}
			}
		}(peer)
	}

	successCount := 1
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
