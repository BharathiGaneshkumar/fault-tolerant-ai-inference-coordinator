package raft

import "time"

func (n *Node) HandleAppendEntries(msg AppendEntriesMsg) AppendEntriesReply {
	success := false

	if msg.Term < n.Term {
		return AppendEntriesReply{FollowerID: n.ID, Term: n.Term, Success: false}
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

	return AppendEntriesReply{FollowerID: n.ID, Term: n.Term, Success: success}
}
func SendAppendEntries(n *Node, transport Transport, peerIDs []int, entries []LogEntry) bool {
	n.Log = append(n.Log, entries...)

	replies := make(chan AppendEntriesReply, len(peerIDs))

	for _, peerID := range peerIDs {
		go func(pid int) {
			for {
				n.mu.Lock()
				nextIdx := n.NextIndex[pid]
				n.mu.Unlock()
				if nextIdx == 0 {
					nextIdx = 1
				}
				prevLogIndex := nextIdx - 1
				prevLogTerm := 0
				if prevLogIndex > 0 {
					prevLogTerm = n.Log[prevLogIndex-1].Term
				}
				entriesToSend := n.Log[prevLogIndex:]

				msg := AppendEntriesMsg{
					LeaderID:     n.ID,
					Term:         n.Term,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entriesToSend,
					LeaderCommit: n.CommitIndex,
				}
				reply := transport.SendAppendEntries(pid, msg)
				if reply.Success {
					n.mu.Lock()
					n.MatchIndex[pid] = len(n.Log)
					n.NextIndex[pid] = len(n.Log) + 1
					n.mu.Unlock()
					replies <- reply
					return
				}

				n.mu.Lock()
				n.NextIndex[pid]--
				if n.NextIndex[pid] < 1 {
					n.NextIndex[pid] = 1
				}
				n.mu.Unlock()
			}
		}(peerID)
	}

	successCount := 1
	for i := 0; i < len(peerIDs); i++ {
		reply := <-replies
		if reply.Success {
			successCount++
		}
	}

	majority := n.ClusterSize/2 + 1
	if successCount >= majority {
		if len(n.Log) > 0 {
			lastEntryTerm := n.Log[len(n.Log)-1].Term
			if lastEntryTerm == n.Term {
				n.CommitIndex = len(n.Log)
			}
		}
		return true
	}

	return false
}
func RunLeaderHeartbeatLoop(n *Node, transport Transport, peerIDs []int, stop chan bool) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			SendAppendEntries(n, transport, peerIDs, []LogEntry{})
		case <-stop:
			return
		}
	}
}
