package raft

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
