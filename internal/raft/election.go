package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func randomElectionTimeout() time.Duration {
	ms := 150 + rand.Intn(150) // random between 150-300ms
	return time.Duration(ms) * time.Millisecond
}

func RunNodeLoop(n *Node, voteInbox chan voteRequestEnvelope, appendInbox chan appendRequestEnvelope, stop chan bool) {
	for {
		if n.State == Leader {
			return
		}

		timeout := time.After(randomElectionTimeout())

		select {
		case envelope := <-voteInbox:
			reply := n.HandleRequestVote(envelope.msg)
			envelope.replyChan <- reply
		case envelope := <-appendInbox:
			reply := n.HandleAppendEntries(envelope.msg)
			envelope.replyChan <- reply
		case <-timeout:
			fmt.Println("node", n.ID, "election timeout fired, becoming candidate")
			n.BecomeCandidate()
			return
		case <-stop:
			return
		}
	}
}

func (n *Node) HandleRequestVote(msg RequestVoteMsg) RequestVoteReply {
	voteGranted := false

	if msg.Term > n.Term {
		n.BecomeFollower(msg.Term)
	}

	myLastLogIndex := len(n.Log)
	myLastLogTerm := 0
	if myLastLogIndex > 0 {
		myLastLogTerm = n.Log[myLastLogIndex-1].Term
	}

	logIsUpToDate := msg.LastLogTerm > myLastLogTerm ||
		(msg.LastLogTerm == myLastLogTerm && msg.LastLogIndex >= myLastLogIndex)

	if msg.Term >= n.Term && (n.VotedFor == 0 || n.VotedFor == msg.CandidateID) && logIsUpToDate {
		voteGranted = true
		n.VotedFor = msg.CandidateID
	}

	return RequestVoteReply{
		VoterID:     n.ID,
		Term:        n.Term,
		VoteGranted: voteGranted,
	}
}
func StartElection(n *Node, transport Transport, peerIDs []int) bool {
	n.BecomeCandidate()

	lastLogIndex := len(n.Log)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = n.Log[lastLogIndex-1].Term
	}

	replies := make(chan RequestVoteReply, len(peerIDs))

	for _, peerID := range peerIDs {
		go func(pid int) {
			msg := RequestVoteMsg{
				CandidateID:  n.ID,
				Term:         n.Term,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := transport.SendRequestVote(pid, msg)
			replies <- reply
		}(peerID)
	}

	for i := 0; i < len(peerIDs); i++ {
		reply := <-replies
		if reply.VoteGranted {
			won := n.ReceiveVote()
			if won {
				fmt.Println("node", n.ID, "won the election, becoming leader for term", n.Term)
				n.BecomeLeader(peerIDs)
				return true
			}
		}
	}

	return false
}
func RunNodeLifecycle(n *Node, voteInbox chan voteRequestEnvelope, appendInbox chan appendRequestEnvelope, transport Transport, peerIDs []int, stop chan bool) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		RunNodeLoop(n, voteInbox, appendInbox, stop)

		if n.State == Candidate {
			won := StartElection(n, transport, peerIDs)
			if won {
				RunLeaderHeartbeatLoop(n, transport, peerIDs, stop)
				return
			}
		}
	}
}
func RunNodeLifecycleGRPC(n *Node, transport Transport, peerIDs []int, stop chan bool) {
	for {
		if n.State == Leader {
			return
		}
		electionTimeout := randomElectionTimeout()
		n.mu.Lock()
		n.LastHeartbeat = time.Now()
		n.mu.Unlock()

		select {
		case <-stop:
			return
		case <-time.After(electionTimeout):
			n.mu.Lock()
			elapsed := time.Since(n.LastHeartbeat)
			n.mu.Unlock()
			if elapsed >= electionTimeout {
				fmt.Println("node", n.ID, "election timeout fired, becoming candidate")
				won := StartElection(n, transport, peerIDs)
				if won {
					RunLeaderHeartbeatLoop(n, transport, peerIDs, stop)
					return
				}
			}
		}
	}
}
