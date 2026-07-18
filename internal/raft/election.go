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

func RunFollowerLoop(n *Node, inbox chan string) {
	for {
		timeout := time.After(randomElectionTimeout())

		select {
		case msg := <-inbox:
			fmt.Println("received heartbeat:", msg)
			// loop again, timeout resets naturally since we're back at top
		case <-timeout:
			fmt.Println("election timeout fired, becoming candidate")
			n.BecomeCandidate()
			return // exit the loop, we're no longer a follower
		}
	}
}

func (n *Node) HandleRequestVote(msg RequestVoteMsg) {
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

	reply := RequestVoteReply{
		VoterID:     n.ID,
		Term:        n.Term,
		VoteGranted: voteGranted,
	}

	msg.ReplyChan <- reply
}
func StartElection(n *Node, peers []Peer) bool {
	n.BecomeCandidate()

	lastLogIndex := len(n.Log)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = n.Log[lastLogIndex-1].Term
	}

	replies := make(chan RequestVoteReply, len(peers))

	for _, peer := range peers {
		go func(p Peer) {
			replyChan := make(chan RequestVoteReply, 1)
			msg := RequestVoteMsg{
				CandidateID:  n.ID,
				Term:         n.Term,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				ReplyChan:    replyChan,
			}
			p.VoteInbox <- msg
			reply := <-replyChan
			replies <- reply
		}(peer)
	}

	for i := 0; i < len(peers); i++ {
		reply := <-replies
		if reply.VoteGranted {
			won := n.ReceiveVote()
			if won {
				n.BecomeLeader()
				return true
			}
		}
	}

	return false
}
