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

	if msg.Term >= n.Term && (n.VotedFor == 0 || n.VotedFor == msg.CandidateID) {
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
