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
	timeout := time.After(randomElectionTimeout())

	select {
	case msg := <-inbox:
		fmt.Println("received heartbeat:", msg)
	case <-timeout:
		fmt.Println("election timeout fired, becoming candidate")
		n.BecomeCandidate()
	}
}
