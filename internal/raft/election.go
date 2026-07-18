package raft

import (
	"math/rand"
	"time"
)

func randomElectionTimeout() time.Duration {
	ms := 150 + rand.Intn(150) // random between 150-300ms
	return time.Duration(ms) * time.Millisecond
}
