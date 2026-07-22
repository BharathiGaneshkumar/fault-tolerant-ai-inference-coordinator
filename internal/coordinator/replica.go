package coordinator

import "time"

type Replica struct {
	ID             int
	Address        string
	Healthy        bool
	LastHeartbeat  time.Time
	ActiveRequests int
}
