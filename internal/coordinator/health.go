package coordinator

import (
	"errors"
	"sync"
	"time"
)

type HealthTracker struct {
	mu       sync.Mutex
	Replicas map[int]*Replica
}

func NewHealthTracker() *HealthTracker {
	return &HealthTracker{
		Replicas: make(map[int]*Replica),
	}
}

func (h *HealthTracker) RegisterReplica(id int, address string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Replicas[id] = &Replica{ID: id, Address: address, Healthy: true, LastHeartbeat: time.Now()}
}

func (h *HealthTracker) UpdateFromHeartbeat(id int, activeRequests int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.Replicas[id]; ok {
		r.Healthy = true
		r.LastHeartbeat = time.Now()
		r.ActiveRequests = activeRequests
	}
}

func (h *HealthTracker) MarkUnhealthyIfStale(timeout time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.Replicas {
		if time.Since(r.LastHeartbeat) > timeout {
			r.Healthy = false
		}
	}
}

func (h *HealthTracker) PickLeastLoadedReplica() (*Replica, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var best *Replica
	for _, r := range h.Replicas {
		if !r.Healthy {
			continue
		}
		if best == nil || r.ActiveRequests < best.ActiveRequests {
			best = r
		}
	}

	if best == nil {
		return nil, errors.New("no healthy replicas available")
	}
	return best, nil
}
