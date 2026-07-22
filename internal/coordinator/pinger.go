package coordinator

import (
	"encoding/json"
	"net/http"
	"time"
)

type ReplicaStatus struct {
	ActiveRequests int `json:"active_requests"`
}

func RunHealthPinger(tracker *HealthTracker, interval time.Duration, stop chan bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	client := &http.Client{Timeout: 500 * time.Millisecond}

	for {
		select {
		case <-ticker.C:
			tracker.mu.Lock()
			var ids []int
			for id := range tracker.Replicas {
				ids = append(ids, id)
			}
			tracker.mu.Unlock()

			for _, id := range ids {
				tracker.mu.Lock()
				addr := tracker.Replicas[id].Address
				tracker.mu.Unlock()

				resp, err := client.Get("http://" + addr + "/health")
				if err != nil {
					continue // leave as-is; MarkUnhealthyIfStale will catch it
				}
				var status ReplicaStatus
				json.NewDecoder(resp.Body).Decode(&status)
				resp.Body.Close()
				tracker.UpdateFromHeartbeat(id, status.ActiveRequests)
			}

			tracker.MarkUnhealthyIfStale(2 * interval)
		case <-stop:
			return
		}
	}
}
