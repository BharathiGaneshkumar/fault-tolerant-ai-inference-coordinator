package coordinator

import "testing"

func TestPickLeastLoadedReplica(t *testing.T) {
	tracker := NewHealthTracker()
	tracker.RegisterReplica(1, "localhost:9001")
	tracker.RegisterReplica(2, "localhost:9002")
	tracker.RegisterReplica(3, "localhost:9003")

	tracker.UpdateFromHeartbeat(1, 5)
	tracker.UpdateFromHeartbeat(2, 1)
	tracker.UpdateFromHeartbeat(3, 3)

	best, err := tracker.PickLeastLoadedReplica()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if best.ID != 2 {
		t.Errorf("expected replica 2 (least loaded), got %v", best.ID)
	}
}

func TestPickLeastLoadedReplica_NoHealthyReplicas(t *testing.T) {
	tracker := NewHealthTracker()
	tracker.RegisterReplica(1, "localhost:9001")
	tracker.MarkUnhealthyIfStale(0) // force everyone stale/unhealthy immediately

	_, err := tracker.PickLeastLoadedReplica()
	if err == nil {
		t.Errorf("expected error when no healthy replicas available")
	}
}
