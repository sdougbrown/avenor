package workflowcontroller

import (
	"strings"
	"testing"
	"time"
)

func TestLeaseExpired(t *testing.T) {
	now := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	expired := &LeaderLease{ExpiresAt: now.Add(-time.Nanosecond)}
	live := &LeaderLease{ExpiresAt: now}

	if !leaseExpired(now, expired) {
		t.Fatal("lease with passed expiry should be expired")
	}
	if leaseExpired(now, live) {
		t.Fatal("lease whose expiry equals now is not yet expired")
	}
	if leaseExpired(now, nil) {
		t.Fatal("nil lease should never be expired")
	}
}

func TestCheckLeaseCAS(t *testing.T) {
	rec := ControllerRecord{
		ControllerID: "alpha",
		OwnerEpoch:   2,
		Leader:       &LeaderLease{LeaseID: "lease_ab", OwnerEpoch: 2},
	}

	if err := checkLeaseCAS(rec, "lease_ab", 2); err != nil {
		t.Fatalf("matching CAS rejected: %v", err)
	}
	for _, tc := range []struct {
		leaseID    string
		ownerEpoch int64
	}{
		{"lease_wrong", 2},
		{"lease_ab", 1},
		{"lease_ab", 3},
	} {
		if err := checkLeaseCAS(rec, tc.leaseID, tc.ownerEpoch); err != ErrConflict {
			t.Fatalf("checkLeaseCAS(%q, %d): got %v, want ErrConflict", tc.leaseID, tc.ownerEpoch, err)
		}
	}
	noLeader := rec
	noLeader.Leader = nil
	if err := checkLeaseCAS(noLeader, "lease_ab", 2); err != ErrConflict {
		t.Fatalf("CAS with no leader: got %v, want ErrConflict", err)
	}
}

func TestNewLeaseID(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 16; i++ {
		id, err := newLeaseID()
		if err != nil {
			t.Fatalf("newLeaseID: %v", err)
		}
		if !strings.HasPrefix(id, "lease_") || len(id) != len("lease_")+32 {
			t.Fatalf("unexpected lease id %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate lease id %q", id)
		}
		seen[id] = true
	}
}

func TestLeaseConstants(t *testing.T) {
	if LeaseTTL <= RenewInterval {
		t.Fatalf("LeaseTTL %s must exceed RenewInterval %s", LeaseTTL, RenewInterval)
	}
}
