package workflowcontroller

import (
	"errors"
	"testing"
	"time"
)

func newWithLeaderStore(t *testing.T) (*ControllerStore, string, string, int64) {
	t.Helper()
	s := NewStoreWithClock(t.TempDir(), func() time.Time { return time.Now().UTC() })
	if _, err := s.Create("c1", 5); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.SetDesiredState("c1", DesiredEnabled, "test"); err != nil {
		t.Fatalf("enable: %v", err)
	}
	rec, ok, err := s.AcquireLease("c1", "owner-1")
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	return s, "c1", rec.Leader.LeaseID, rec.Leader.OwnerEpoch
}

func TestWithLeaderRunsCallbackUnderLiveLease(t *testing.T) {
	s, id, leaseID, epoch := newWithLeaderStore(t)
	ran := false
	if err := s.WithLeader(id, leaseID, epoch, func() error {
		ran = true
		return nil
	}); err != nil {
		t.Fatalf("WithLeader: %v", err)
	}
	if !ran {
		t.Fatal("callback did not run under a live leader lease")
	}
}

func TestWithLeaderRejectsWrongLeaseOrEpoch(t *testing.T) {
	s, id, leaseID, epoch := newWithLeaderStore(t)
	cases := map[string]struct {
		leaseID string
		epoch   int64
	}{
		"wrong lease id": {leaseID + "x", epoch},
		"wrong epoch":    {leaseID, epoch + 1},
		"epoch zero":     {leaseID, 0},
		"unknown lease":  {"lease_missing", epoch},
	}
	for name, tc := range cases {
		err := s.WithLeader(id, tc.leaseID, tc.epoch, func() error { return nil })
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("%s: error = %v, want ErrNotLeader", name, err)
		}
	}
	if err := s.WithLeader("missing", leaseID, epoch, func() error { return nil }); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unknown controller: error = %v, want ErrNotLeader", err)
	}
}

func TestWithLeaderRejectsDisabledAndExpired(t *testing.T) {
	s, id, leaseID, epoch := newWithLeaderStore(t)
	if _, err := s.SetDesiredState(id, DesiredDisabled, "test"); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if err := s.WithLeader(id, leaseID, epoch, func() error { return nil }); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("disabled controller: error = %v, want ErrNotLeader", err)
	}
	if _, err := s.SetDesiredState(id, DesiredEnabled, "re-enable"); err != nil {
		t.Fatalf("enable: %v", err)
	}
	s.now = func() time.Time { return time.Now().UTC().Add(2 * LeaseTTL) }
	if err := s.WithLeader(id, leaseID, epoch, func() error { return nil }); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expired lease: error = %v, want ErrNotLeader", err)
	}
}
