package workflowcontroller

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeClock is a manual clock for driving lease expiry in tests.
type fakeClock struct {
	cur time.Time
}

func (c *fakeClock) Now() time.Time { return c.cur }

func (c *fakeClock) Advance(d time.Duration) { c.cur = c.cur.Add(d) }

func newTestStore(t *testing.T) (*ControllerStore, *fakeClock) {
	t.Helper()
	clock := &fakeClock{cur: time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)}
	return NewStoreWithClock(t.TempDir(), clock.Now), clock
}

// eventKinds reads a controller's event log and returns the kinds in order.
func eventKinds(t *testing.T, s *ControllerStore, controllerID string) []string {
	t.Helper()
	data, err := os.ReadFile(s.eventsPath(controllerID))
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var kinds []string
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		var e ControllerEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("unmarshal event line %q: %v", line, err)
		}
		kinds = append(kinds, e.Kind)
	}
	return kinds
}

func eventReasons(t *testing.T, s *ControllerStore, controllerID, kind string) []string {
	t.Helper()
	data, err := os.ReadFile(s.eventsPath(controllerID))
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var reasons []string
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		var e ControllerEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("unmarshal event line %q: %v", line, err)
		}
		if e.Kind == kind {
			reasons = append(reasons, e.Reason)
		}
	}
	return reasons
}

func mustCreate(t *testing.T, s *ControllerStore, id string, maxInflight int) ControllerRecord {
	t.Helper()
	rec, err := s.Create(id, maxInflight)
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return rec
}

func mustEnable(t *testing.T, s *ControllerStore, id string) ControllerRecord {
	t.Helper()
	rec, err := s.SetDesiredState(id, DesiredEnabled, "")
	if err != nil {
		t.Fatalf("enable %s: %v", id, err)
	}
	return rec
}

func mustAcquire(t *testing.T, s *ControllerStore, id, owner string) ControllerRecord {
	t.Helper()
	rec, acquired, err := s.AcquireLease(id, owner)
	if err != nil || !acquired {
		t.Fatalf("acquire %s for %s: acquired=%v err=%v", id, owner, acquired, err)
	}
	return rec
}

func TestCreateIdempotentAndConflict(t *testing.T) {
	s, _ := newTestStore(t)

	first := mustCreate(t, s, "alpha", 4)
	again, err := s.Create("alpha", 4)
	if err != nil {
		t.Fatalf("idempotent re-create: %v", err)
	}
	if again.ControllerID != first.ControllerID || again.MaxInflight != first.MaxInflight ||
		again.Revision != first.Revision || !again.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("re-create returned a different record: %+v vs %+v", again, first)
	}
	if got := len(eventKinds(t, s, "alpha")); got != 1 {
		t.Fatalf("re-create appended events, log has %d lines", got)
	}

	if _, err := s.Create("alpha", 9); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting re-create: got err=%v, want ErrConflict", err)
	}
}

func TestCreateValidation(t *testing.T) {
	s, _ := newTestStore(t)
	for _, id := range []string{"", "a/b", ".hidden", "sp ace"} {
		if _, err := s.Create(id, 1); !errors.Is(err, ErrInvalidController) {
			t.Fatalf("Create(%q): got err=%v, want ErrInvalidController", id, err)
		}
	}
	for _, n := range []int{0, -1} {
		if _, err := s.Create("ok", n); !errors.Is(err, ErrInvalidController) {
			t.Fatalf("Create max_inflight %d: got err=%v, want ErrInvalidController", n, err)
		}
	}
}

func TestCompetingLeadersSequential(t *testing.T) {
	root := t.TempDir()
	s1 := NewStoreWithClock(root, time.Now)
	s2 := NewStoreWithClock(root, time.Now)
	mustEnable(t, s1, mustCreate(t, s1, "alpha", 2).ControllerID)

	rec1 := mustAcquire(t, s1, "alpha", "owner-1")
	epoch1 := rec1.OwnerEpoch

	rec2, acquired, err := s2.AcquireLease("alpha", "owner-2")
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if acquired {
		t.Fatal("second acquire succeeded while lease unexpired")
	}
	if rec2.Leader == nil || rec2.Leader.OwnerID != "owner-1" {
		t.Fatalf("second acquire did not observe the live lease: %+v", rec2.Leader)
	}

	if _, err := s1.ReleaseLease("alpha", rec1.Leader.LeaseID, epoch1); err != nil {
		t.Fatalf("release: %v", err)
	}
	rec2, acquired, err = s2.AcquireLease("alpha", "owner-2")
	if err != nil || !acquired {
		t.Fatalf("acquire after release: acquired=%v err=%v", acquired, err)
	}
	if rec2.Leader.OwnerID != "owner-2" || rec2.Leader.OwnerEpoch != epoch1+1 {
		t.Fatalf("unexpected new lease: %+v", rec2.Leader)
	}
}

func TestRenewReleaseCASConflicts(t *testing.T) {
	s, _ := newTestStore(t)
	mustEnable(t, s, mustCreate(t, s, "alpha", 1).ControllerID)
	rec := mustAcquire(t, s, "alpha", "owner-1")
	epoch := rec.OwnerEpoch
	leaseID := rec.Leader.LeaseID

	if _, err := s.RenewLease("alpha", "wrong-lease", epoch); !errors.Is(err, ErrConflict) {
		t.Fatalf("renew with wrong lease id: got err=%v, want ErrConflict", err)
	}
	if _, err := s.RenewLease("alpha", leaseID, epoch-1); !errors.Is(err, ErrConflict) {
		t.Fatalf("renew with stale epoch: got err=%v, want ErrConflict", err)
	}
	if _, err := s.RenewLease("alpha", leaseID, epoch+1); !errors.Is(err, ErrConflict) {
		t.Fatalf("renew with future epoch: got err=%v, want ErrConflict", err)
	}
	if _, err := s.ReleaseLease("alpha", "wrong-lease", epoch); !errors.Is(err, ErrConflict) {
		t.Fatalf("release with wrong lease id: got err=%v, want ErrConflict", err)
	}
	if _, err := s.ReleaseLease("alpha", leaseID, epoch-1); !errors.Is(err, ErrConflict) {
		t.Fatalf("release with stale epoch: got err=%v, want ErrConflict", err)
	}

	// A successful release invalidates further CAS operations on the old lease.
	if _, err := s.ReleaseLease("alpha", leaseID, epoch); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := s.RenewLease("alpha", leaseID, epoch); !errors.Is(err, ErrConflict) {
		t.Fatalf("renew after release: got err=%v, want ErrConflict", err)
	}
	if _, err := s.ReleaseLease("alpha", leaseID, epoch); !errors.Is(err, ErrConflict) {
		t.Fatalf("double release: got err=%v, want ErrConflict", err)
	}
}

func TestDisableReleasesLiveLease(t *testing.T) {
	s, _ := newTestStore(t)
	mustEnable(t, s, mustCreate(t, s, "alpha", 1).ControllerID)
	rec := mustAcquire(t, s, "alpha", "owner-1")
	epoch := rec.OwnerEpoch

	disabled, err := s.SetDesiredState("alpha", DesiredDisabled, "maintenance")
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if disabled.Leader != nil {
		t.Fatalf("disable left a live lease: %+v", disabled.Leader)
	}
	if disabled.DisabledReason != "maintenance" {
		t.Fatalf("disabled reason not recorded: %q", disabled.DisabledReason)
	}

	// Idempotent disable: already disabled with no lease, no new events.
	before := len(eventKinds(t, s, "alpha"))
	if _, err := s.SetDesiredState("alpha", DesiredDisabled, "maintenance"); err != nil {
		t.Fatalf("idempotent disable: %v", err)
	}
	if got := len(eventKinds(t, s, "alpha")); got != before {
		t.Fatalf("idempotent disable appended events: %d -> %d", before, got)
	}

	if _, _, err := s.AcquireLease("alpha", "owner-2"); !errors.Is(err, ErrDisabled) {
		t.Fatalf("acquire while disabled: got err=%v, want ErrDisabled", err)
	}

	mustEnable(t, s, "alpha")
	rec, acquired, err := s.AcquireLease("alpha", "owner-2")
	if err != nil || !acquired {
		t.Fatalf("acquire after re-enable: acquired=%v err=%v", acquired, err)
	}
	if rec.OwnerEpoch != epoch+1 {
		t.Fatalf("owner epoch after re-acquire: got %d, want %d", rec.OwnerEpoch, epoch+1)
	}

	kinds := eventKinds(t, s, "alpha")
	want := []string{EventCreated, EventEnabled, EventLeaderAcquired, EventDisabled, EventLeaderReleased, EventEnabled, EventLeaderAcquired}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("event kinds: got %v, want %v", kinds, want)
	}
}

func TestRecoverExpiresLease(t *testing.T) {
	s, clock := newTestStore(t)
	mustEnable(t, s, mustCreate(t, s, "alpha", 1).ControllerID)
	rec := mustAcquire(t, s, "alpha", "owner-1")
	oldEpoch := rec.OwnerEpoch

	clock.Advance(LeaseTTL + time.Second)
	recovered, err := s.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(recovered) != 1 || recovered[0].ControllerID != "alpha" {
		t.Fatalf("recovered records: %+v", recovered)
	}
	if recovered[0].Leader != nil {
		t.Fatalf("recovery left an expired lease: %+v", recovered[0].Leader)
	}
	if reasons := eventReasons(t, s, "alpha", EventLeaderExpired); len(reasons) != 1 || reasons[0] != "recovery" {
		t.Fatalf("leader_expired reasons: %v", reasons)
	}

	rec, acquired, err := s.AcquireLease("alpha", "owner-2")
	if err != nil || !acquired {
		t.Fatalf("acquire after recovery: acquired=%v err=%v", acquired, err)
	}
	if rec.OwnerEpoch != oldEpoch+1 {
		t.Fatalf("owner epoch after recovery: got %d, want %d", rec.OwnerEpoch, oldEpoch+1)
	}
}

func TestAcquireLeaseReplacesExpiredLease(t *testing.T) {
	s, clock := newTestStore(t)
	mustEnable(t, s, mustCreate(t, s, "alpha", 1).ControllerID)
	rec := mustAcquire(t, s, "alpha", "owner-1")
	oldEpoch := rec.OwnerEpoch

	clock.Advance(LeaseTTL + time.Second)

	// Re-acquire directly without Recover: the expired lease must be cleared
	// in place so a new owner can take leadership.
	rec, acquired, err := s.AcquireLease("alpha", "owner-2")
	if err != nil || !acquired {
		t.Fatalf("acquire over expired lease: acquired=%v err=%v", acquired, err)
	}
	if rec.OwnerEpoch != oldEpoch+1 {
		t.Fatalf("owner epoch after re-acquire: got %d, want %d", rec.OwnerEpoch, oldEpoch+1)
	}
	if rec.Leader == nil || rec.Leader.OwnerID != "owner-2" {
		t.Fatalf("new leader not recorded: %+v", rec.Leader)
	}

	kinds := eventKinds(t, s, "alpha")
	want := []string{EventCreated, EventEnabled, EventLeaderAcquired, EventLeaderExpired, EventLeaderAcquired}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("event kinds: got %v, want %v", kinds, want)
	}
	if reasons := eventReasons(t, s, "alpha", EventLeaderExpired); len(reasons) != 1 || reasons[0] != "timeout" {
		t.Fatalf("leader_expired reasons: got %v, want [timeout]", reasons)
	}
}

func TestTruncatedFinalEventLine(t *testing.T) {
	s, _ := newTestStore(t)
	mustCreate(t, s, "alpha", 1)

	good, err := os.ReadFile(s.eventsPath("alpha"))
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if err := os.WriteFile(s.eventsPath("alpha"), append(good, []byte(`{"kind":"enabled","seq":2,"tim`)...), 0o644); err != nil {
		t.Fatalf("append garbage: %v", err)
	}

	if _, ok, err := s.Get("alpha"); err != nil || !ok {
		t.Fatalf("get with truncated final line: ok=%v err=%v", ok, err)
	}
	after, err := os.ReadFile(s.eventsPath("alpha"))
	if err != nil {
		t.Fatalf("re-read events: %v", err)
	}
	if string(after) != string(good) {
		t.Fatalf("truncated line not removed: %q", after)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatalf("recover with truncated final line: %v", err)
	}
}

func TestMidFileCorruptionIsError(t *testing.T) {
	s, _ := newTestStore(t)
	mustEnable(t, s, mustCreate(t, s, "alpha", 1).ControllerID)

	good, err := os.ReadFile(s.eventsPath("alpha"))
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(good), "\n"), "\n")
	corrupt := lines[0] + "\n{not json\n" + lines[1] + "\n"
	if err := os.WriteFile(s.eventsPath("alpha"), []byte(corrupt), 0o644); err != nil {
		t.Fatalf("write corrupt log: %v", err)
	}

	if _, _, err := s.Get("alpha"); err == nil || !strings.Contains(err.Error(), "corrupted") {
		t.Fatalf("get with mid-file corruption: got err=%v, want corruption error", err)
	}
	if _, err := s.Recover(); err == nil || !strings.Contains(err.Error(), "corrupted") {
		t.Fatalf("recover with mid-file corruption: got err=%v, want corruption error", err)
	}
}

func TestProjectionFailureIsolation(t *testing.T) {
	s, _ := newTestStore(t)

	// A directory at the projection path must not block Create...
	projDir := filepath.Join(s.ControllersRoot(), "alpha", "controller.md")
	if err := os.MkdirAll(projDir, 0o755); err != nil {
		t.Fatalf("mkdir projection dir: %v", err)
	}
	if _, err := s.Create("alpha", 1); err != nil {
		t.Fatalf("create with failing projection: %v", err)
	}
	// ...nor SetDesiredState.
	if _, err := s.SetDesiredState("alpha", DesiredEnabled, ""); err != nil {
		t.Fatalf("enable with failing projection: %v", err)
	}
	if _, ok, err := s.Get("alpha"); err != nil || !ok {
		t.Fatalf("get after projection failures: ok=%v err=%v", ok, err)
	}
}

func TestRenewalEventSuppression(t *testing.T) {
	s, clock := newTestStore(t)
	mustEnable(t, s, mustCreate(t, s, "alpha", 1).ControllerID)
	rec := mustAcquire(t, s, "alpha", "owner-1")
	acquiredAt := clock.cur
	if rec.Revision != 3 {
		t.Fatalf("revision after acquire: got %d, want 3", rec.Revision)
	}

	clock.Advance(10 * time.Second)
	rec, err := s.RenewLease("alpha", rec.Leader.LeaseID, rec.OwnerEpoch)
	if err != nil {
		t.Fatalf("first renew: %v", err)
	}
	if rec.Revision != 4 {
		t.Fatalf("revision after event-bearing renew: got %d, want 4", rec.Revision)
	}
	if got := len(eventReasons(t, s, "alpha", EventLeaderRenewed)); got != 1 {
		t.Fatalf("leader_renewed events after first renew: got %d, want 1", got)
	}

	// A second renewal inside the one-minute window suppresses the event but
	// still extends the lease.
	clock.Advance(10 * time.Second)
	rec, err = s.RenewLease("alpha", rec.Leader.LeaseID, rec.OwnerEpoch)
	if err != nil {
		t.Fatalf("second renew: %v", err)
	}
	if rec.Revision != 4 {
		t.Fatalf("revision after suppressed renew: got %d, want 4", rec.Revision)
	}
	if got := len(eventReasons(t, s, "alpha", EventLeaderRenewed)); got != 1 {
		t.Fatalf("leader_renewed events after suppressed renew: got %d, want 1", got)
	}
	wantExpiry := acquiredAt.Add(20 * time.Second).Add(LeaseTTL)
	if !rec.Leader.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("expiry after two renewals: got %s, want %s", rec.Leader.ExpiresAt, wantExpiry)
	}

	// The snapshot on disk reflects the suppressed renewal.
	fresh, _, err := s.Get("alpha")
	if err != nil {
		t.Fatalf("get after suppressed renew: %v", err)
	}
	if !fresh.Leader.ExpiresAt.Equal(wantExpiry) || fresh.Revision != 4 {
		t.Fatalf("persisted snapshot stale after suppressed renew: %+v", fresh)
	}
}

func TestListSortedAndMissingDir(t *testing.T) {
	s, _ := newTestStore(t)

	records, err := s.List()
	if err != nil {
		t.Fatalf("list with missing controllers dir: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("list with missing controllers dir: got %+v, want empty", records)
	}

	mustCreate(t, s, "beta", 1)
	mustCreate(t, s, "alpha", 2)
	mustCreate(t, s, "gamma", 3)

	records, err = s.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var ids []string
	for _, rec := range records {
		ids = append(ids, rec.ControllerID)
	}
	if strings.Join(ids, ",") != "alpha,beta,gamma" {
		t.Fatalf("list order: got %v, want [alpha beta gamma]", ids)
	}
}
