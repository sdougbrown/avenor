package workflowcontroller

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestValidateControllerID(t *testing.T) {
	valid := []string{"a", "controller-1", "Main_Controller.2", "A9_-."}
	for _, id := range valid {
		if err := validateControllerID(id); err != nil {
			t.Fatalf("validateControllerID(%q): unexpected err %v", id, err)
		}
	}
	invalid := []string{"", ".hidden", "a/b", "a b", "café", "a\nb"}
	for _, id := range invalid {
		if err := validateControllerID(id); err == nil {
			t.Fatalf("validateControllerID(%q): expected error", id)
		}
	}
}

func TestControllerRecordJSONRoundTrip(t *testing.T) {
	acquired := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	rec := ControllerRecord{
		SchemaVersion: SchemaVersion,
		ControllerID:  "alpha",
		DesiredState:  DesiredEnabled,
		MaxInflight:   4,
		Revision:      7,
		OwnerEpoch:    3,
		Leader: &LeaderLease{
			LeaseID:    "lease_ab",
			OwnerID:    "owner-1",
			OwnerEpoch: 3,
			AcquiredAt: acquired,
			RenewedAt:  acquired.Add(10 * time.Second),
			ExpiresAt:  acquired.Add(LeaseTTL),
		},
		CreatedAt:          acquired,
		UpdatedAt:          acquired.Add(10 * time.Second),
		LastRenewalEventAt: acquired.Add(10 * time.Second),
	}

	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"last_renewal_event_at"`) {
		t.Fatalf("last_renewal_event_at not persisted: %s", data)
	}
	var back ControllerRecord
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Leader == nil || back.Leader.LeaseID != rec.Leader.LeaseID ||
		!back.Leader.ExpiresAt.Equal(rec.Leader.ExpiresAt) ||
		!back.LastRenewalEventAt.Equal(rec.LastRenewalEventAt) ||
		back.Revision != rec.Revision || back.DesiredState != rec.DesiredState {
		t.Fatalf("round trip mismatch: %+v", back)
	}
	if back.PollCursors != nil {
		t.Fatalf("nil PollCursors should stay nil, got %+v", back.PollCursors)
	}

	// A non-nil empty PollCursors persists as {} and round-trips non-nil.
	rec.PollCursors = map[string]*PollCursor{}
	rec.Leader = nil
	rec.LastRenewalEventAt = time.Time{}
	data, err = json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal with poll cursors: %v", err)
	}
	if !strings.Contains(string(data), `"poll_cursors":{}`) {
		t.Fatalf("empty non-nil PollCursors should persist as {}: %s", data)
	}
	back = ControllerRecord{}
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal with poll cursors: %v", err)
	}
	if back.PollCursors == nil || len(back.PollCursors) != 0 {
		t.Fatalf("PollCursors did not round-trip: %+v", back.PollCursors)
	}
	if !back.LastRenewalEventAt.IsZero() {
		t.Fatalf("zero LastRenewalEventAt should round-trip zero, got %s", back.LastRenewalEventAt)
	}
}

func TestApplyEventSequences(t *testing.T) {
	now := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	rec := ControllerRecord{SchemaVersion: SchemaVersion, ControllerID: "alpha"}

	events := []ControllerEvent{
		{Kind: EventCreated, Seq: 1, Time: now, MaxInflight: 4},
		{Kind: EventEnabled, Seq: 2, Time: now.Add(time.Second)},
		{Kind: EventLeaderAcquired, Seq: 3, Time: now.Add(2 * time.Second), LeaseID: "lease_ab", OwnerID: "owner-1", OwnerEpoch: 1, ExpiresAt: now.Add(2*time.Second + LeaseTTL)},
		{Kind: EventLeaderRenewed, Seq: 4, Time: now.Add(12 * time.Second), LeaseID: "lease_ab", OwnerEpoch: 1, ExpiresAt: now.Add(12*time.Second + LeaseTTL)},
		{Kind: EventLeaderExpired, Seq: 5, Time: now.Add(60 * time.Second), Reason: "timeout"},
	}
	for _, e := range events {
		if err := applyEvent(&rec, e); err != nil {
			t.Fatalf("apply %s: %v", e.Kind, err)
		}
	}
	if rec.Revision != 5 || rec.MaxInflight != 4 || rec.DesiredState != DesiredEnabled {
		t.Fatalf("unexpected record state: %+v", rec)
	}
	if rec.Leader != nil {
		t.Fatalf("leader should be cleared by leader_expired: %+v", rec.Leader)
	}
	if rec.LastRenewalEventAt != events[3].Time {
		t.Fatalf("last renewal event time: got %s, want %s", rec.LastRenewalEventAt, events[3].Time)
	}

	if err := applyEvent(&rec, ControllerEvent{Kind: "bogus", Seq: 6, Time: now}); err == nil {
		t.Fatal("unknown event kind should error")
	}
}
