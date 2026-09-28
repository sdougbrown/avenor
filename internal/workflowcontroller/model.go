package workflowcontroller

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SchemaVersion is the schema version stamped on every controller record.
const SchemaVersion = 1

// DesiredState is the operator-declared desired state of a controller.
type DesiredState string

const (
	// DesiredEnabled lets the controller acquire leadership and dispatch work.
	DesiredEnabled DesiredState = "enabled"
	// DesiredDisabled keeps the controller inert: no leadership, no dispatch.
	DesiredDisabled DesiredState = "disabled"
)

// Controller event kinds appended to events.ndjson. The seq of an event is
// its 1-based position in the per-controller log; the snapshot's Revision is
// the last applied seq.
const (
	EventCreated        = "created"
	EventEnabled        = "enabled"
	EventDisabled       = "disabled"
	EventLeaderAcquired = "leader_acquired"
	EventLeaderRenewed  = "leader_renewed"
	EventLeaderReleased = "leader_released"
	EventLeaderExpired  = "leader_expired"
)

// Sentinel errors returned by the controller store.
var (
	// ErrNotFound is returned when a controller does not exist.
	ErrNotFound = errors.New("controller not found")
	// ErrConflict is returned on a stale compare-and-swap (wrong lease id or
	// owner epoch) or a Create against an existing, differing controller.
	ErrConflict = errors.New("controller state conflict")
	// ErrInvalidController is returned for a malformed controller id or a
	// non-positive max_inflight.
	ErrInvalidController = errors.New("invalid controller")
	// ErrDisabled is returned when leadership is requested while the
	// controller's desired state is disabled.
	ErrDisabled = errors.New("controller disabled")
	// ErrNotLeader is returned when the caller's (lease id, owner epoch) pair
	// is not the current unexpired leader lease of an enabled controller.
	ErrNotLeader = errors.New("not the current leader")
)

// LeaderLease is the leadership lease held by one controller owner.
type LeaderLease struct {
	LeaseID    string    `json:"lease_id"`
	OwnerID    string    `json:"owner_id"`
	OwnerEpoch int64     `json:"owner_epoch"`
	AcquiredAt time.Time `json:"acquired_at"`
	RenewedAt  time.Time `json:"renewed_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// ControllerRecord is the authoritative state of one controller, persisted as
// controller.json and advanced by the events in events.ndjson.
type ControllerRecord struct {
	SchemaVersion int
	ControllerID  string
	DesiredState  DesiredState
	MaxInflight   int
	Revision      int64
	OwnerEpoch    int64
	Leader        *LeaderLease
	// PollCursors is reserved for future per-poll checkpoints and is always
	// nil or empty in this stage; it round-trips through the snapshot.
	PollCursors    map[string]json.RawMessage
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DisabledReason string

	// LastRenewalEventAt is the wall-clock time of the last leader_renewed
	// event appended for the current lease; it throttles renewal events to at
	// most one per minute while renewals themselves still extend the lease.
	LastRenewalEventAt time.Time `json:"last_renewal_event_at"`
}

// ControllerEvent is one line of a controller's events.ndjson log.
type ControllerEvent struct {
	Kind        string    `json:"kind"`
	Seq         int64     `json:"seq"`
	Time        time.Time `json:"time"`
	MaxInflight int       `json:"max_inflight,omitempty"`
	Reason      string    `json:"reason,omitempty"`
	LeaseID     string    `json:"lease_id,omitempty"`
	OwnerID     string    `json:"owner_id,omitempty"`
	OwnerEpoch  int64     `json:"owner_epoch,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

// LastRenewalEventAt is exported on ControllerRecord; the custom
// MarshalJSON/UnmarshalJSON below omit the zero value and round-trip it.
type controllerRecordJSON struct {
	SchemaVersion      int                         `json:"schema_version"`
	ControllerID       string                      `json:"controller_id"`
	DesiredState       DesiredState                `json:"desired_state"`
	MaxInflight        int                         `json:"max_inflight"`
	Revision           int64                       `json:"revision"`
	OwnerEpoch         int64                       `json:"owner_epoch"`
	Leader             *LeaderLease                `json:"leader,omitempty"`
	PollCursors        *map[string]json.RawMessage `json:"poll_cursors,omitempty"`
	CreatedAt          time.Time                   `json:"created_at"`
	UpdatedAt          time.Time                   `json:"updated_at"`
	DisabledReason     string                      `json:"disabled_reason,omitempty"`
	LastRenewalEventAt *time.Time                  `json:"last_renewal_event_at,omitempty"`
}

// MarshalJSON writes the record with an empty-but-present poll_cursors object
// when PollCursors is non-nil, and omits it entirely when nil.
func (r ControllerRecord) MarshalJSON() ([]byte, error) {
	a := controllerRecordJSON{
		SchemaVersion:  r.SchemaVersion,
		ControllerID:   r.ControllerID,
		DesiredState:   r.DesiredState,
		MaxInflight:    r.MaxInflight,
		Revision:       r.Revision,
		OwnerEpoch:     r.OwnerEpoch,
		Leader:         r.Leader,
		CreatedAt:      r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
		DisabledReason: r.DisabledReason,
	}
	if r.PollCursors != nil {
		a.PollCursors = &r.PollCursors
	}
	if !r.LastRenewalEventAt.IsZero() {
		t := r.LastRenewalEventAt
		a.LastRenewalEventAt = &t
	}
	return json.Marshal(a)
}

// UnmarshalJSON reads a record written by MarshalJSON.
func (r *ControllerRecord) UnmarshalJSON(data []byte) error {
	var a controllerRecordJSON
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*r = ControllerRecord{
		SchemaVersion:  a.SchemaVersion,
		ControllerID:   a.ControllerID,
		DesiredState:   a.DesiredState,
		MaxInflight:    a.MaxInflight,
		Revision:       a.Revision,
		OwnerEpoch:     a.OwnerEpoch,
		Leader:         a.Leader,
		CreatedAt:      a.CreatedAt,
		UpdatedAt:      a.UpdatedAt,
		DisabledReason: a.DisabledReason,
	}
	if a.PollCursors != nil {
		r.PollCursors = *a.PollCursors
	}
	if a.LastRenewalEventAt != nil {
		r.LastRenewalEventAt = *a.LastRenewalEventAt
	}
	return nil
}

// validateControllerID enforces the controller id charset: non-empty,
// [A-Za-z0-9._-] only, and no leading dot (which would hide the directory).
func validateControllerID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: controller id must be non-empty", ErrInvalidController)
	}
	if strings.HasPrefix(id, ".") {
		return fmt.Errorf("%w: controller id %q must not start with a dot", ErrInvalidController, id)
	}
	for _, r := range id {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
		default:
			return fmt.Errorf("%w: controller id %q contains invalid character %q", ErrInvalidController, id, r)
		}
	}
	return nil
}

// applyEvent reduces one event into rec. Only events with Seq greater than the
// record's Revision are applied during replay.
func applyEvent(rec *ControllerRecord, e ControllerEvent) error {
	rec.Revision = e.Seq
	rec.UpdatedAt = e.Time
	switch e.Kind {
	case EventCreated:
		rec.DesiredState = DesiredDisabled
		rec.MaxInflight = e.MaxInflight
		rec.DisabledReason = ""
		rec.Leader = nil
		rec.OwnerEpoch = 0
		rec.LastRenewalEventAt = time.Time{}
	case EventEnabled:
		rec.DesiredState = DesiredEnabled
		rec.DisabledReason = ""
	case EventDisabled:
		rec.DesiredState = DesiredDisabled
		rec.DisabledReason = e.Reason
	case EventLeaderAcquired:
		rec.OwnerEpoch = e.OwnerEpoch
		rec.LastRenewalEventAt = time.Time{}
		rec.Leader = &LeaderLease{
			LeaseID:    e.LeaseID,
			OwnerID:    e.OwnerID,
			OwnerEpoch: e.OwnerEpoch,
			AcquiredAt: e.Time,
			RenewedAt:  e.Time,
			ExpiresAt:  e.ExpiresAt,
		}
	case EventLeaderRenewed:
		if rec.Leader != nil {
			rec.Leader.RenewedAt = e.Time
			rec.Leader.ExpiresAt = e.ExpiresAt
		}
		rec.LastRenewalEventAt = e.Time
	case EventLeaderReleased, EventLeaderExpired:
		rec.Leader = nil
	default:
		return fmt.Errorf("unknown controller event kind %q", e.Kind)
	}
	return nil
}
