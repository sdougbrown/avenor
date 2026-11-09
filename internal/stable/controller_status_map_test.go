package stable

import (
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// TestControllerStatusMapDispatchBackoffUntil asserts the status RPC
// visibility contract for the runner's dispatch backoff: a held retry
// time is surfaced to the operator, and a cleared backoff reports nil.
func TestControllerStatusMapDispatchBackoffUntil(t *testing.T) {
	rec := workflowcontroller.ControllerRecord{ControllerID: "c1"}
	next := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

	t.Run("surfaces a held backoff", func(t *testing.T) {
		runner := &workflowcontroller.RunnerStatus{DispatchBackoffUntil: next}
		got := controllerStatusMap(rec, "self", runner, nil)
		if v, ok := got["dispatch_backoff_until"].(time.Time); !ok || !v.Equal(next) {
			t.Fatalf("dispatch_backoff_until = %#v, want %v", got["dispatch_backoff_until"], next)
		}
	})

	t.Run("nil when no backoff is held", func(t *testing.T) {
		runner := &workflowcontroller.RunnerStatus{}
		got := controllerStatusMap(rec, "self", runner, nil)
		// Key presence is part of the RPC shape: a dropped entry
		// would render differently than an explicit null, so the
		// assertion must pin the key existing with a nil value
		// (a bare != nil check is satisfied by an absent key too).
		v, ok := got["dispatch_backoff_until"]
		if !ok {
			t.Fatal("dispatch_backoff_until key missing from the status map; want present with a nil value")
		}
		if v != nil {
			t.Fatalf("dispatch_backoff_until = %#v, want nil", v)
		}
	})
}
