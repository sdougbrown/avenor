package control

import (
	"encoding/json"
	"testing"

	"github.com/sdougbrown/avenor/internal/admission"
)

func spawnTestResponse(t *testing.T, spawnErr error) Response {
	t.Helper()
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{spawnErr: spawnErr})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(s.Stop)
	c := mustDial(t, path)
	t.Cleanup(func() { _ = c.Close() })
	params, _ := json.Marshal(map[string]any{"prompt": "hello", "dir": "/tmp"})
	if err := writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "spawn", Params: params}); err != nil {
		t.Fatalf("write spawn: %v", err)
	}
	return readResp(t, c)
}

// TestSpawnIdempotencyConflictErrorMapped: a reused idempotency key with
// different parameters maps to the app-range -32031, distinct from the
// -32602 used by param-validation failures.
func TestSpawnIdempotencyConflictErrorMapped(t *testing.T) {
	r := spawnTestResponse(t, &IdempotencyConflictError{Key: "key_a"})
	if r.Error == nil {
		t.Fatal("expected spawn error")
	}
	if r.Error.Code != -32031 {
		t.Fatalf("error code = %d, want -32031", r.Error.Code)
	}
	if r.Error.Message != "idempotency key reused with different parameters" {
		t.Fatalf("error message = %q, want the conflict message", r.Error.Message)
	}
}

// TestSpawnIdempotencyCapacityErrorMapped: a full idempotency store maps to
// -32030.
func TestSpawnIdempotencyCapacityErrorMapped(t *testing.T) {
	r := spawnTestResponse(t, &IdempotencyCapacityError{Key: "key_b", Capacity: 1})
	if r.Error == nil {
		t.Fatal("expected spawn error")
	}
	if r.Error.Code != -32030 {
		t.Fatalf("error code = %d, want -32030", r.Error.Code)
	}
	if r.Error.Message != "idempotency capacity exhausted" {
		t.Fatalf("error message = %q, want the capacity message", r.Error.Message)
	}
}

// TestSpawnAdmissionCapacityErrorStillMapped (regression): an admission
// capacity error still maps to -32050 with its structured data fields.
func TestSpawnAdmissionCapacityErrorStillMapped(t *testing.T) {
	r := spawnTestResponse(t, &admission.CapacityError{Source: "tree", Limit: 3, Active: 3, RootID: "root_x"})
	if r.Error == nil {
		t.Fatal("expected spawn error")
	}
	if r.Error.Code != -32050 {
		t.Fatalf("error code = %d, want -32050", r.Error.Code)
	}
	data, ok := r.Error.Data.(map[string]any)
	if !ok {
		t.Fatalf("error data type: %T", r.Error.Data)
	}
	if data["source"] != "tree" {
		t.Fatalf("source = %v, want tree", data["source"])
	}
	if data["retryable"] != true {
		t.Fatalf("retryable = %v, want true", data["retryable"])
	}
	if data["root_id"] != "root_x" {
		t.Fatalf("root_id = %v, want root_x", data["root_id"])
	}
	if data["limit"] != float64(3) || data["active"] != float64(3) {
		t.Fatalf("limit/active = %#v/%#v, want 3/3", data["limit"], data["active"])
	}
}
