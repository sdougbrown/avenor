package mcpserver

import (
	"strings"
	"testing"
)

func TestNewRunRegistry(t *testing.T) {
	r := NewRunRegistry()
	if r == nil {
		t.Fatal("expected non-nil registry")
	}
	if len(r.All()) != 0 {
		t.Fatal("expected empty registry")
	}
}

func TestRunRegistryStore(t *testing.T) {
	r := NewRunRegistry()
	info := &RunInfo{
		RunID:        "run-1",
		Label:        "my-run",
		SupervisorID: "/tmp/sup.sock",
		RuntimeID:    "rt-1",
	}
	if err := r.Store(info); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(r.All()) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(r.All()))
	}

	// A different runtime ID for the same (supervisor, run) key is a collision.
	err := r.Store(&RunInfo{RunID: "run-1", SupervisorID: "/tmp/sup.sock", RuntimeID: "rt-other"})
	if err == nil {
		t.Fatal("expected error for conflicting runtime ID on same run key")
	}

	// Same run, same runtime: benign reuse that replaces the entry.
	if err := r.Store(&RunInfo{RunID: "run-1", Label: "renamed", SupervisorID: "/tmp/sup.sock", RuntimeID: "rt-1"}); err != nil {
		t.Fatalf("unexpected error on benign reuse: %v", err)
	}
	if len(r.All()) != 1 {
		t.Fatalf("expected 1 entry after reuse, got %d", len(r.All()))
	}
	if r.Lookup("/tmp/sup.sock", "run-1").Label != "renamed" {
		t.Fatal("expected benign reuse to replace the entry")
	}
	if r.LookupUnique("my-run") != nil {
		t.Fatal("expected old label mapping to be dropped on reuse")
	}
}

func TestRunRegistryLabelCollision(t *testing.T) {
	r := NewRunRegistry()
	if err := r.Store(&RunInfo{RunID: "run-1", Label: "shared", SupervisorID: "/tmp/a.sock", RuntimeID: "rt-1"}); err != nil {
		t.Fatal(err)
	}
	err := r.Store(&RunInfo{RunID: "run-2", Label: "shared", SupervisorID: "/tmp/b.sock", RuntimeID: "rt-2"})
	if err == nil || !strings.Contains(err.Error(), "label") {
		t.Fatalf("expected label collision error, got %v", err)
	}
	if r.Lookup("/tmp/b.sock", "run-2") != nil {
		t.Fatal("colliding run must not be stored")
	}
}

func TestRunRegistryStoreValidatesBeforeMutating(t *testing.T) {
	r := NewRunRegistry()
	if err := r.Store(&RunInfo{RunID: "run-a", Label: "a", SupervisorID: "/tmp/a.sock", RuntimeID: "rt-1"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Store(&RunInfo{RunID: "run-b", Label: "b", SupervisorID: "/tmp/b.sock", RuntimeID: "rt-2"}); err != nil {
		t.Fatal(err)
	}

	// Re-storing A with B's label is a collision and must not disturb A's
	// existing label mapping.
	err := r.Store(&RunInfo{RunID: "run-a", Label: "b", SupervisorID: "/tmp/a.sock", RuntimeID: "rt-1"})
	if err == nil || !strings.Contains(err.Error(), "label") {
		t.Fatalf("expected label collision error, got %v", err)
	}
	if ri := r.LookupUnique("a"); ri == nil || ri.RunID != "run-a" {
		t.Fatalf("A's label mapping was lost after a failed re-store: %#v", ri)
	}
	if ri := r.Lookup("/tmp/a.sock", "run-a"); ri == nil || ri.Label != "a" {
		t.Fatalf("A's entry was disturbed by a failed re-store: %#v", ri)
	}
	if ri := r.LookupUnique("b"); ri == nil || ri.RunID != "run-b" {
		t.Fatalf("B's label mapping was disturbed: %#v", ri)
	}

	// Re-storing with the same label is still benign reuse.
	if err := r.Store(&RunInfo{RunID: "run-a", Label: "a", SupervisorID: "/tmp/a.sock", RuntimeID: "rt-1"}); err != nil {
		t.Fatalf("same-label re-store failed: %v", err)
	}
	if ri := r.LookupUnique("a"); ri == nil || ri.RunID != "run-a" {
		t.Fatalf("same-label re-store lost the mapping: %#v", ri)
	}
}

func TestRunRegistryLookup(t *testing.T) {
	r := NewRunRegistry()
	r.Store(&RunInfo{RunID: "run-1", Label: "my-run", SupervisorID: "/tmp/a.sock"})
	r.Store(&RunInfo{RunID: "run-2", Label: "other-run", SupervisorID: "/tmp/a.sock"})

	if ri := r.Lookup("/tmp/a.sock", "run-1"); ri == nil || ri.Label != "my-run" {
		t.Fatal("scoped lookup by run_id failed")
	}
	if ri := r.Lookup("/tmp/b.sock", "run-1"); ri != nil {
		t.Fatal("entry from one supervisor must not leak to another")
	}
	if r.LookupUnique("my-run") == nil {
		t.Fatal("unique lookup by label failed")
	}
	if r.LookupUnique("nonexistent") != nil {
		t.Fatal("expected nil for unknown key")
	}
}

func TestRunRegistryLookupUniqueAmbiguousRunID(t *testing.T) {
	r := NewRunRegistry()
	r.Store(&RunInfo{RunID: "run-1", Label: "label-a", SupervisorID: "/tmp/a.sock"})
	r.Store(&RunInfo{RunID: "run-1", Label: "label-b", SupervisorID: "/tmp/b.sock"})

	if ri := r.LookupUnique("run-1"); ri != nil {
		t.Fatalf("expected nil for a run ID registered on two supervisors, got %#v", ri)
	}
	if ri := r.LookupUnique("label-a"); ri == nil || ri.SupervisorID != "/tmp/a.sock" {
		t.Fatalf("unique label lookup = %#v", ri)
	}
}

func TestRunRegistryRemove(t *testing.T) {
	r := NewRunRegistry()
	info := &RunInfo{RunID: "run-1", Label: "my-run", SupervisorID: "/tmp/a.sock"}
	r.Store(info)

	removed := r.Remove("/tmp/a.sock", "run-1")
	if removed == nil {
		t.Fatal("expected non-nil removed entry")
	}
	if len(r.All()) != 0 {
		t.Fatal("expected empty registry after remove")
	}
	if r.Lookup("/tmp/a.sock", "run-1") != nil {
		t.Fatal("expected nil after remove")
	}
	if r.LookupUnique("my-run") != nil {
		t.Fatal("expected nil label lookup after remove")
	}

	if r.Remove("/tmp/a.sock", "nonexistent") != nil {
		t.Fatal("expected nil for removing nonexistent key")
	}
	if r.Remove("/tmp/b.sock", "run-1") != nil {
		t.Fatal("expected nil for removing from unregistered supervisor")
	}
}

func TestRunRegistryAll(t *testing.T) {
	r := NewRunRegistry()
	r.Store(&RunInfo{RunID: "run-1", SupervisorID: "/tmp/a.sock"})
	r.Store(&RunInfo{RunID: "run-2", SupervisorID: "/tmp/a.sock"})
	r.Store(&RunInfo{RunID: "run-3", SupervisorID: "/tmp/b.sock"})

	all := r.All()
	if len(all) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(all))
	}
}

func TestRunRegistryLookupRuntimeIsScopedBySupervisor(t *testing.T) {
	r := NewRunRegistry()
	if err := r.Store(&RunInfo{RunID: "run-a", RuntimeID: "rt_1", Label: "alpha", SupervisorID: "/tmp/a.sock"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Store(&RunInfo{RunID: "run-b", RuntimeID: "rt_1", Label: "beta", SupervisorID: "/tmp/b.sock"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Store(&RunInfo{RunID: "run-c", RuntimeID: "rt_9", Label: "gamma", SupervisorID: "/tmp/b.sock"}); err != nil {
		t.Fatal(err)
	}

	if ri := r.LookupRuntime("/tmp/a.sock", "rt_1"); ri == nil || ri.RunID != "run-a" {
		t.Fatalf("runtime lookup on a.sock = %#v, want run-a", ri)
	}
	if ri := r.LookupRuntime("/tmp/b.sock", "rt_1"); ri == nil || ri.RunID != "run-b" {
		t.Fatalf("runtime lookup on b.sock = %#v, want run-b", ri)
	}
	if ri := r.LookupRuntime("/tmp/c.sock", "rt_1"); ri != nil {
		t.Fatalf("runtime lookup on an unregistered supervisor = %#v, want nil", ri)
	}
	if ri := r.LookupRuntime("/tmp/a.sock", ""); ri != nil {
		t.Fatalf("runtime lookup of an empty id = %#v, want nil", ri)
	}

	if ri := r.LookupRuntimeUnique("rt_1"); ri != nil {
		t.Fatalf("runtime id owned by two supervisors = %#v, want nil", ri)
	}
	if ri := r.LookupRuntimeUnique("rt_9"); ri == nil || ri.RunID != "run-c" {
		t.Fatalf("runtime id owned by one supervisor = %#v, want run-c", ri)
	}
	if ri := r.LookupRuntimeUnique(""); ri != nil {
		t.Fatalf("unique lookup of an empty id = %#v, want nil", ri)
	}

	// A supervisor restart mints rt_1 again while the pre-restart entry stays
	// registered, so two entries under one supervisor can share a runtime id.
	// That ambiguity answers nil rather than whichever entry the map iteration
	// reached first.
	if err := r.Store(&RunInfo{RunID: "run-d", RuntimeID: "rt_7", SupervisorID: "/tmp/a.sock"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Store(&RunInfo{RunID: "run-e", RuntimeID: "rt_7", SupervisorID: "/tmp/a.sock"}); err != nil {
		t.Fatal(err)
	}
	if ri := r.LookupRuntime("/tmp/a.sock", "rt_7"); ri != nil {
		t.Fatalf("runtime id owned by two runs under one supervisor = %#v, want nil", ri)
	}
}
