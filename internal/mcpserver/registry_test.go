package mcpserver

import (
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
		RunID: "run-1",
		Label: "my-run",
	}
	r.Store(info)

	if len(r.All()) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(r.All()))
	}

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for duplicate run_id")
		}
	}()
	r.Store(&RunInfo{RunID: "run-1", Label: "other"})
}

func TestRunRegistryLookup(t *testing.T) {
	r := NewRunRegistry()
	r.Store(&RunInfo{RunID: "run-1", Label: "my-run"})
	r.Store(&RunInfo{RunID: "run-2", Label: "other-run"})

	if ri := r.Lookup("run-1"); ri == nil || ri.Label != "my-run" {
		t.Fatal("lookup by run_id failed")
	}
	if ri := r.Lookup("my-run"); ri == nil || ri.RunID != "run-1" {
		t.Fatal("lookup by label failed")
	}
	if ri := r.Lookup("nonexistent"); ri != nil {
		t.Fatal("expected nil for unknown key")
	}
}

func TestRunRegistryRemove(t *testing.T) {
	r := NewRunRegistry()
	info := &RunInfo{RunID: "run-1", Label: "my-run"}
	r.Store(info)

	removed := r.Remove("run-1")
	if removed == nil {
		t.Fatal("expected non-nil removed entry")
	}
	if len(r.All()) != 0 {
		t.Fatal("expected empty registry after remove")
	}
	if r.Lookup("run-1") != nil {
		t.Fatal("expected nil after remove")
	}
	if r.Lookup("my-run") != nil {
		t.Fatal("expected nil label lookup after remove")
	}

	if r.Remove("nonexistent") != nil {
		t.Fatal("expected nil for removing nonexistent key")
	}
}

func TestRunRegistryAll(t *testing.T) {
	r := NewRunRegistry()
	r.Store(&RunInfo{RunID: "run-1"})
	r.Store(&RunInfo{RunID: "run-2"})
	r.Store(&RunInfo{RunID: "run-3"})

	all := r.All()
	if len(all) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(all))
	}
}
