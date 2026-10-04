package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/stable"
)

// --- Stage 6: idempotent spawn (MCP server half) ---
//
// The MCP server forwards an idempotency key to the supervisor and re-derives
// the run identity from the returned result. These tests drive the MCP server
// against a fake supervisor that emulates the stable supervisor's idempotency
// gate (stored hits, hash conflicts, and in-flight reservations) so the
// server-side contract is verified without a real runtime.

// fakeIdempotentSupervisor emulates the stable supervisor's idempotency gate
// for the MCP spawn/follow-up path. It keeps a per-key store of (hash, result)
// plus in-flight reservations so concurrent duplicate spawns observe the
// holder's single result.
type fakeIdempotentSupervisor struct {
	mu       sync.Mutex
	entries  map[string]fakeIdemEntry
	inflight map[string]*fakeIdemFlight
	nextRT   int
	runtimes int
	// spawnCalls counts Spawn invocations, one per attempt that reached the
	// supervisor (hits, waiters, and rejected conflicts included).
	spawnCalls int
	listRuns   []map[string]any
	// gate, when non-nil, blocks the holder's doSpawn until the test closes
	// it, so concurrent duplicates overlap the holder's in-flight spawn
	// instead of serializing into completed-cache hits.
	gate chan struct{}
	// waiterSignal, when non-nil, receives one token per waiter just before
	// it blocks on a flight's done channel, letting tests order a release
	// after waiters are parked.
	waiterSignal chan<- struct{}
}

type fakeIdemEntry struct {
	result map[string]any
	hash   string
}

type fakeIdemFlight struct {
	hash   string
	done   chan struct{}
	result map[string]any
	err    error
}

func newFakeIdempotentSupervisor() *fakeIdempotentSupervisor {
	return &fakeIdempotentSupervisor{
		entries:  map[string]fakeIdemEntry{},
		inflight: map[string]*fakeIdemFlight{},
	}
}

func (f *fakeIdempotentSupervisor) Spawn(params map[string]any) (map[string]any, error) {
	f.mu.Lock()
	f.spawnCalls++
	f.mu.Unlock()
	key, _ := params["idempotency_key"].(string)
	if key == "" {
		return f.doSpawn(params), nil
	}
	// Marshal to JSON and unmarshal into the typed SpawnParams, mirroring the
	// real supervisor's map->JSON->SpawnParams path, then hash with the real
	// stable.IdempotencyHash so the fake and production share one exclusion set.
	b, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	var sp stable.SpawnParams
	if err := json.Unmarshal(b, &sp); err != nil {
		return nil, err
	}
	hash, err := stable.IdempotencyHash(sp)
	if err != nil {
		return nil, err
	}
	result, flight, err := f.begin(key, hash)
	if err != nil {
		return nil, err
	}
	if flight == nil {
		return result, nil
	}
	if f.gate != nil {
		<-f.gate
	}
	res := f.doSpawn(params)
	f.commit(key, flight, res)
	return res, nil
}

func (f *fakeIdempotentSupervisor) begin(key, hash string) (map[string]any, *fakeIdemFlight, error) {
	f.mu.Lock()
	if e, ok := f.entries[key]; ok {
		if e.hash != hash {
			f.mu.Unlock()
			return nil, nil, errors.New("idempotency key reused with different parameters")
		}
		f.mu.Unlock()
		return e.result, nil, nil
	}
	if fl, ok := f.inflight[key]; ok {
		if fl.hash != hash {
			f.mu.Unlock()
			return nil, nil, errors.New("idempotency key reused with different parameters")
		}
		if f.waiterSignal != nil {
			f.waiterSignal <- struct{}{}
		}
		f.mu.Unlock()
		<-fl.done
		return fl.result, nil, fl.err
	}
	fl := &fakeIdemFlight{hash: hash, done: make(chan struct{})}
	f.inflight[key] = fl
	f.mu.Unlock()
	return nil, fl, nil
}

func (f *fakeIdempotentSupervisor) commit(key string, fl *fakeIdemFlight, result map[string]any) {
	f.mu.Lock()
	fl.result = result
	f.entries[key] = fakeIdemEntry{result: result, hash: fl.hash}
	if cur, ok := f.inflight[key]; ok && cur == fl {
		delete(f.inflight, key)
	}
	f.mu.Unlock()
	close(fl.done)
}

// doSpawn "spawns" a new runtime, recording a list entry so a fresh MCP server
// can rehydrate the run. It returns the runtime's result with the caller's
// attempt-local artifact paths, mirroring the stable supervisor.
func (f *fakeIdempotentSupervisor) doSpawn(params map[string]any) map[string]any {
	f.mu.Lock()
	f.nextRT++
	rt := f.nextRT
	f.runtimes++
	label, _ := params["label"].(string)
	sentinel, _ := params["sentinel_file"].(string)
	onEvent, _ := params["on_event"].(string)
	f.listRuns = append(f.listRuns, map[string]any{
		"runtime_id":    fmt.Sprintf("rt_%d", rt),
		"session_id":    fmt.Sprintf("ses_%d", rt),
		"label":         label,
		"sentinel_file": sentinel,
		"on_event":      onEvent,
		"dir":           params["dir"],
	})
	f.mu.Unlock()
	return map[string]any{
		"runtime_id":    fmt.Sprintf("rt_%d", rt),
		"session_id":    fmt.Sprintf("ses_%d", rt),
		"sentinel_file": sentinel,
		"on_event":      onEvent,
	}
}

// RuntimeCount reports how many runtimes the fake has spawned.
func (f *fakeIdempotentSupervisor) RuntimeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runtimes
}

// SpawnCallCount reports how many spawn attempts reached the supervisor.
func (f *fakeIdempotentSupervisor) SpawnCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.spawnCalls
}

func (f *fakeIdempotentSupervisor) List() ([]map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, len(f.listRuns))
	copy(out, f.listRuns)
	return out, nil
}

func (f *fakeIdempotentSupervisor) Status(runtimeID string) (map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.listRuns {
		if e["runtime_id"] == runtimeID {
			return map[string]any{
				"runtime_id": runtimeID,
				"session_id": e["session_id"],
				"status":     "running",
			}, nil
		}
	}
	return nil, fmt.Errorf("runtime %q not found", runtimeID)
}

// --- ControlClient no-op methods ---

func (f *fakeIdempotentSupervisor) Shutdown(mode string) error { return nil }
func (f *fakeIdempotentSupervisor) Close() error               { return nil }
func (f *fakeIdempotentSupervisor) Closed() bool               { return false }
func (f *fakeIdempotentSupervisor) AnswerPermission(runtimeID, requestID, optionID string) error {
	return nil
}
func (f *fakeIdempotentSupervisor) WorkflowStatus(workflowID string) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) WorkflowWait(workflowID string, timeout time.Duration) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) WorkflowInspect(workflowID string) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) WorkflowEvents(workflowID string, afterSeq int64, limit int) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) WorkflowComplete(workflowID string, fields map[string]any) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) WorkflowGate(workflowID string, fields map[string]any) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) WorkflowControllerStatus(controllerID string) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) WorkflowControllerList() (map[string]any, error) {
	return nil, nil
}

// newIdempotencyServer builds an MCP server against a shared fake supervisor.
func newIdempotencyServer(t *testing.T, sup *fakeIdempotentSupervisor) *Server {
	t.Helper()
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: sup})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestIdempotencySpawnRetryReturnsOriginalRun(t *testing.T) {
	sup := newFakeIdempotentSupervisor()

	// First server: original spawn.
	s1 := newIdempotencyServer(t, sup)
	_, r1, err := s1.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	m1 := r1.(map[string]any)
	runID1, _ := m1["run_id"].(string)
	label1, _ := m1["label"].(string)
	if runID1 == "" {
		t.Fatal("expected non-empty run_id")
	}
	if label1 != runID1 {
		t.Fatalf("derived label = %v, want the original run ID %s", label1, runID1)
	}

	// Second server (fresh registry): retry the same key + params.
	s2 := newIdempotencyServer(t, sup)
	_, r2, err := s2.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	m2 := r2.(map[string]any)
	runID2, _ := m2["run_id"].(string)
	label2, _ := m2["label"].(string)

	// Both responses identify the original run.
	if runID2 != runID1 {
		t.Fatalf("retry run_id = %s, want the original %s", runID2, runID1)
	}
	if label2 != label1 {
		t.Fatalf("retry label = %s, want the original %s", label2, label1)
	}
	if m1["supervisor_id"] != m2["supervisor_id"] {
		t.Fatalf("supervisor_id mismatch: %v vs %v", m1["supervisor_id"], m2["supervisor_id"])
	}

	// Exactly one runtime was spawned (the retry was a stored hit), and both
	// keyed attempts reached the supervisor's Spawn.
	if n := sup.RuntimeCount(); n != 1 {
		t.Fatalf("runtimes = %d, want 1 (a retry must not spawn a second)", n)
	}
	if n := sup.SpawnCallCount(); n != 2 {
		t.Fatalf("supervisor Spawn calls = %d, want 2 (both attempts reached the supervisor)", n)
	}

	// A same-server retry (third attempt on s2) is also a stored hit that
	// reaches the supervisor.
	_, r3, err := s2.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if r3.(map[string]any)["run_id"] != runID1 {
		t.Fatalf("same-server retry run_id = %v, want the original %s", r3.(map[string]any)["run_id"], runID1)
	}
	if n := sup.SpawnCallCount(); n != 3 {
		t.Fatalf("supervisor Spawn calls = %d, want 3 after the same-server retry", n)
	}
	if n := sup.RuntimeCount(); n != 1 {
		t.Fatalf("runtimes = %d, want 1 after the same-server retry", n)
	}

	// The second server's registry records the original run's artifact paths,
	// not the retry's freshly generated temp paths.
	ri := s2.registry.LookupUnique(runID2)
	if ri == nil {
		t.Fatal("expected the retry's registry entry for the original run")
	}
	if got := filepath.Base(ri.SentinelPath); got != "avenor-run-"+runID1+".done" {
		t.Fatalf("retry sentinel basename = %s, want the original run's avenor-run-%s.done", got, runID1)
	}
	if got := filepath.Base(ri.EventLogPath); got != "avenor-run-"+runID1+".log" {
		t.Fatalf("retry event log basename = %s, want the original run's avenor-run-%s.log", got, runID1)
	}
	// The registry entry carries the original run's identity (label, runtime,
	// session), not the retry's.
	if ri.Label != runID1 {
		t.Fatalf("retry registry label = %s, want the original run ID %s", ri.Label, runID1)
	}
	if ri.RuntimeID != "rt_1" {
		t.Fatalf("retry registry runtime = %s, want the original rt_1", ri.RuntimeID)
	}
	if ri.SessionID != "ses_1" {
		t.Fatalf("retry registry session = %s, want the original ses_1", ri.SessionID)
	}
}

func TestIdempotencySpawnRetryExplicitLabelKept(t *testing.T) {
	sup := newFakeIdempotentSupervisor()

	// An explicit label is a semantic parameter: it stays in the hash and is
	// preserved on a retry (not replaced by the original run's ID).
	s1 := newIdempotencyServer(t, sup)
	_, r1, err := s1.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		Label:          "my-label",
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	m1 := r1.(map[string]any)
	if m1["label"] != "my-label" {
		t.Fatalf("original label = %v, want my-label", m1["label"])
	}

	s2 := newIdempotencyServer(t, sup)
	_, r2, err := s2.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		Label:          "my-label",
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	m2 := r2.(map[string]any)
	if m2["label"] != "my-label" {
		t.Fatalf("retry label = %v, want my-label (an explicit label is kept)", m2["label"])
	}
	if m2["run_id"] != m1["run_id"] {
		t.Fatalf("retry run_id = %v, want the original %v", m2["run_id"], m1["run_id"])
	}
	if n := sup.RuntimeCount(); n != 1 {
		t.Fatalf("runtimes = %d, want 1", n)
	}
	if n := sup.SpawnCallCount(); n != 2 {
		t.Fatalf("supervisor Spawn calls = %d, want 2 (both attempts reached the supervisor)", n)
	}
}

func TestIdempotencySpawnRetryHashMismatchFails(t *testing.T) {
	sup := newFakeIdempotentSupervisor()

	s1 := newIdempotencyServer(t, sup)
	if _, _, err := s1.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		Prompt:         "original prompt",
		IdempotencyKey: "k1",
	}); err != nil {
		t.Fatal(err)
	}

	// Same key, different prompt: the supervisor rejects the retry.
	s2 := newIdempotencyServer(t, sup)
	_, _, err := s2.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		Prompt:         "different prompt",
		IdempotencyKey: "k1",
	})
	if err == nil {
		t.Fatal("expected a hash-mismatch error on the retry")
	}
	if !strings.Contains(err.Error(), "idempotency key reused with different parameters") {
		t.Fatalf("error = %v, want the idempotency conflict message", err)
	}
	if n := sup.RuntimeCount(); n != 1 {
		t.Fatalf("runtimes = %d, want 1 (a conflicting retry must not spawn)", n)
	}
	// The conflicting retry still reached the supervisor's Spawn and was
	// rejected there; no runtime was started for it.
	if n := sup.SpawnCallCount(); n != 2 {
		t.Fatalf("supervisor Spawn calls = %d, want 2 (the conflicting retry reached the supervisor)", n)
	}
}

// TestIdempotencyKeyLengthValidation: a key longer than 256 bytes is rejected
// at the MCP param level, before any spawn reaches the supervisor; a 256-byte
// key is accepted.
func TestIdempotencyKeyLengthValidation(t *testing.T) {
	sup := newFakeIdempotentSupervisor()
	s := newIdempotencyServer(t, sup)
	longKey := strings.Repeat("k", 257)
	if _, _, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		IdempotencyKey: longKey,
	}); err == nil {
		t.Fatal("expected a key-length error for a 257-byte key")
	} else if !strings.Contains(err.Error(), "idempotency_key too long") {
		t.Fatalf("error = %v, want the key-length message", err)
	}
	if n := sup.SpawnCallCount(); n != 0 {
		t.Fatalf("supervisor Spawn calls = %d, want 0 (a too-long key must be rejected pre-spawn)", n)
	}

	// A 256-byte key is accepted and reaches the supervisor.
	sup2 := newFakeIdempotentSupervisor()
	s2 := newIdempotencyServer(t, sup2)
	maxKey := strings.Repeat("k", 256)
	if _, _, err := s2.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		IdempotencyKey: maxKey,
	}); err != nil {
		t.Fatalf("a 256-byte key should be accepted: %v", err)
	}
	if n := sup2.SpawnCallCount(); n != 1 {
		t.Fatalf("supervisor Spawn calls = %d, want 1 (a 256-byte key reaches the supervisor)", n)
	}
}

func TestIdempotencyFollowUpRetrySequential(t *testing.T) {
	sup := newFakeIdempotentSupervisor()

	// Seed a parent run.
	s0 := newIdempotencyServer(t, sup)
	_, parent, err := s0.handleAvenorSpawn(context.Background(), nil, spawnArgs{RepoDir: "/tmp/repo"})
	if err != nil {
		t.Fatal(err)
	}
	parentID, _ := parent.(map[string]any)["run_id"].(string)

	// First follow-up with a key.
	s1 := newIdempotencyServer(t, sup)
	_, r1, err := s1.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:          parentID,
		Message:        "continue",
		IdempotencyKey: "fk",
	})
	if err != nil {
		t.Fatal(err)
	}
	m1 := r1.(map[string]any)
	fuID1, _ := m1["run_id"].(string)
	if fuID1 == "" {
		t.Fatal("expected non-empty follow-up run_id")
	}

	// Retry the follow-up with the same key (fresh server).
	s2 := newIdempotencyServer(t, sup)
	_, r2, err := s2.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:          parentID,
		Message:        "continue",
		IdempotencyKey: "fk",
	})
	if err != nil {
		t.Fatal(err)
	}
	m2 := r2.(map[string]any)
	if m2["run_id"] != fuID1 {
		t.Fatalf("retry follow-up run_id = %v, want the original %v", m2["run_id"], fuID1)
	}
	if m2["label"] != m1["label"] {
		t.Fatalf("retry follow-up label = %v, want the original %v", m2["label"], m1["label"])
	}
	// The registry records the follow-up's artifact paths, not the retry's
	// freshly generated temp paths.
	ri := s2.registry.LookupUnique(fuID1)
	if ri == nil {
		t.Fatal("expected the follow-up's registry entry")
	}
	if got, want := ri.SentinelPath, filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.done", fuID1)); got != want {
		t.Fatalf("follow-up sentinel = %s, want %s", got, want)
	}
	if got, want := ri.EventLogPath, filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.log", fuID1)); got != want {
		t.Fatalf("follow-up event log = %s, want %s", got, want)
	}
	// One parent runtime + one follow-up runtime = 2 total; the retry was a
	// hit that still reached the supervisor.
	if n := sup.RuntimeCount(); n != 2 {
		t.Fatalf("runtimes = %d, want 2 (parent + one follow-up)", n)
	}
	if n := sup.SpawnCallCount(); n != 3 {
		t.Fatalf("supervisor Spawn calls = %d, want 3 (parent + both follow-up attempts)", n)
	}

	// A same-server retry (third follow-up attempt on s2) is also a stored
	// hit that reaches the supervisor.
	_, r3, err := s2.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:          parentID,
		Message:        "continue",
		IdempotencyKey: "fk",
	})
	if err != nil {
		t.Fatal(err)
	}
	if r3.(map[string]any)["run_id"] != fuID1 {
		t.Fatalf("same-server retry follow-up run_id = %v, want the original %s", r3.(map[string]any)["run_id"], fuID1)
	}
	if n := sup.SpawnCallCount(); n != 4 {
		t.Fatalf("supervisor Spawn calls = %d, want 4 after the same-server retry", n)
	}
	if n := sup.RuntimeCount(); n != 2 {
		t.Fatalf("runtimes = %d, want 2 after the same-server retry", n)
	}
}

func TestIdempotencyFollowUpRetryConcurrent(t *testing.T) {
	sup := newFakeIdempotentSupervisor()
	sup.gate = make(chan struct{})

	// Seed a parent run.
	s0 := newIdempotencyServer(t, sup)
	_, parent, err := s0.handleAvenorSpawn(context.Background(), nil, spawnArgs{RepoDir: "/tmp/repo"})
	if err != nil {
		t.Fatal(err)
	}
	parentID, _ := parent.(map[string]any)["run_id"].(string)

	// Two concurrent follow-ups with the same key: both observe the holder's
	// single result and only one runtime is spawned. The holder's follow-up
	// spawn is held on the gate until the other parks inside the in-flight
	// wait, so the second attempt exercises the waiter path rather than
	// serializing into a completed-cache hit.
	parked := make(chan struct{}, 1)
	sup.waiterSignal = parked

	var wg sync.WaitGroup
	results := make([]map[string]any, 2)
	errs := make([]error, 2)
	servers := []*Server{newIdempotencyServer(t, sup), newIdempotencyServer(t, sup)}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := servers[i]
			_, r, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
				RunID:          parentID,
				Message:        "continue",
				IdempotencyKey: "fk",
			})
			if err != nil {
				errs[i] = err
				return
			}
			results[i] = r.(map[string]any)
		}(i)
	}
	// Wait for the second attempt to park inside the in-flight wait before
	// releasing the holder.
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("the second follow-up did not park inside the in-flight wait")
	}
	close(sup.gate)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent follow-up %d: %v", i, err)
		}
	}
	id0, _ := results[0]["run_id"].(string)
	id1, _ := results[1]["run_id"].(string)
	if id0 == "" || id1 == "" {
		t.Fatalf("expected non-empty run_ids, got %v / %v", id0, id1)
	}
	if id0 != id1 {
		t.Fatalf("concurrent follow-ups returned different run_ids: %s vs %s", id0, id1)
	}
	// One parent runtime + one follow-up runtime = 2 total; both keyed
	// attempts reached the supervisor.
	if n := sup.RuntimeCount(); n != 2 {
		t.Fatalf("runtimes = %d, want 2 (parent + one follow-up)", n)
	}
	if n := sup.SpawnCallCount(); n != 3 {
		t.Fatalf("supervisor Spawn calls = %d, want 3 (parent + both follow-up attempts)", n)
	}
	// The registry records the follow-up's artifact paths, not the retry's
	// freshly generated temp paths.
	ri := servers[0].registry.LookupUnique(id0)
	if ri == nil {
		t.Fatal("expected the follow-up's registry entry")
	}
	if got, want := ri.SentinelPath, filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.done", id0)); got != want {
		t.Fatalf("follow-up sentinel = %s, want %s", got, want)
	}
	if got, want := ri.EventLogPath, filepath.Join(os.TempDir(), fmt.Sprintf("avenor-run-%s.log", id0)); got != want {
		t.Fatalf("follow-up event log = %s, want %s", got, want)
	}
}

func TestIdempotencySpawnAndFollowUpKeysDoNotCollide(t *testing.T) {
	// (a) A follow-up with key k must not be treated as a retry of a spawn with
	// key k: the "spawn:" / "follow_up:" prefixes keep the key spaces apart.
	t.Run("spawn k then follow-up k", func(t *testing.T) {
		sup := newFakeIdempotentSupervisor()
		s := newIdempotencyServer(t, sup)
		_, spawnRes, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
			RepoDir:        "/tmp/repo",
			IdempotencyKey: "k",
		})
		if err != nil {
			t.Fatal(err)
		}
		spawnID, _ := spawnRes.(map[string]any)["run_id"].(string)

		_, fuRes, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
			RunID:          spawnID,
			Message:        "continue",
			IdempotencyKey: "k",
		})
		if err != nil {
			t.Fatal(err)
		}
		fuID, _ := fuRes.(map[string]any)["run_id"].(string)
		if fuID == spawnID {
			t.Fatalf("follow-up run_id %s equals the spawn run_id; the key spaces collided", fuID)
		}
		if n := sup.RuntimeCount(); n != 2 {
			t.Fatalf("runtimes = %d, want 2 (the follow-up must spawn a new runtime)", n)
		}
		if n := sup.SpawnCallCount(); n != 2 {
			t.Fatalf("supervisor Spawn calls = %d, want 2", n)
		}
	})

	// (b) A spawn keyed "follow_up:k" must not be hit by a follow-up keyed k.
	t.Run("spawn follow_up:k then follow-up k", func(t *testing.T) {
		sup := newFakeIdempotentSupervisor()
		s := newIdempotencyServer(t, sup)
		_, spawnRes, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
			RepoDir:        "/tmp/repo",
			IdempotencyKey: "follow_up:k",
		})
		if err != nil {
			t.Fatal(err)
		}
		spawnID, _ := spawnRes.(map[string]any)["run_id"].(string)

		_, fuRes, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
			RunID:          spawnID,
			Message:        "continue",
			IdempotencyKey: "k",
		})
		if err != nil {
			t.Fatal(err)
		}
		fuID, _ := fuRes.(map[string]any)["run_id"].(string)
		if fuID == spawnID {
			t.Fatalf("follow-up run_id %s equals the spawn run_id; a false hit across the prefix", fuID)
		}
		if n := sup.RuntimeCount(); n != 2 {
			t.Fatalf("runtimes = %d, want 2", n)
		}
		if n := sup.SpawnCallCount(); n != 2 {
			t.Fatalf("supervisor Spawn calls = %d, want 2", n)
		}
	})
}
