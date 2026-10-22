package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/stable"
)

// --- Stage 6: idempotent spawn (MCP server half) ---
//
// The MCP server forwards an idempotency key to the supervisor and re-derives
// the run identity from the returned result. These tests drive the MCP server
// against a fake supervisor that emulates the stable supervisor's idempotency
// gate (stored hits and hash conflicts) so the server-side contract is verified
// without a real runtime.

// fakeIdempotentSupervisor emulates the stable supervisor's idempotency gate
// for the MCP spawn/follow-up path. It keeps a per-key store of (hash, result)
// and replays the stored result on a key hit.
type fakeIdempotentSupervisor struct {
	mu       sync.Mutex
	entries  map[string]fakeIdemEntry
	nextRT   int
	runtimes int
	// spawnCalls counts Spawn invocations, one per attempt that reached the
	// supervisor (hits and rejected conflicts included).
	spawnCalls int
	listRuns   []map[string]any
}

type fakeIdemEntry struct {
	result map[string]any
	hash   string
}

func newFakeIdempotentSupervisor() *fakeIdempotentSupervisor {
	return &fakeIdempotentSupervisor{
		entries: map[string]fakeIdemEntry{},
	}
}

func (f *fakeIdempotentSupervisor) Spawn(params map[string]any) (map[string]any, error) {
	return f.spawn(params)
}

// SpawnMCP mirrors Spawn: the fake has no policy layer to enforce.
func (f *fakeIdempotentSupervisor) SpawnMCP(params map[string]any) (map[string]any, error) {
	return f.Spawn(params)
}

func (f *fakeIdempotentSupervisor) spawn(params map[string]any) (map[string]any, error) {
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
	f.mu.Lock()
	if e, ok := f.entries[key]; ok {
		if e.hash != hash {
			f.mu.Unlock()
			return nil, errors.New("idempotency key reused with different parameters")
		}
		f.mu.Unlock()
		return e.result, nil
	}
	f.mu.Unlock()
	res := f.doSpawn(params)
	f.mu.Lock()
	f.entries[key] = fakeIdemEntry{result: res, hash: hash}
	f.mu.Unlock()
	return res, nil
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

func (f *fakeIdempotentSupervisor) Shutdown(mode string) error    { return nil }
func (f *fakeIdempotentSupervisor) Cancel(runtimeID string) error { return nil }
func (f *fakeIdempotentSupervisor) Close() error                  { return nil }
func (f *fakeIdempotentSupervisor) Closed() bool                  { return false }
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
func (f *fakeIdempotentSupervisor) WorkflowCancel(workflowID, actor, reason string) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) WorkflowControllerStatus(controllerID string) (map[string]any, error) {
	return nil, nil
}
func (f *fakeIdempotentSupervisor) MessageSendContext(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageWaitContext(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return messaging.WaitResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageReplyContext(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageWithdrawContext(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return messaging.WithdrawResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageReceiveContext(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return messaging.ReceiveResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessagePeersContext(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	return messaging.PeersResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessagePreflightContext(ctx context.Context) (messaging.PreflightResult, error) {
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-test"}, nil
}
func (f *fakeIdempotentSupervisor) ResumeContext(_ context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	return messaging.ResumeResult{}, nil
}
func (f *fakeIdempotentSupervisor) SessionRenameContext(_ context.Context, sessionID, name string) (messaging.RenameResult, error) {
	return messaging.RenameResult{}, nil
}
func (f *fakeIdempotentSupervisor) SessionLastContext(_ context.Context, runtimeID string) (messaging.PeerLast, error) {
	return messaging.PeerLast{}, nil
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

// TestSpawnKeySpaceIsolatedFromResume pins the MCP spawn surface's "spawn:" key
// prefix: a spawn keyed "follow_up:k" reaches the supervisor as
// "spawn:follow_up:k". The supervisor namespaces resume keys with
// runstate.FollowUpIdempotencyKeyPrefix, so without the spawn prefix a resume
// keyed k would hit the spawn's stored result.
func TestSpawnKeySpaceIsolatedFromResume(t *testing.T) {
	sup := newFakeIdempotentSupervisor()
	s := newIdempotencyServer(t, sup)
	_, spawnRes, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/repo",
		Prompt:         "start",
		IdempotencyKey: "follow_up:k",
	})
	if err != nil {
		t.Fatal(err)
	}
	spawnID, _ := spawnRes.(map[string]any)["run_id"].(string)
	if spawnID == "" {
		t.Fatal("expected a non-empty spawn run_id")
	}
	runs, err := sup.List()
	if err != nil {
		t.Fatal(err)
	}
	spawnRuntime, _ := runs[0]["runtime_id"].(string)
	if spawnRuntime == "" {
		t.Fatal("expected the spawn to record a runtime")
	}

	// Drive the supervisor-side gate exactly as a keyed resume does: the
	// resume namespaces the caller key with the follow_up: prefix.
	resumed, err := sup.Spawn(map[string]any{
		"prompt":          "continue",
		"dir":             "/tmp/repo",
		"idempotency_key": runstate.FollowUpIdempotencyKeyPrefix + "k",
	})
	if err != nil {
		t.Fatalf("resume reused the spawn's key space: %v", err)
	}
	if id, _ := resumed["runtime_id"].(string); id == "" || id == spawnRuntime {
		t.Fatalf("resume runtime = %v, want a new runtime distinct from the spawn %s", resumed["runtime_id"], spawnRuntime)
	}
	if n := sup.RuntimeCount(); n != 2 {
		t.Fatalf("runtimes = %d, want 2 (spawn plus resume)", n)
	}
}
