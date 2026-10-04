package mcpserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- Stage 5: registry rehydration from the supervisor ---

const rehydrateUUID = "6f9619ff-8b86-d011-b42d-00cf4fc964ff"

// rehydrateEntry builds a stable supervisor list entry for a run spawned with
// MCP run UUID rehydrateUUID.
func rehydrateEntry(mutate ...func(map[string]any)) map[string]any {
	entry := map[string]any{
		"runtime_id":    "rt_re_1",
		"session_id":    "ses_re_1",
		"label":         "agent-x",
		"sentinel_file": filepath.Join(os.TempDir(), "avenor-run-"+rehydrateUUID+".done"),
		"on_event":      filepath.Join(os.TempDir(), "avenor-run-"+rehydrateUUID+".log"),
		"dir":           "/tmp/re-repo",
		"started_at":    float64(1721300000000),
		"thinking":      "high",
		"auto_approve":  true,
		"agent":         "claude",
		"agent_profile": "cloud",
		"run_id":        "supervisor-wide-42",
	}
	for _, fn := range mutate {
		fn(entry)
	}
	return entry
}

func TestAvenorStatusRehydratesFromSupervisorList(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"mcp uuid", rehydrateUUID},
		{"runtime id", "rt_re_1"},
		{"unique label", "agent-x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeClient{
				listResult:   []map[string]any{rehydrateEntry()},
				statusResult: map[string]any{"status": "running", "session_id": "ses_live"},
			}
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
			if err != nil {
				t.Fatal(err)
			}

			_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: tc.key})
			if err != nil {
				t.Fatal(err)
			}
			m := statusOutputMap(t, result)
			if m["run_id"] != rehydrateUUID {
				t.Errorf("run_id = %v, want %s", m["run_id"], rehydrateUUID)
			}
			if m["label"] != "agent-x" {
				t.Errorf("label = %v, want agent-x", m["label"])
			}
			if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "rt_re_1" {
				t.Errorf("status runtime IDs = %v, want [rt_re_1]", fake.statusCapturedRuntimeIDs)
			}

			ri := s.registry.Lookup("", rehydrateUUID)
			if ri == nil {
				t.Fatal("expected rehydrated registry entry")
			}
			if ri.RuntimeID != "rt_re_1" || ri.SessionID != "ses_re_1" || ri.Label != "agent-x" {
				t.Errorf("rehydrated entry = %#v", ri)
			}
			if ri.Dir != "/tmp/re-repo" || ri.Thinking != "high" || !ri.AutoApprove {
				t.Errorf("rehydrated entry = %#v", ri)
			}
			if ri.Agent != "claude" || ri.AgentProfile != "cloud" {
				t.Errorf("rehydrated identity = %#v", ri)
			}
			if !ri.CreatedAt.Equal(time.UnixMilli(1721300000000)) {
				t.Errorf("CreatedAt = %v, want unix ms 1721300000000", ri.CreatedAt)
			}
		})
	}
}

func TestAvenorEventsRehydratesFromSupervisorList(t *testing.T) {
	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "rehydrate-events.log")
	content := "{\"event\":\"start\",\"type\":\"lifecycle\"}\n{\"event\":\"prompt\",\"type\":\"turn\"}\n"
	if err := os.WriteFile(eventLogPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		key  string
	}{
		{"mcp uuid", rehydrateUUID},
		{"runtime id", "rt_re_1"},
		{"unique label", "agent-x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeClient{listResult: []map[string]any{rehydrateEntry(func(entry map[string]any) {
				entry["on_event"] = eventLogPath
			})}}
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
			if err != nil {
				t.Fatal(err)
			}

			_, result, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: tc.key})
			if err != nil {
				t.Fatal(err)
			}
			events, _ := result.(map[string]any)["events"].([]map[string]any)
			if len(events) != 2 {
				t.Fatalf("events = %#v, want 2 entries", events)
			}
			ri := s.registry.Lookup("", rehydrateUUID)
			if ri == nil || ri.EventLogPath != eventLogPath {
				t.Fatalf("rehydrated entry = %#v", ri)
			}
		})
	}
}

func TestAvenorEventsRehydratesUnderAutostartedSupervisorPath(t *testing.T) {
	const socketPath = "/tmp/avenor-events-autostart.sock"
	origStart := startSupervisorFunc
	defer func() { startSupervisorFunc = origStart }()

	var listCalls atomic.Int32
	fake := &fakeClient{
		listFunc: func() ([]map[string]any, error) {
			n := listCalls.Add(1)
			if n == 1 {
				return []map[string]any{rehydrateEntry()}, nil
			}
			return nil, fmt.Errorf("list must not be called again")
		},
		statusResult: map[string]any{"status": "running", "session_id": "ses_re_1"},
	}
	startSupervisorFunc = func(string, time.Duration) (*supervisorLifecycle, error) {
		return &supervisorLifecycle{socketPath: socketPath, client: fake}, nil
	}

	// A fresh server has no client yet: the first events call must autostart
	// the default supervisor and scope the rehydrated entry to the real
	// socket path, not "".
	s, err := NewServer(Options{Transport: "stdio"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, _, err = s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: rehydrateUUID})
	if err != nil {
		t.Fatal(err)
	}
	ri := s.registry.Lookup(socketPath, rehydrateUUID)
	if ri == nil {
		t.Fatalf("rehydrated entry not scoped to the autostarted socket path: %#v", s.registry.All())
	}
	if s.registry.Lookup("", rehydrateUUID) != nil {
		t.Fatal("rehydrated entry leaked into the empty supervisor scope")
	}

	// A second call must resolve the cached entry without re-listing.
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: rehydrateUUID})
	if err != nil {
		t.Fatal(err)
	}
	m := statusOutputMap(t, result)
	if m["run_id"] != rehydrateUUID {
		t.Fatalf("status = %#v", m)
	}
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1 (the second call must use the cached entry)", got)
	}
}

func TestAvenorResultRehydratesFromSupervisorList(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"mcp uuid", rehydrateUUID},
		{"runtime id", "rt_re_1"},
		{"unique label", "agent-x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeClient{
				listResult:   []map[string]any{rehydrateEntry()},
				statusResult: map[string]any{"status": "done", "session_id": "ses_live"},
				resultResult: map[string]any{"final_output": "full answer"},
			}
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
			if err != nil {
				t.Fatal(err)
			}

			_, value, err := s.handleAvenorResult(context.Background(), nil, resultArgs{RunID: tc.key})
			if err != nil {
				t.Fatal(err)
			}
			m := value.(map[string]any)
			if m["ready"] != true || m["output"] != "full answer" {
				t.Fatalf("result = %#v", m)
			}
			if m["run_id"] != rehydrateUUID || m["label"] != "agent-x" {
				t.Fatalf("result identity = %#v", m)
			}
			if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "rt_re_1" {
				t.Fatalf("status runtime IDs = %v, want [rt_re_1]", fake.statusCapturedRuntimeIDs)
			}
		})
	}
}

func TestAvenorFollowUpRehydratesFromSupervisorList(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "avenor-run-"+rehydrateUUID+".done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_re_1\n"), 0644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeClient{
		listResult: []map[string]any{rehydrateEntry(func(entry map[string]any) {
			entry["sentinel_file"] = sentinelPath
		})},
		spawnResult: map[string]any{"runtime_id": "rt_followup_re", "session_id": "ses_followup_re"},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   rehydrateUUID,
		Message: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	p := fake.spawnCapturedParams
	if p["session_id"] != "ses_re_1" {
		t.Errorf("session_id = %v, want ses_re_1 from the rehydrated sentinel", p["session_id"])
	}
	if p["thinking"] != "high" {
		t.Errorf("thinking = %v, want high restored from the rehydrated entry", p["thinking"])
	}
	if got, ok := p["auto_approve"].(bool); !ok || !got {
		t.Errorf("auto_approve = %T %v, want true restored from the rehydrated entry", p["auto_approve"], p["auto_approve"])
	}
	if p["agent"] != "claude" || p["dir"] != "/tmp/re-repo" {
		t.Errorf("follow-up params = %#v", p)
	}
	followupID, _ := result.(map[string]any)["run_id"].(string)
	ri := s.registry.LookupUnique(followupID)
	if ri == nil || !ri.AutoApprove || ri.Thinking != "high" {
		t.Fatalf("follow-up registry entry = %#v", ri)
	}
}

func TestAvenorAnswerPermissionRehydratesFromSupervisorList(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"mcp uuid", rehydrateUUID},
		{"runtime id", "rt_re_1"},
		{"unique label", "agent-x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeClient{listResult: []map[string]any{rehydrateEntry()}}
			s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
			if err != nil {
				t.Fatal(err)
			}

			_, result, err := s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{
				RunID:     tc.key,
				OptionID:  "opt_allow",
				RequestID: "req_1",
			})
			if err != nil {
				t.Fatal(err)
			}
			if m, _ := result.(map[string]any); m["ok"] != true {
				t.Fatalf("result = %#v", result)
			}
			if len(fake.answerPermissionCalls) != 1 || fake.answerPermissionCalls[0].runtimeID != "rt_re_1" {
				t.Fatalf("answer permission calls = %#v", fake.answerPermissionCalls)
			}
		})
	}
}

func TestLookupRunAmbiguousLabel(t *testing.T) {
	fake := &fakeClient{listResult: []map[string]any{
		{"runtime_id": "rt_a", "label": "dup"},
		{"runtime_id": "rt_b", "label": "dup"},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "dup"})
	if err == nil || !strings.Contains(err.Error(), "ambiguous label dup") ||
		!strings.Contains(err.Error(), "rt_a") || !strings.Contains(err.Error(), "rt_b") {
		t.Fatalf("error = %v, want ambiguous label naming both runtimes", err)
	}
	if len(fake.statusCapturedRuntimeIDs) != 0 {
		t.Fatalf("status calls = %v, want none after ambiguity", fake.statusCapturedRuntimeIDs)
	}

	// A cached label must still establish uniqueness through the list.
	if err := s.registry.Store(&RunInfo{RunID: "cached-run", Label: "dup", RuntimeID: "rt_cached"}); err != nil {
		t.Fatal(err)
	}
	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "dup"})
	if err == nil || !strings.Contains(err.Error(), "ambiguous label dup") {
		t.Fatalf("cached label lookup error = %v, want ambiguity despite cached entry", err)
	}
}

func TestLookupRunScopedBySupervisor(t *testing.T) {
	uuidB := "c9bf9e57-1685-4c89-bafb-ff5af830be8a"
	clA := &fakeClient{listResult: []map[string]any{rehydrateEntry(func(entry map[string]any) {
		entry["runtime_id"] = "rt_same"
		entry["label"] = "shared-a"
	})}}
	clB := &fakeClient{listResult: []map[string]any{rehydrateEntry(func(entry map[string]any) {
		entry["runtime_id"] = "rt_same"
		entry["label"] = "shared-b"
		entry["sentinel_file"] = filepath.Join(os.TempDir(), "avenor-run-"+uuidB+".done")
	})}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: clA})
	if err != nil {
		t.Fatal(err)
	}

	riA, err := s.lookupRun(clA, "/tmp/supA.sock", rehydrateUUID)
	if err != nil {
		t.Fatal(err)
	}
	riB, err := s.lookupRun(clB, "/tmp/supB.sock", uuidB)
	if err != nil {
		t.Fatal(err)
	}
	if riA == riB || riA.RunID != rehydrateUUID || riB.RunID != uuidB {
		t.Fatalf("entries = %#v / %#v, want distinct per supervisor", riA, riB)
	}
	if riA.RuntimeID != "rt_same" || riB.RuntimeID != "rt_same" {
		t.Fatalf("runtime IDs = %s / %s, want identical rt_same on both supervisors", riA.RuntimeID, riB.RuntimeID)
	}
	if s.registry.Lookup("/tmp/supA.sock", rehydrateUUID) != riA {
		t.Fatal("supervisor A entry missing or wrong")
	}
	if s.registry.Lookup("/tmp/supB.sock", uuidB) != riB {
		t.Fatal("supervisor B entry missing or wrong")
	}
	if s.registry.Lookup("/tmp/supA.sock", uuidB) != nil {
		t.Fatal("entry from supervisor B leaked into supervisor A's scope")
	}

	// A key that only exists on B is not discoverable on A and does not
	// disturb A's entry.
	ri, err := s.lookupRun(clA, "/tmp/supA.sock", uuidB)
	if err != nil {
		t.Fatal(err)
	}
	if ri != nil {
		t.Fatalf("lookup on A for B's run = %#v, want nil", ri)
	}
	if s.registry.Lookup("/tmp/supA.sock", rehydrateUUID) != riA {
		t.Fatal("supervisor A entry was clobbered")
	}
}

func TestCachedRunFromOtherSupervisorNotUsed(t *testing.T) {
	fake := &fakeClient{statusResult: map[string]any{"status": "running", "session_id": "ses_default"}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{
		RunID:        "run-x",
		Label:        "run-x",
		RuntimeID:    "rt_from_a",
		SupervisorID: "/tmp/supA.sock",
		SentinelPath: filepath.Join(t.TempDir(), "avenor-run-other.done"),
	}); err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run-x"})
	if err != nil {
		t.Fatal(err)
	}
	m := statusOutputMap(t, result)
	if m["session_id"] != "ses_default" {
		t.Fatalf("status = %#v, want the default supervisor's live answer", m)
	}
	if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "run-x" {
		t.Fatalf("status runtime IDs = %v, want raw fallback [run-x]", fake.statusCapturedRuntimeIDs)
	}
	if s.registry.Lookup("/tmp/supA.sock", "run-x") == nil {
		t.Fatal("supervisor A entry must remain intact")
	}
}

func TestLookupRunListErrorPropagates(t *testing.T) {
	fake := &fakeClient{listErr: fmt.Errorf("connection refused")}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run-1"})
	if err == nil || !strings.Contains(err.Error(), "list runs") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("status error = %v, want propagated list error", err)
	}

	_, _, err = s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: "run-1"})
	if err == nil || !strings.Contains(err.Error(), "list runs") {
		t.Fatalf("events error = %v, want propagated list error, not not-found", err)
	}
}

func TestLookupRunNoMatchKeepsExistingBehavior(t *testing.T) {
	fake := &fakeClient{
		listResult:   []map[string]any{},
		statusResult: map[string]any{"status": "running", "session_id": "ses_direct"},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	// Raw runtime ID still resolves through the supervisor.
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt_direct"})
	if err != nil {
		t.Fatal(err)
	}
	m := statusOutputMap(t, result)
	if m["run_id"] != "rt_direct" || m["status"] != "running" {
		t.Fatalf("status = %#v", m)
	}

	// Unknown run keeps the not-found behavior on run-scoped tools.
	_, _, err = s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{RunID: "nonexistent", Message: "continue"})
	if err == nil || !strings.Contains(err.Error(), "run not found in registry") {
		t.Fatalf("follow-up error = %v, want run not found in registry", err)
	}
	_, _, err = s.handleAvenorAnswerPermission(context.Background(), nil, permissionArgs{RunID: "nonexistent", OptionID: "opt_allow"})
	if err == nil || !strings.Contains(err.Error(), "run \"nonexistent\" not found") {
		t.Fatalf("permission error = %v, want raw not-found", err)
	}
}

func TestSupervisorRunIDNeverUsedAsMCPRunID(t *testing.T) {
	sentinelPath := filepath.Join(t.TempDir(), "avenor-run-"+rehydrateUUID+".done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_wide\n"), 0644); err != nil {
		t.Fatal(err)
	}
	fake := &fakeClient{
		listResult: []map[string]any{rehydrateEntry(func(entry map[string]any) {
			entry["sentinel_file"] = sentinelPath
		})},
		statusResult: map[string]any{"status": "running"},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	// The supervisor-wide run_id is not a key this server recognizes.
	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "supervisor-wide-42"})
	if err != nil {
		t.Fatalf("status by supervisor-wide run_id should fall back to the raw query, got %v", err)
	}
	if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "supervisor-wide-42" {
		t.Fatalf("status runtime IDs = %v, want raw [supervisor-wide-42]", fake.statusCapturedRuntimeIDs)
	}

	// By MCP UUID, the sentinel wins and the MCP run ID is the parsed UUID.
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: rehydrateUUID})
	if err != nil {
		t.Fatal(err)
	}
	m := statusOutputMap(t, result)
	if m["run_id"] != rehydrateUUID {
		t.Fatalf("run_id = %v, want the MCP UUID, never the supervisor-wide run_id", m["run_id"])
	}
	ri := s.registry.Lookup("", rehydrateUUID)
	if ri == nil || ri.RunID != rehydrateUUID {
		t.Fatalf("registry entry = %#v, want RunID = MCP UUID", ri)
	}
}

func TestAvenorEventsUsesRegistryWithoutSupervisor(t *testing.T) {
	dir := t.TempDir()
	eventLogPath := filepath.Join(dir, "solo.log")
	if err := os.WriteFile(eventLogPath, []byte("{\"event\":\"tick\",\"type\":\"lifecycle\"}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	// No control client is available: the registry-only fast path must serve
	// the request without any supervisor RPC.
	s := &Server{registry: NewRunRegistry(), opts: Options{NoAutostart: true}}
	if err := s.registry.Store(&RunInfo{
		RunID:        "run-events-solo",
		SupervisorID: "",
		EventLogPath: eventLogPath,
	}); err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: "run-events-solo"})
	if err != nil {
		t.Fatalf("registry-only events failed: %v", err)
	}
	events, _ := result.(map[string]any)["events"].([]map[string]any)
	if len(events) != 1 || events[0]["event"] != "tick" {
		t.Fatalf("events = %#v", events)
	}
}

func TestResultSupervisorIDRoutingFallbacks(t *testing.T) {
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: &fakeClient{}})
	if err != nil {
		t.Fatal(err)
	}

	if got := s.resultSupervisorID("run-a", "/tmp/explicit.sock"); got != "/tmp/explicit.sock" {
		t.Fatalf("explicit supervisor hint = %q", got)
	}
	if got := s.resultSupervisorID("run-a", ""); got != "" {
		t.Fatalf("uncached run hint = %q, want empty", got)
	}
	if err := s.registry.Store(&RunInfo{RunID: "run-a", SupervisorID: "/tmp/supA.sock"}); err != nil {
		t.Fatal(err)
	}
	if got := s.resultSupervisorID("run-a", ""); got != "/tmp/supA.sock" {
		t.Fatalf("cached unique run hint = %q, want /tmp/supA.sock", got)
	}
	if err := s.registry.Store(&RunInfo{RunID: "run-a", SupervisorID: "/tmp/supB.sock"}); err != nil {
		t.Fatal(err)
	}
	if got := s.resultSupervisorID("run-a", ""); got != "" {
		t.Fatalf("ambiguous run hint = %q, want empty", got)
	}
}

func TestStatusSelectsScopedRegistryEntry(t *testing.T) {
	fake := &fakeClient{statusResult: map[string]any{"status": "running", "session_id": "ses_b"}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.defaultSupervisorPath = "/tmp/supB.sock"
	if err := s.registry.Store(&RunInfo{RunID: "shared-run", RuntimeID: "rt_from_a", SupervisorID: "/tmp/supA.sock"}); err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{RunID: "shared-run", RuntimeID: "rt_from_b", SupervisorID: "/tmp/supB.sock"}); err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{
		RunID:        "shared-run",
		SupervisorID: "/tmp/supB.sock",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.statusCapturedRuntimeIDs) != 1 || fake.statusCapturedRuntimeIDs[0] != "rt_from_b" {
		t.Fatalf("status runtime IDs = %v, want supervisor B's runtime", fake.statusCapturedRuntimeIDs)
	}
	m := statusOutputMap(t, result)
	if m["run_id"] != "shared-run" {
		t.Fatalf("status = %#v", m)
	}
	if s.registry.Lookup("/tmp/supA.sock", "shared-run") == nil || s.registry.Lookup("/tmp/supB.sock", "shared-run") == nil {
		t.Fatal("both scoped entries must survive")
	}
}
