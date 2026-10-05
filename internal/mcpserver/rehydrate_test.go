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
			m, ok := result.(map[string]any)
			if !ok {
				t.Fatalf("result = %T, want map[string]any", result)
			}
			events, ok := m["events"].([]map[string]any)
			if !ok {
				t.Fatalf("events = %T, want []map[string]any", m["events"])
			}
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

	fake := &fakeClient{
		listFunc: func() ([]map[string]any, error) {
			return []map[string]any{rehydrateEntry()}, nil
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

	// A second call must resolve the rehydrated entry.
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: rehydrateUUID})
	if err != nil {
		t.Fatal(err)
	}
	m := statusOutputMap(t, result)
	if m["run_id"] != rehydrateUUID {
		t.Fatalf("status = %#v", m)
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
			m, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("value = %T, want map[string]any", value)
			}
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

func TestLookupRunRepointsStaleLabelAfterSupervisorRestart(t *testing.T) {
	const supA = "/tmp/supA.sock"
	const newUUID = "11111111-2222-3333-4444-555555555555"
	fake := &fakeClient{listFunc: func() ([]map[string]any, error) { return nil, fmt.Errorf("list must not be called") }}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	// A pre-restart entry cached under the same explicit socket path.
	if err := s.registry.Store(&RunInfo{RunID: "run1", Label: "x", RuntimeID: "rt-old", SupervisorID: supA}); err != nil {
		t.Fatal(err)
	}

	// The restarted supervisor's list carries only the new run, which claims
	// the stale label. The list is consumed: a second call is an error.
	var listCalls atomic.Int32
	fake.listFunc = func() ([]map[string]any, error) {
		if n := listCalls.Add(1); n > 1 {
			return nil, fmt.Errorf("list must not be called again (call %d)", n)
		}
		return []map[string]any{{
			"runtime_id":    "rt-new",
			"label":         "x",
			"sentinel_file": filepath.Join(os.TempDir(), "avenor-run-"+newUUID+".done"),
		}}, nil
	}

	ri, err := s.lookupRun(fake, supA, "x")
	if err != nil {
		t.Fatalf("lookupRun = %v, want success after re-pointing the stale label", err)
	}
	if ri.RunID != newUUID || ri.RuntimeID != "rt-new" {
		t.Fatalf("re-pointed entry = %#v, want the new live run", ri)
	}
	if got := s.registry.LookupLabel(supA, "x"); got == nil || got.RunID != newUUID {
		t.Fatalf("LookupLabel(x) = %#v, want the new run", got)
	}
	if s.registry.Lookup(supA, "run1") == nil {
		t.Fatal("stale pre-restart entry must remain discoverable by run ID")
	}

	// A second lookup by the new run's ID resolves the cached entry without
	// re-listing.
	if _, err := s.lookupRun(fake, supA, newUUID); err != nil {
		t.Fatalf("second lookupRun = %v", err)
	}
	if n := listCalls.Load(); n != 1 {
		t.Fatalf("list calls = %d, want 1 (the second lookup must not re-list)", n)
	}
}

func TestLookupRunKeepsStillLiveLabelCollision(t *testing.T) {
	const supA = "/tmp/supA.sock"
	fake := &fakeClient{listResult: []map[string]any{
		{"runtime_id": "rt-old", "label": "x"},
		{"runtime_id": "rt-new", "label": "x"},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{RunID: "run1", Label: "x", RuntimeID: "rt-old", SupervisorID: supA}); err != nil {
		t.Fatal(err)
	}

	// Both runtimes are live and both claim the label: the ambiguity error
	// fires and the live entry is not re-pointed.
	_, err = s.lookupRun(fake, supA, "x")
	if err == nil || !strings.Contains(err.Error(), "ambiguous label x") {
		t.Fatalf("lookupRun = %v, want ambiguity (both runtimes still live)", err)
	}
	if s.registry.Lookup(supA, "run1") == nil {
		t.Fatal("still-live entry must not be re-pointed")
	}
}

func TestLookupRunDoesNotRepointOtherSupervisorsLabel(t *testing.T) {
	const supA = "/tmp/supA.sock"
	const supB = "/tmp/supB.sock"
	fake := &fakeClient{listResult: []map[string]any{{"runtime_id": "rt-new", "label": "x"}}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	// The label is cached under supervisor B.
	if err := s.registry.Store(&RunInfo{RunID: "run-b", Label: "x", RuntimeID: "rt-b", SupervisorID: supB}); err != nil {
		t.Fatal(err)
	}

	// Discovery on supervisor A collides with B's label: it errors and never
	// re-points B's entry.
	_, err = s.lookupRun(fake, supA, "x")
	if err == nil || !strings.Contains(err.Error(), "already maps") {
		t.Fatalf("lookupRun = %v, want a label-collision error (label owned by supervisor B)", err)
	}
	if s.registry.Lookup(supB, "run-b") == nil {
		t.Fatal("supervisor B's entry must not be re-pointed by supervisor A's discovery")
	}
	if got := s.registry.LookupLabel(supB, "x"); got == nil || got.RunID != "run-b" {
		t.Fatalf("supervisor B's label = %#v, want run-b intact", got)
	}
}

func TestAvenorStatusListFiltersRegistryBySupervisor(t *testing.T) {
	const supA = "/tmp/supA.sock"
	const supB = "/tmp/supB.sock"
	// Supervisor B's list returns a runtime that supervisor A has cached.
	fake := &fakeClient{listResult: []map[string]any{
		{"runtime_id": "rt_shared", "status": "running", "session_id": "ses_b"},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{
		RunID:        "run-a",
		Label:        "label-a",
		RuntimeID:    "rt_shared",
		SupervisorID: supA,
	}); err != nil {
		t.Fatal(err)
	}
	s.defaultSupervisorPath = supB

	// The list output against supervisor B must not bleed supervisor A's
	// registry identity into a matching runtime; the run is a raw supervisor
	// B run.
	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatal(err)
	}
	runs := statusOutputRuns(t, result)
	if len(runs) != 1 {
		t.Fatalf("runs = %#v, want 1", runs)
	}
	if runs[0]["runtime_id"] != "rt_shared" {
		t.Fatalf("runtime_id = %v, want rt_shared", runs[0]["runtime_id"])
	}
	if runs[0]["run_id"] != nil {
		t.Fatalf("run_id = %v, want absent (supervisor A's entry must not bleed into B)", runs[0]["run_id"])
	}
	if runs[0]["label"] != nil {
		t.Fatalf("label = %v, want absent (supervisor A's entry must not bleed into B)", runs[0]["label"])
	}
	if s.registry.Lookup(supA, "run-a") == nil {
		t.Fatal("supervisor A's registry entry must remain intact")
	}
}

// --- PR2 verdict: validate label availability before spawn ---

func TestSpawnSkipsLabelPrecheckForKeyedSpawn(t *testing.T) {
	var spawnCalls atomic.Int32
	fake := &fakeClient{
		listResult: []map[string]any{{"runtime_id": "rt-live", "label": "taken"}},
		spawnFunc: func(map[string]any) (map[string]any, error) {
			spawnCalls.Add(1)
			return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	// A keyed spawn whose label is live-claimed skips the pre-check: the
	// request reaches the supervisor's idempotency gate (modeled here as the
	// Spawn call).
	_, spawnRes, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:        "/tmp/test-repo",
		Label:          "taken",
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("keyed spawn with live-claimed label errored: %v", err)
	}
	if n := spawnCalls.Load(); n != 1 {
		t.Fatalf("spawn calls = %d, want 1 (pre-check skipped, gate consulted)", n)
	}
	// The no-sentinel response exercises the keyed re-derivation's false
	// branch: the run must retain the fresh identity (run_id not overwritten
	// to empty) and the registry entry must stay intact.
	runID, _ := spawnRes.(map[string]any)["run_id"].(string)
	if runID == "" {
		t.Fatal("expected non-empty run_id")
	}
	ri := s.registry.LookupUnique(runID)
	if ri == nil {
		t.Fatal("expected the registry entry for the spawned run")
	}
	if ri.RunID != runID {
		t.Fatalf("registry run = %s, want %s", ri.RunID, runID)
	}
	if ri.Label != "taken" {
		t.Fatalf("registry label = %s, want taken", ri.Label)
	}
	if ri.RuntimeID == "" {
		t.Fatal("expected non-empty registry runtime_id")
	}
}

func TestSpawnRejectsLabelClaimedByLiveRun(t *testing.T) {
	var spawnCalls atomic.Int32
	fake := &fakeClient{
		listResult: []map[string]any{{"runtime_id": "rt-live", "label": "taken"}},
		spawnFunc: func(map[string]any) (map[string]any, error) {
			spawnCalls.Add(1)
			return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir: "/tmp/test-repo",
		Label:   "taken",
	})
	if err == nil || !strings.Contains(err.Error(), "label already in use: taken") {
		t.Fatalf("error = %v, want label already in use", err)
	}
	if n := spawnCalls.Load(); n != 0 {
		t.Fatalf("spawn calls = %d, want 0 (no run left untracked)", n)
	}
}

func TestSpawnRejectsLabelClaimedByOtherSupervisor(t *testing.T) {
	const supA = "/tmp/supA.sock"
	const supB = "/tmp/supB.sock"
	var spawnCalls atomic.Int32
	fake := &fakeClient{
		listResult: []map[string]any{{"runtime_id": "rt-other", "label": "other"}},
		spawnFunc: func(map[string]any) (map[string]any, error) {
			spawnCalls.Add(1)
			return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake, SupervisorSocket: supA})
	if err != nil {
		t.Fatal(err)
	}
	// The label is claimed by a cached entry on supervisor B.
	if err := s.registry.Store(&RunInfo{RunID: "run-x", Label: "shared", RuntimeID: "rt-x", SupervisorID: supB}); err != nil {
		t.Fatal(err)
	}

	// Supervisor A's live list does not carry the label: the pre-check must
	// reject before spawn so no run is left live but untracked.
	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir: "/tmp/test-repo",
		Label:   "shared",
	})
	if err == nil || !strings.Contains(err.Error(), "label already in use: shared") ||
		!strings.Contains(err.Error(), supB) {
		t.Fatalf("error = %v, want the mapped-supervisor rejection naming supervisor B", err)
	}
	if n := spawnCalls.Load(); n != 0 {
		t.Fatalf("spawn calls = %d, want 0 (no run left untracked)", n)
	}
	if got := s.registry.LookupLabel(supB, "shared"); got == nil || got.RunID != "run-x" {
		t.Fatalf("supervisor B entry = %#v, want run-x untouched", got)
	}
}

func TestSpawnReclaimsLabelFromEndedRunOnOtherSupervisor(t *testing.T) {
	const supA = "/tmp/supA.sock"
	const supB = "/tmp/supB.sock"
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "avenor-run-9f9619ff-8b86-d011-b42d-00cf4fc964ff.done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses-x\n"), 0644); err != nil {
		t.Fatal(err)
	}
	var spawnCalls atomic.Int32
	fake := &fakeClient{
		listResult: []map[string]any{{"runtime_id": "rt-other", "label": "other"}},
		spawnFunc: func(map[string]any) (map[string]any, error) {
			spawnCalls.Add(1)
			return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake, SupervisorSocket: supA})
	if err != nil {
		t.Fatal(err)
	}
	// The label is claimed by an ended entry on supervisor B.
	if err := s.registry.Store(&RunInfo{
		RunID:        "run-x",
		Label:        "shared",
		RuntimeID:    "rt-x",
		SupervisorID: supB,
		SentinelPath: sentinelPath,
	}); err != nil {
		t.Fatal(err)
	}

	// The ended run no longer holds the label: the pre-check must repoint
	// the mapping and let the spawn proceed.
	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir: "/tmp/test-repo",
		Label:   "shared",
	})
	if err != nil {
		t.Fatalf("spawn with ended cross-supervisor holder failed: %v", err)
	}
	if n := spawnCalls.Load(); n != 1 {
		t.Fatalf("spawn calls = %d, want 1", n)
	}
	// The label mapping re-pointed to the new run on supervisor A.
	got := s.registry.LabelHolder("shared")
	if got == nil || got.SupervisorID != supA || got.RunID == "run-x" {
		t.Fatalf("label holder = %#v, want the new run on supervisor A", got)
	}
	// The ended run stays discoverable by run ID.
	if s.registry.Lookup(supB, "run-x") == nil {
		t.Fatal("ended supervisor B entry must remain discoverable by run ID")
	}
}

func TestSpawnToleratesListErrorInLabelPrecheck(t *testing.T) {
	var spawnCalls atomic.Int32
	fake := &fakeClient{
		listErr: fmt.Errorf("supervisor unavailable"),
		spawnFunc: func(map[string]any) (map[string]any, error) {
			spawnCalls.Add(1)
			return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	// A list failure must not block the spawn: the registry collision check
	// remains the backstop.
	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir: "/tmp/test-repo",
		Label:   "anything",
	})
	if err != nil {
		t.Fatalf("spawn with list-error pre-check failed: %v", err)
	}
	if n := spawnCalls.Load(); n != 1 {
		t.Fatalf("spawn calls = %d, want 1", n)
	}
}

func TestSpawnReapsStaleLabelMapping(t *testing.T) {
	var spawnCalls atomic.Int32
	fake := &fakeClient{
		listResult: []map[string]any{{"runtime_id": "rt-other", "label": "other"}},
		spawnFunc: func(map[string]any) (map[string]any, error) {
			spawnCalls.Add(1)
			return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	// A stale pre-restart entry claiming the label, whose runtime is not live.
	if err := s.registry.Store(&RunInfo{RunID: "run-old", Label: "taken", RuntimeID: "rt-dead", SupervisorID: ""}); err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir: "/tmp/test-repo",
		Label:   "taken",
	})
	if err != nil {
		t.Fatalf("spawn = %v, want success after reaping the stale mapping", err)
	}
	if n := spawnCalls.Load(); n != 1 {
		t.Fatalf("spawn calls = %d, want 1", n)
	}
	runID, _ := result.(map[string]any)["run_id"].(string)
	ri := s.registry.Lookup("", runID)
	if ri == nil || ri.RuntimeID != "rt-new" {
		t.Fatalf("registry entry = %#v, want the new run", ri)
	}
	// The stale mapping was reaped and re-pointed to the new run.
	if got := s.registry.LookupLabel("", "taken"); got == nil || got.RunID != runID {
		t.Fatalf("LookupLabel(taken) = %#v, want the new run", got)
	}
	if s.registry.Lookup("", "run-old") == nil {
		t.Fatal("stale pre-restart entry must remain discoverable by run ID")
	}
}

func TestFollowUpRejectsLabelClaimedByLiveRun(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "avenor-run-"+rehydrateUUID+".done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_re_1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	var spawnCalls atomic.Int32
	fake := &fakeClient{
		listResult: []map[string]any{
			rehydrateEntry(func(entry map[string]any) { entry["sentinel_file"] = sentinelPath }),
			{"runtime_id": "rt-live", "label": "taken"},
		},
		spawnFunc: func(map[string]any) (map[string]any, error) {
			spawnCalls.Add(1)
			return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:   rehydrateUUID,
		Message: "continue",
		Label:   "taken",
	})
	if err == nil || !strings.Contains(err.Error(), "label already in use: taken") {
		t.Fatalf("error = %v, want label already in use", err)
	}
	if n := spawnCalls.Load(); n != 0 {
		t.Fatalf("spawn calls = %d, want 0 (no run left untracked)", n)
	}
}

func TestSpawnReclaimsLabelFromEndedRun(t *testing.T) {
	t.Run("ended run does not hold the label", func(t *testing.T) {
		var spawnCalls atomic.Int32
		fake := &fakeClient{
			listResult: []map[string]any{{"runtime_id": "rt-ended", "label": "taken", "status": "done"}},
			spawnFunc: func(map[string]any) (map[string]any, error) {
				spawnCalls.Add(1)
				return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
			},
		}
		s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
		if err != nil {
			t.Fatal(err)
		}

		// An ended run's label is free to re-claim: the spawn must succeed.
		_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
			RepoDir: "/tmp/test-repo",
			Label:   "taken",
		})
		if err != nil {
			t.Fatalf("spawn = %v, want success (an ended run does not hold the label)", err)
		}
		if n := spawnCalls.Load(); n != 1 {
			t.Fatalf("spawn calls = %d, want 1", n)
		}
	})

	t.Run("ended mapping is unlinked and re-pointed", func(t *testing.T) {
		var spawnCalls atomic.Int32
		fake := &fakeClient{
			listResult: []map[string]any{{"runtime_id": "rt-ended", "label": "taken", "status": "done"}},
			spawnFunc: func(map[string]any) (map[string]any, error) {
				spawnCalls.Add(1)
				return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
			},
		}
		s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
		if err != nil {
			t.Fatal(err)
		}
		// A stale mapping whose runtime is ended: the label must be unlinked
		// and re-pointed to the new run.
		if err := s.registry.Store(&RunInfo{RunID: "run-old", Label: "taken", RuntimeID: "rt-ended", SupervisorID: ""}); err != nil {
			t.Fatal(err)
		}

		_, result, err := s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
			RepoDir: "/tmp/test-repo",
			Label:   "taken",
		})
		if err != nil {
			t.Fatalf("spawn = %v, want success after unlinking the ended mapping", err)
		}
		if n := spawnCalls.Load(); n != 1 {
			t.Fatalf("spawn calls = %d, want 1", n)
		}
		runID, _ := result.(map[string]any)["run_id"].(string)
		if got := s.registry.LookupLabel("", "taken"); got == nil || got.RunID != runID {
			t.Fatalf("LookupLabel(taken) = %#v, want the new run", got)
		}
		if s.registry.Lookup("", "run-old") == nil {
			t.Fatal("the ended run must remain discoverable by run ID")
		}
	})
}

func TestAvenorStatusListMergesRegistryIdentityWithMatchingScope(t *testing.T) {
	const supA = "/tmp/supA.sock"
	// The registry entry's supervisor matches the requested scope, so the
	// list output must merge the registry identity fields into the runtime
	// entry.
	fake := &fakeClient{listResult: []map[string]any{
		{"runtime_id": "rt_merge", "status": "running", "session_id": "ses_merge"},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.defaultSupervisorPath = supA
	if err := s.registry.Store(&RunInfo{
		RunID:            "run-merge",
		Label:            "label-merge",
		RuntimeID:        "rt_merge",
		SupervisorID:     supA,
		RosterFile:       "/repo/roster.json",
		RosterEntry:      "planner",
		EffectiveBackend: "agy",
		EffectiveAgent:   "planner-agent",
		EffectiveModel:   "planner-model",
	}); err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatal(err)
	}
	runs := statusOutputRuns(t, result)
	if len(runs) != 1 {
		t.Fatalf("runs = %#v, want 1", runs)
	}
	if runs[0]["run_id"] != "run-merge" {
		t.Fatalf("run_id = %v, want run-merge (registry identity merged)", runs[0]["run_id"])
	}
	if runs[0]["label"] != "label-merge" {
		t.Fatalf("label = %v, want label-merge (registry identity merged)", runs[0]["label"])
	}
	if runs[0]["roster_entry"] != "planner" || runs[0]["effective_backend"] != "agy" || runs[0]["effective_agent"] != "planner-agent" {
		t.Fatalf("roster identity missing from merged list entry: %#v", runs[0])
	}
	if runs[0]["runtime_id"] != "rt_merge" || runs[0]["status"] != "running" || runs[0]["session_id"] != "ses_merge" {
		t.Fatalf("live runtime fields missing from merged list entry: %#v", runs[0])
	}
}

func TestAvenorEventsCrossSupervisorGuard(t *testing.T) {
	const supA = "/tmp/supA.sock"
	const supB = "/tmp/supB.sock"
	const runX = "6f9619ff-8b86-d011-b42d-00cf4fc964ff"

	dir := t.TempDir()
	eventLogA := filepath.Join(dir, "events-a.log")
	eventLogB := filepath.Join(dir, "events-b.log")
	if err := os.WriteFile(eventLogA, []byte("{\"event\":\"from-a\",\"type\":\"lifecycle\"}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(eventLogB, []byte("{\"event\":\"from-b\",\"type\":\"lifecycle\"}\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// supA's list carries the run (by sentinel), scoped to supA.
	fake := &fakeClient{listResult: []map[string]any{{
		"runtime_id":    "rt-a",
		"sentinel_file": filepath.Join(dir, "avenor-run-"+runX+".done"),
		"on_event":      eventLogA,
	}}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	s.defaultSupervisorPath = supA
	// The run is cached under supB with a different event log.
	if err := s.registry.Store(&RunInfo{
		RunID:        runX,
		RuntimeID:    "rt-b",
		SupervisorID: supB,
		EventLogPath: eventLogB,
	}); err != nil {
		t.Fatal(err)
	}

	// Requesting the run under supA must not use supB's cached entry; it
	// re-resolves under supA and reads supA's event log.
	_, result, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{
		RunID:        runX,
		SupervisorID: supA,
	})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result = %T, want map[string]any", result)
	}
	events, ok := m["events"].([]map[string]any)
	if !ok {
		t.Fatalf("events = %T, want []map[string]any", m["events"])
	}
	if len(events) != 1 || events[0]["event"] != "from-a" {
		t.Fatalf("events = %#v, want supA's event (from-a), not supB's (from-b)", events)
	}
	// supB's cached entry must remain intact (not clobbered by the re-resolution).
	if got := s.registry.Lookup(supB, runX); got == nil || got.EventLogPath != eventLogB {
		t.Fatalf("supB entry = %#v, want intact with eventLogB", got)
	}
}

func TestLookupRunResolvesCachedLabelAfterRuntimeLeavesList(t *testing.T) {
	const sup = "/tmp/sup.sock"
	// The run's runtime left the live list (e.g. a supervisor restart); the
	// label is still cached. The list carries a different run, not the one
	// with the label.
	fake := &fakeClient{listResult: []map[string]any{
		{"runtime_id": "rt-other", "label": "other"},
	}}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.registry.Store(&RunInfo{
		RunID:        "run-x",
		Label:        "mylabel",
		RuntimeID:    "rt-x",
		SupervisorID: sup,
	}); err != nil {
		t.Fatal(err)
	}

	ri, err := s.lookupRun(fake, sup, "mylabel")
	if err != nil {
		t.Fatalf("lookupRun = %v, want the cached label entry", err)
	}
	if ri == nil || ri.RunID != "run-x" || ri.RuntimeID != "rt-x" {
		t.Fatalf("lookupRun = %#v, want run-x (rt-x)", ri)
	}
}
func TestFollowUpSkipsLabelPrecheckForKeyedFollowUp(t *testing.T) {
	dir := t.TempDir()
	sentinelPath := filepath.Join(dir, "avenor-run-"+rehydrateUUID+".done")
	if err := os.WriteFile(sentinelPath, []byte("DONE\nSESSION=ses_re_1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	var spawnCalls atomic.Int32
	fake := &fakeClient{
		listResult: []map[string]any{
			rehydrateEntry(func(entry map[string]any) { entry["sentinel_file"] = sentinelPath }),
			{"runtime_id": "rt-live", "label": "taken"},
		},
		spawnFunc: func(map[string]any) (map[string]any, error) {
			spawnCalls.Add(1)
			return map[string]any{"runtime_id": "rt-new", "session_id": "ses-new"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	// A keyed follow-up whose label is live-claimed skips the pre-check: the
	// request reaches the supervisor's idempotency gate (modeled here as the
	// Spawn call).
	_, fuRes, err := s.handleAvenorFollowUp(context.Background(), nil, followUpArgs{
		RunID:          rehydrateUUID,
		Message:        "continue",
		Label:          "taken",
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("keyed follow-up with live-claimed label errored: %v", err)
	}
	if n := spawnCalls.Load(); n != 1 {
		t.Fatalf("spawn calls = %d, want 1 (pre-check skipped, gate consulted)", n)
	}
	// The no-sentinel response exercises the keyed re-derivation's false
	// branch: the follow-up must retain the fresh identity (run_id not
	// overwritten to empty) and the registry entry must stay intact.
	runID, _ := fuRes.(map[string]any)["run_id"].(string)
	if runID == "" {
		t.Fatal("expected non-empty run_id")
	}
	ri := s.registry.LookupUnique(runID)
	if ri == nil {
		t.Fatal("expected the registry entry for the follow-up run")
	}
	if ri.RunID != runID {
		t.Fatalf("registry run = %s, want %s", ri.RunID, runID)
	}
	if ri.Label != "taken" {
		t.Fatalf("registry label = %s, want taken", ri.Label)
	}
	if ri.RuntimeID == "" {
		t.Fatal("expected non-empty registry runtime_id")
	}
}
