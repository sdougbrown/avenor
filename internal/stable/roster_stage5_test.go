package stable

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

func writeStage5Roster(t *testing.T, dir string, contents string) string {
	t.Helper()
	path := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func scriptedStage5Provider(id, stopReason string) *stableScriptedProvider {
	return &stableScriptedProvider{
		attempt: -1,
		scripts: []stableScriptedAttempt{{
			sessionID: id,
			events: []stableScriptedEvent{{event: events.Event{
				Event:     "session.end",
				SessionID: id,
				Fields:    map[string]any{"stop_reason": stopReason},
			}}},
		}},
	}
}

func waitStage5Done(t *testing.T, child *childRuntime) {
	t.Helper()
	select {
	case <-child.done:
	case <-time.After(5 * time.Second):
		t.Fatal("stable child did not finish")
	}
}

func TestStableWorkflowBackendPropagationAtProviderBoundary(t *testing.T) {
	for _, mode := range []string{"loop", "team"} {
		for _, supplied := range []string{"", "agy"} {
			t.Run(mode+"/"+supplied, func(t *testing.T) {
				dir := t.TempDir()
				configPath := filepath.Join(dir, mode+".json")
				contents := `{"pre":[{"name":"work","prompt":"work"}]}`
				if mode == "team" {
					contents = `{"team":[{"name":"work","prompt":"work"}]}`
				}
				if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
				sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-boundary-" + mode + "-" + supplied + ".sock", MaxRuntimes: 1})
				want := supplied
				if want == "" {
					want = cli.DefaultBackend
				}
				got := ""
				sup.newProviderFunc = func(_ runtime.StartOptions, backend string) (runtime.Provider, error) {
					got = backend
					return scriptedStage5Provider("ses_boundary", "end_turn"), nil
				}
				params := SpawnParams{Dir: dir, Backend: supplied}
				if mode == "loop" {
					params.LoopFile = configPath
				} else {
					params.TeamFile = configPath
				}
				result, err := sup.spawn(params)
				if err != nil {
					t.Fatalf("spawn: %v", err)
				}
				sup.controlMu.Lock()
				child := sup.runtimes[result.RuntimeID]
				sup.controlMu.Unlock()
				if child == nil {
					t.Fatal("child was not registered")
				}
				waitStage5Done(t, child)
				if got != want {
					t.Fatalf("backend at newProviderFunc = %q, want %q", got, want)
				}
			})
		}
	}
}

func TestStableDirectRosterIdentityAndStatus(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-direct-roster.sock", MaxRuntimes: 1})
	provider := scriptedStage5Provider("ses_roster", "end_turn")
	var gotOpts runtime.StartOptions
	var gotBackend string
	sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		gotOpts, gotBackend = opts, backend
		return provider, nil
	}

	rosterPath := writeStage5Roster(t, t.TempDir(), `{"review":{"backend":"agy","agent":"reviewer","model":"review-model"}}`)
	result, err := sup.spawn(SpawnParams{
		Prompt:       "review",
		Dir:          t.TempDir(),
		RosterFile:   rosterPath,
		RosterEntry:  "review",
		AgentProfile: "cloud",
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer sup.cancelRuntime(result.RuntimeID)

	if gotBackend != "agy" || gotOpts.Agent != "reviewer" || gotOpts.Model != "review-model" || gotOpts.AgentProfile != "cloud" {
		t.Fatalf("provider identity = backend %q opts %+v", gotBackend, gotOpts)
	}
	statusAny, err := sup.RuntimeStatus(result.RuntimeID)
	if err != nil {
		t.Fatalf("RuntimeStatus: %v", err)
	}
	status := statusAny.(map[string]any)
	for key, want := range map[string]any{
		"backend":           "agy",
		"agent":             "reviewer",
		"model":             "review-model",
		"agent_profile":     "cloud",
		"roster_file":       rosterPath,
		"roster_entry":      "review",
		"effective_backend": "agy",
		"effective_agent":   "reviewer",
		"effective_model":   "review-model",
	} {
		if status[key] != want {
			t.Fatalf("status[%q] = %v, want %v (status=%#v)", key, status[key], want, status)
		}
	}
	identity, ok := sup.sessionIdentity("ses_roster")
	if !ok || identity.Backend != "agy" || identity.AgentProfile != "cloud" {
		t.Fatalf("session identity = %#v, ok=%v", identity, ok)
	}
}

func TestStableRosterPhasesReachProviderBoundary(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"plan":{"backend":"pi","agent":"planner","model":"planner-model"},
		"ship":{"backend":"agy","agent":"executor","model":"executor-model"}
	}`)
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{
		"roster_file":"roster.json",
		"pre":[{"name":"plan","prompt":"plan","roster_entry":"plan"}],
		"post":[{"name":"ship","prompt":"ship","roster_entry":"ship"},{"name":"plain","prompt":"plain"}]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-phase-roster.sock", MaxRuntimes: 1})
	var mu sync.Mutex
	var backends []string
	var options []runtime.StartOptions
	var sequence int
	sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		mu.Lock()
		sequence++
		backends = append(backends, backend)
		options = append(options, opts)
		id := fmt.Sprintf("ses_phase_%d", sequence)
		mu.Unlock()
		return scriptedStage5Provider(id, "end_turn"), nil
	}
	result, err := sup.spawn(SpawnParams{LoopFile: loopPath, Dir: dir, Backend: "gemini-acp", AgentProfile: "cloud"})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[result.RuntimeID]
	sup.controlMu.Unlock()
	if child == nil {
		t.Fatal("loop child was not registered")
	}
	waitStage5Done(t, child)

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"pi", "agy", "gemini-acp"}; len(backends) != len(want) || backends[0] != want[0] || backends[1] != want[1] || backends[2] != want[2] {
		t.Fatalf("backends = %v, want %v", backends, want)
	}
	if options[0].Agent != "planner" || options[0].Model != "planner-model" || options[0].AgentProfile != "cloud" || options[1].Agent != "executor" || options[1].Model != "executor-model" || options[1].AgentProfile != "cloud" || options[2].Agent != "" || options[2].Model != "" || options[2].AgentProfile != "cloud" {
		t.Fatalf("phase options = %+v", options)
	}
	for _, id := range []string{"ses_phase_1", "ses_phase_2", "ses_phase_3"} {
		if _, ok := sup.sessionIdentity(id); !ok {
			t.Fatalf("missing authoritative identity for %s", id)
		}
	}
}

func TestStableTeamRosterMembersUseDifferentBackends(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"security":{"backend":"pi","agent":"security","model":"security-model"},
		"style":{"backend":"gemini-acp","agent":"style","model":"style-model"}
	}`)
	teamPath := filepath.Join(dir, "team.json")
	if err := os.WriteFile(teamPath, []byte(`{"roster_file":"roster.json","team":[{"name":"security","prompt":"security","roster_entry":"security"},{"name":"style","prompt":"style","roster_entry":"style"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-team-roster.sock", MaxRuntimes: 2})
	var mu sync.Mutex
	var backends []string
	var options []runtime.StartOptions
	var sequence int
	sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		mu.Lock()
		sequence++
		backends = append(backends, backend)
		options = append(options, opts)
		id := fmt.Sprintf("ses_member_%d", sequence)
		mu.Unlock()
		return scriptedStage5Provider(id, "end_turn"), nil
	}
	result, err := sup.spawn(SpawnParams{TeamFile: teamPath, Dir: dir})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[result.RuntimeID]
	sup.controlMu.Unlock()
	if child == nil {
		t.Fatal("team child was not registered")
	}
	waitStage5Done(t, child)

	mu.Lock()
	defer mu.Unlock()
	seen := map[string]runtime.StartOptions{}
	for i, backend := range backends {
		seen[backend] = options[i]
	}
	if len(seen) != 2 || seen["pi"].Agent != "security" || seen["gemini-acp"].Agent != "style" {
		t.Fatalf("team identities backends=%v options=%v", backends, options)
	}
}

func TestStableRosterMigratesInlineTeamIdentity(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{"review":{"backend":"pi","agent":"roster-agent","model":"roster-model"}}`)
	teamPath := filepath.Join(dir, "team.json")
	if err := os.WriteFile(teamPath, []byte(`{"roster_file":"roster.json","team":[{"name":"review","prompt":"review","roster_entry":"review"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-migration.sock", MaxRuntimes: 1})
	var gotOpts runtime.StartOptions
	var gotBackend string
	sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		gotOpts, gotBackend = opts, backend
		return scriptedStage5Provider("ses_migration", "end_turn"), nil
	}
	result, err := sup.spawn(SpawnParams{
		TeamFile: teamPath, Dir: dir, Backend: "pi", Agent: "old-agent", Model: "old-model",
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[result.RuntimeID]
	sup.controlMu.Unlock()
	if child == nil {
		t.Fatal("child was not registered")
	}
	waitStage5Done(t, child)
	if gotBackend != "pi" || gotOpts.Agent != "roster-agent" || gotOpts.Model != "roster-model" {
		t.Fatalf("migrated identity backend=%q opts=%+v", gotBackend, gotOpts)
	}
}

func TestStableRosterThinkingValidatesBeforeProviderCreation(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{"slow":{"backend":"agy","agent":"executor"}}`)
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"roster_file":"roster.json","pre":[{"name":"work","prompt":"work","roster_entry":"slow"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-thinking.sock", MaxRuntimes: 1})
	called := false
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		called = true
		return scriptedStage5Provider("ses_never", "end_turn"), nil
	}
	result, err := sup.spawn(SpawnParams{LoopFile: loopPath, Dir: dir, Thinking: "low"})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[result.RuntimeID]
	sup.controlMu.Unlock()
	if child == nil {
		t.Fatal("child was not registered")
	}
	waitStage5Done(t, child)
	if called {
		t.Fatal("provider was created before effective roster thinking validation")
	}
}

func TestStableRosterRetryRetainsResolvedIdentity(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{"retry":{"backend":"pi","agent":"retry-agent","model":"retry-model"}}`)
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"roster_file":"roster.json","pre":[{"name":"work","prompt":"work","roster_entry":"retry"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-retry.sock", MaxRuntimes: 1})
	var mu sync.Mutex
	var backends []string
	var options []runtime.StartOptions
	attempt := 0
	sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		mu.Lock()
		attempt++
		backends = append(backends, backend)
		options = append(options, opts)
		id := fmt.Sprintf("ses_retry_%d", attempt)
		stop := "error"
		if attempt == 2 {
			stop = "end_turn"
		}
		mu.Unlock()
		return scriptedStage5Provider(id, stop), nil
	}
	result, err := sup.spawn(SpawnParams{LoopFile: loopPath, Dir: dir, MaxRetries: 1})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[result.RuntimeID]
	sup.controlMu.Unlock()
	if child == nil {
		t.Fatal("child was not registered")
	}
	waitStage5Done(t, child)
	mu.Lock()
	defer mu.Unlock()
	if len(backends) != 2 || backends[0] != "pi" || backends[1] != "pi" || options[0].Agent != options[1].Agent || options[0].Model != options[1].Model {
		t.Fatalf("retry identity backends=%v options=%v", backends, options)
	}
}

func TestStableSessionIdentityAdoptionDoesNotStaleOverwrite(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-adoption-map.sock", MaxRuntimes: 1})
	child := &childRuntime{
		id:               "rt_map",
		provider:         &stableFakeProvider{},
		session:          runtime.Session{SessionID: "provisional-new"},
		effectiveBackend: "pi",
		effectiveAgent:   "new-agent",
		effectiveModel:   "new-model",
		agentProfile:     "cloud",
	}
	older := &stableFakeProvider{}
	newer := child.provider
	child.provider = newer
	sup.rememberSessionIdentity("provisional-old", effectiveIdentity{Backend: "agy", Agent: "old-agent", Model: "old-model"}, older)
	sup.rememberSessionIdentity("provisional-new", effectiveIdentity{Backend: "pi", Agent: "new-agent", Model: "new-model", AgentProfile: "cloud"}, newer)

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		sup.adoptChildSessionID(child, older, "provisional-old", "conv-old")
	}()
	sup.adoptChildSessionID(child, newer, "provisional-new", "conv-new")
	close(start)
	wg.Wait()

	identity, ok := sup.sessionIdentity("conv-new")
	if !ok || identity.Backend != "pi" || identity.Agent != "new-agent" {
		t.Fatalf("new authoritative identity = %#v, ok=%v", identity, ok)
	}
	if _, ok := sup.sessionIdentity("conv-old"); ok {
		t.Fatal("stale adoption installed an old authoritative identity")
	}
}

func TestStableFollowUpUsesAuthoritativeEffectiveIdentity(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage5-followup-map.sock", MaxRuntimes: 1})
	provider := &thinkingCaptureProvider{}
	child := &childRuntime{
		provider:         provider,
		backend:          cli.DefaultBackend,
		effectiveBackend: "pi",
		effectiveAgent:   "agent",
		effectiveModel:   "model",
		agentProfile:     "cloud",
		label:            "follow-up",
		dir:              "/work",
		thinking:         "high",
	}
	sup.rememberSessionIdentity("authoritative", effectiveIdentity{Backend: "pi", Agent: "mapped-agent", Model: "mapped-model", AgentProfile: "cloud"}, provider)
	if _, err := sup.attemptSession(context.Background(), child, "authoritative"); err != nil {
		t.Fatalf("attemptSession: %v", err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.opts) != 1 || provider.opts[0].Agent != "mapped-agent" || provider.opts[0].Model != "mapped-model" || provider.opts[0].AgentProfile != "cloud" {
		t.Fatalf("follow-up options = %+v", provider.opts)
	}
}

func TestSpawnParamsRosterFieldsRoundTrip(t *testing.T) {
	var params SpawnParams
	if err := json.Unmarshal([]byte(`{"prompt":"work","roster_file":"roster.json","roster_entry":"planner"}`), &params); err != nil {
		t.Fatal(err)
	}
	if params.RosterFile != "roster.json" || params.RosterEntry != "planner" {
		t.Fatalf("params = %+v", params)
	}
}
