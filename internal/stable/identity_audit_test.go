package stable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestSessionAttemptRejectsConcurrentProviderIDCollision(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-session-collision.sock", MaxRuntimes: 1})
	providers := []runtime.Provider{&stableFakeProvider{}, &stableFakeProvider{}}
	identities := []effectiveIdentity{
		{Backend: "pi", Agent: "first", Model: "model-first", AgentProfile: "cloud"},
		{Backend: "agy", Agent: "second", Model: "model-second", AgentProfile: "local"},
	}

	type result struct {
		index   int
		attempt *sessionAttempt
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i := range providers {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			attempt, err := sup.registerSessionAttempt("shared-session", identities[index], providers[index], "")
			results <- result{index: index, attempt: attempt, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	var winner result
	successes := 0
	for got := range results {
		if got.err == nil {
			successes++
			winner = got
		}
	}
	if successes != 1 {
		t.Fatalf("successful claims = %d, want exactly one", successes)
	}
	mapped, ok := sup.sessionIdentity("shared-session")
	if !ok || !authoritativeIdentityEqual(mapped, identities[winner.index]) {
		t.Fatalf("mapped identity = %#v, want winner %#v", mapped, identities[winner.index])
	}
	sup.releaseSessionAttempt(winner.attempt)
}

func TestSessionAttemptRejectsAuthoritativeIDCollision(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-authoritative-collision.sock", MaxRuntimes: 1})
	firstProvider, secondProvider := &stableFakeProvider{}, &stableFakeProvider{}
	firstIdentity := effectiveIdentity{Backend: "pi", Agent: "first", Model: "first-model"}
	secondIdentity := effectiveIdentity{Backend: "agy", Agent: "second", Model: "second-model"}
	first, err := sup.registerSessionAttempt("pending-first", firstIdentity, firstProvider, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := sup.registerSessionAttempt("pending-second", secondIdentity, secondProvider, "")
	if err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{provider: secondProvider, session: runtime.Session{SessionID: "pending-second"}}
	if !sup.adoptSessionAttempt(child, first, "pending-first", "shared-authoritative") {
		t.Fatal("first authoritative adoption was rejected")
	}
	if sup.adoptSessionAttempt(child, second, "pending-second", "shared-authoritative") {
		t.Fatal("second provider claimed an already-owned authoritative ID")
	}
	if mapped, ok := sup.sessionIdentity("shared-authoritative"); !ok || !authoritativeIdentityEqual(mapped, firstIdentity) {
		t.Fatalf("authoritative collision overwrote identity: %#v, ok=%v", mapped, ok)
	}
	sup.releaseSessionAttempt(first)
	sup.releaseSessionAttempt(second)
}

func TestStableTeamAuthoritativeIDCollisionFailsWorkflowDeterministically(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"first":{"backend":"pi","agent":"first","model":"model-first"},
		"second":{"backend":"agy","agent":"second","model":"model-second"}
	}`)
	teamPath := filepath.Join(dir, "collision-team.json")
	if err := os.WriteFile(teamPath, []byte(`{"roster_file":"roster.json","team":[
		{"name":"first","prompt":"first","roster_entry":"first"},
		{"name":"second","prompt":"second","roster_entry":"second"}
	]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	first := &stableDeferredProvider{
		provisionalID: "pending-first", realID: "shared-authoritative", backend: "pi",
		events: deferredStartEndEvents("shared-authoritative", nil),
	}
	second := &stableDeferredProvider{
		provisionalID: "pending-second", realID: "shared-authoritative", backend: "agy",
		events: deferredStartEndEvents("shared-authoritative", nil),
	}
	sentinelPath := filepath.Join(dir, "collision.done")
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-team-collision-e2e.sock", MaxRuntimes: 1})
	sup.newProviderFunc = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		if opts.Agent == "first" {
			return first, nil
		}
		return second, nil
	}
	spawned, err := sup.spawn(SpawnParams{TeamFile: teamPath, Dir: dir, SentinelFile: sentinelPath, AgentProfile: "cloud"})
	if err != nil {
		t.Fatal(err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[spawned.RuntimeID]
	sup.controlMu.Unlock()
	waitStage5Done(t, child)

	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatal(err)
	}
	status := statusAny.(map[string]any)
	if status["exit_code"] != 1 || status["status"] != "ended" {
		t.Fatalf("collision workflow status = %#v, want failed terminal attempt", status)
	}
	// Team configuration order, never callback timing, owns the tombstone even
	// though either provider may win the shared authoritative ID race.
	if status["effective_agent"] != "second" || status["effective_model"] != "model-second" || status["agent_profile"] != "cloud" {
		t.Fatalf("collision tombstone identity = %#v, want deterministic second member", status)
	}
	data, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "FAILED") {
		t.Fatalf("collision sentinel = %q, want FAILED", data)
	}
	mapped, ok := sup.sessionIdentity("shared-authoritative")
	if !ok || (mapped.Agent != "first" && mapped.Agent != "second") {
		t.Fatalf("winning authoritative mapping = %#v, ok=%v", mapped, ok)
	}
}

func TestStableParallelTeamAdoptionUsesPerAttemptOwnership(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"first":{"backend":"pi","agent":"first","model":"model-first"},
		"second":{"backend":"agy","agent":"second","model":"model-second"}
	}`)
	teamPath := filepath.Join(dir, "team.json")
	if err := os.WriteFile(teamPath, []byte(`{"roster_file":"roster.json","team":[
		{"name":"first","prompt":"first","roster_entry":"first"},
		{"name":"second","prompt":"second","roster_entry":"second"}
	]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	firstStart, firstEnd := make(chan struct{}), make(chan struct{})
	secondStart, secondEnd := make(chan struct{}), make(chan struct{})
	first := &stableDeferredProvider{
		provisionalID: "pending-first", realID: "authoritative-first", backend: "pi",
		events: deferredGatedEvents("authoritative-first", firstStart, firstEnd),
	}
	second := &stableDeferredProvider{
		provisionalID: "pending-second", realID: "authoritative-second", backend: "agy",
		events: deferredGatedEvents("authoritative-second", secondStart, secondEnd),
	}
	providersStarted := make(chan struct{}, 2)
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-team-interleave.sock", MaxRuntimes: 1})
	sup.newProviderFunc = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		providersStarted <- struct{}{}
		if opts.Agent == "first" {
			return first, nil
		}
		return second, nil
	}

	spawned, err := sup.spawn(SpawnParams{TeamFile: teamPath, Dir: dir, AgentProfile: "cloud"})
	if err != nil {
		t.Fatal(err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[spawned.RuntimeID]
	sup.controlMu.Unlock()
	for i := 0; i < 2; i++ {
		select {
		case <-providersStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("team providers did not both start")
		}
	}

	// Release the provider that does not own the aggregate presentation slot.
	// The old implementation rejected exactly this legitimate adoption.
	child.mu.Lock()
	aggregate := child.provider
	child.mu.Unlock()
	var nonAggregateStart, nonAggregateEnd chan struct{}
	var nonAggregateID string
	if aggregate == first {
		nonAggregateStart, nonAggregateEnd, nonAggregateID = secondStart, secondEnd, "authoritative-second"
	} else {
		nonAggregateStart, nonAggregateEnd, nonAggregateID = firstStart, firstEnd, "authoritative-first"
	}
	close(nonAggregateStart)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := sup.sessionIdentity(nonAggregateID); ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if identity, ok := sup.sessionIdentity(nonAggregateID); !ok || identity.AgentProfile != "cloud" {
		t.Fatalf("non-aggregate authoritative identity = %#v, ok=%v", identity, ok)
	}
	close(nonAggregateEnd)

	if nonAggregateID == "authoritative-first" {
		close(secondStart)
		close(secondEnd)
	} else {
		close(firstStart)
		close(firstEnd)
	}
	waitStage5Done(t, child)

	// Team order, not callback interleaving, defines the final phase identity.
	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatal(err)
	}
	status := statusAny.(map[string]any)
	for key, want := range map[string]any{
		"session_id": "authoritative-second", "effective_backend": "agy",
		"effective_agent": "second", "effective_model": "model-second",
		"agent_profile": "cloud", "roster_entry": "second",
	} {
		if status[key] != want {
			t.Fatalf("status[%s] = %v, want %v; status=%#v", key, status[key], want, status)
		}
	}
}

func TestStableWorkflowRejectsSameBackendIdentityChangeOnResume(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"first":{"backend":"pi","agent":"first","model":"first-model"},
		"second":{"backend":"pi","agent":"second","model":"second-model"}
	}`)
	loopPath := filepath.Join(dir, "resume-conflict.json")
	if err := os.WriteFile(loopPath, []byte(`{"roster_file":"roster.json","pre":[
		{"name":"first","prompt":"first","roster_entry":"first"},
		{"name":"second","prompt":"second","roster_entry":"second","resume_from_previous":true}
	]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-workflow-resume-conflict.sock", MaxRuntimes: 1})
	var mu sync.Mutex
	providerCalls := 0
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		mu.Lock()
		providerCalls++
		mu.Unlock()
		return scriptedStage5Provider("workflow-resume-session", "end_turn"), nil
	}
	sentinelPath := filepath.Join(dir, "resume-conflict.done")
	spawned, err := sup.spawn(SpawnParams{LoopFile: loopPath, Dir: dir, SentinelFile: sentinelPath, MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[spawned.RuntimeID]
	sup.controlMu.Unlock()
	waitStage5Done(t, child)
	mu.Lock()
	calls := providerCalls
	mu.Unlock()
	if calls != 1 {
		t.Fatalf("provider calls = %d, want second conflicting phase rejected before provider creation", calls)
	}
	if identity, ok := sup.sessionIdentity("workflow-resume-session"); !ok || identity.Agent != "first" || identity.Model != "first-model" {
		t.Fatalf("authoritative workflow identity = %#v, ok=%v", identity, ok)
	}
	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatal(err)
	}
	status := statusAny.(map[string]any)
	if status["session_id"] != "workflow-resume-session" || status["effective_agent"] != "first" || status["effective_model"] != "first-model" || status["exit_code"] != 1 {
		t.Fatalf("failed workflow tombstone = %#v, want finalized first-phase identity", status)
	}
	data, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); !strings.Contains(got, "FAILED") || !strings.Contains(got, "SESSION=workflow-resume-session") {
		t.Fatalf("failed sentinel = %q, want finalized session", got)
	}
}

func TestStableFailedTeamFinalizesIdentityBeforeErrorReturn(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"first":{"backend":"pi","agent":"first","model":"first-model"},
		"never-started":{"backend":"agy","agent":"second","model":"second-model"}
	}`)
	teamPath := filepath.Join(dir, "team-error.json")
	if err := os.WriteFile(teamPath, []byte(`{"roster_file":"roster.json",
		"pre":[{"name":"first","prompt":"first","roster_entry":"first"}],
		"post":[{"name":"never-started","prompt":"second","roster_entry":"never-started"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sentinelPath := filepath.Join(dir, "team-error.done")
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-team-error-finalize.sock", MaxRuntimes: 1})
	var calls int
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		calls++
		if calls == 1 {
			return scriptedStage5Provider("team-first-session", "end_turn"), nil
		}
		return nil, fmt.Errorf("injected provider failure")
	}
	spawned, err := sup.spawn(SpawnParams{TeamFile: teamPath, Dir: dir, SentinelFile: sentinelPath, AgentProfile: "cloud"})
	if err != nil {
		t.Fatal(err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[spawned.RuntimeID]
	sup.controlMu.Unlock()
	waitStage5Done(t, child)

	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatal(err)
	}
	status := statusAny.(map[string]any)
	if status["exit_code"] != 1 || status["session_id"] != "team-first-session" || status["effective_agent"] != "first" || status["effective_model"] != "first-model" {
		t.Fatalf("failed team tombstone = %#v, want completed first-phase identity", status)
	}
	data, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); !strings.Contains(got, "FAILED") || !strings.Contains(got, "SESSION=team-first-session") {
		t.Fatalf("failed team sentinel = %q, want finalized first session", got)
	}
}

func TestStableDirectResumeRestoresCompleteAuthoritativeIdentity(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-resume-restore.sock", MaxRuntimes: 1})
	mapped := effectiveIdentity{
		Backend: "pi", Agent: "mapped-agent", Model: "mapped-model", AgentProfile: "cloud",
		RosterFile: "/repo/original-roster.json", RosterEntry: "planner",
	}
	sup.rememberSessionIdentity("resume-session", mapped, nil)
	provider := &thinkingCaptureProvider{}
	var gotBackend string
	var gotOpts runtime.StartOptions
	sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		gotBackend, gotOpts = backend, opts
		return provider, nil
	}

	result, err := sup.spawn(SpawnParams{
		Prompt: "continue", SessionID: "resume-session", Backend: "pi", Dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sup.cancelRuntime(result.RuntimeID)
	if gotBackend != mapped.Backend || gotOpts.Agent != mapped.Agent || gotOpts.Model != mapped.Model || gotOpts.AgentProfile != mapped.AgentProfile {
		t.Fatalf("restored provider identity: backend=%q opts=%+v", gotBackend, gotOpts)
	}
	if result.RosterFile != mapped.RosterFile || result.RosterEntry != mapped.RosterEntry {
		t.Fatalf("restored roster metadata = %q/%q", result.RosterFile, result.RosterEntry)
	}

	for name, params := range map[string]SpawnParams{
		"backend": {Prompt: "x", SessionID: "resume-session", Backend: "agy"},
		"agent":   {Prompt: "x", SessionID: "resume-session", Agent: "other"},
		"model":   {Prompt: "x", SessionID: "resume-session", Model: "other"},
		"profile": {Prompt: "x", SessionID: "resume-session", AgentProfile: "other"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := sup.spawn(params); err == nil {
				t.Fatal("expected authoritative resume conflict")
			}
		})
	}
}

func TestStableResumeRejectsChangedRosterWithoutOverwritingMapping(t *testing.T) {
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"planner":{"backend":"pi","agent":"changed","model":"changed-model"}}`)
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-resume-roster.sock", MaxRuntimes: 1})
	mapped := effectiveIdentity{
		Backend: "pi", Agent: "original", Model: "original-model", AgentProfile: "cloud",
		RosterFile: rosterPath, RosterEntry: "planner",
	}
	sup.rememberSessionIdentity("roster-session", mapped, nil)
	called := false
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		called = true
		return nil, fmt.Errorf("must not be called")
	}

	_, err := sup.spawn(SpawnParams{
		Prompt: "continue", SessionID: "roster-session", RosterFile: rosterPath,
		RosterEntry: "planner", AgentProfile: "cloud",
	})
	if err == nil {
		t.Fatal("expected changed-roster conflict")
	}
	if called {
		t.Fatal("provider was created before changed-roster conflict")
	}
	if got, ok := sup.sessionIdentity("roster-session"); !ok || !authoritativeIdentityEqual(got, mapped) {
		t.Fatalf("authoritative mapping was overwritten: %#v, ok=%v", got, ok)
	}
}

func TestStableCompletedLoopRetainsFinalRosterIdentity(t *testing.T) {
	testStableCompletedWorkflowRetainsFinalRosterIdentity(t, "loop")
}

func TestStableCompletedTeamRetainsFinalRosterIdentity(t *testing.T) {
	testStableCompletedWorkflowRetainsFinalRosterIdentity(t, "team")
}

func testStableCompletedWorkflowRetainsFinalRosterIdentity(t *testing.T, mode string) {
	t.Helper()
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"first":{"backend":"pi","agent":"first","model":"model-first"},
		"final":{"backend":"agy","agent":"final"}
	}`)
	configPath := filepath.Join(dir, mode+".json")
	var config string
	if mode == "loop" {
		config = `{"roster_file":"roster.json","pre":[{"name":"first","prompt":"first","roster_entry":"first"}],"post":[{"name":"final","prompt":"final","roster_entry":"final"}]}`
	} else {
		config = `{"roster_file":"roster.json","pre":[{"name":"first","prompt":"first","roster_entry":"first"}],"post":[{"name":"final","prompt":"final","roster_entry":"final"}]}`
	}
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	sup := NewSupervisor(Config{ControlSocket: "/tmp/audit-final-" + mode + ".sock", MaxRuntimes: 1})
	var mu sync.Mutex
	sequence := 0
	sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		mu.Lock()
		sequence++
		id := fmt.Sprintf("%s-session-%d", mode, sequence)
		mu.Unlock()
		return scriptedStage5Provider(id, "end_turn"), nil
	}
	params := SpawnParams{Dir: dir, Backend: "gemini-acp", Model: "stale-run-model", AgentProfile: "cloud"}
	if mode == "loop" {
		params.LoopFile = configPath
	} else {
		params.TeamFile = configPath
	}
	spawned, err := sup.spawn(params)
	if err != nil {
		t.Fatal(err)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[spawned.RuntimeID]
	sup.controlMu.Unlock()
	waitStage5Done(t, child)
	if sup.runtimes[child.id] != child {
		t.Fatal("completed workflow status tombstone was removed")
	}
	statusAny, err := sup.RuntimeStatus(child.id)
	if err != nil {
		t.Fatal(err)
	}
	status := statusAny.(map[string]any)
	for key, want := range map[string]any{
		"session_id": mode + "-session-2", "effective_backend": "agy",
		"effective_agent": "final", "effective_model": "",
		"agent_profile": "cloud", "roster_entry": "final", "status": "ended",
	} {
		if status[key] != want {
			t.Fatalf("status[%s] = %v, want %v; status=%#v", key, status[key], want, status)
		}
	}
}
