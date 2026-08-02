package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type stage6Session struct {
	opts   runtime.StartOptions
	events chan events.Event
}

type stage6ResumeProvider struct {
	mu       sync.Mutex
	nextID   int
	sessions map[string]*stage6Session
	starts   int
	resumes  int
}

func newStage6ResumeProvider() *stage6ResumeProvider {
	return &stage6ResumeProvider{sessions: make(map[string]*stage6Session)}
}

func (p *stage6ResumeProvider) Start(_ context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextID++
	id := fmt.Sprintf("stage6-session-%d", p.nextID)
	p.sessions[id] = &stage6Session{opts: opts, events: make(chan events.Event, 2)}
	p.starts++
	return runtime.Session{SessionID: id}, nil
}

func (p *stage6ResumeProvider) Resume(_ context.Context, sessionID string) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	session, ok := p.sessions[sessionID]
	if !ok {
		return runtime.Session{}, fmt.Errorf("unknown session %q", sessionID)
	}
	session.events = make(chan events.Event, 2)
	p.resumes++
	return runtime.Session{SessionID: sessionID}, nil
}

func (p *stage6ResumeProvider) Prompt(_ context.Context, sessionID, _ string) error {
	p.mu.Lock()
	session, ok := p.sessions[sessionID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown session %q", sessionID)
	}
	session.events <- events.Event{Event: "session.end", SessionID: sessionID, Fields: map[string]any{"stop_reason": "end_turn"}}
	close(session.events)
	return nil
}

func (*stage6ResumeProvider) Cancel(context.Context, string) error { return nil }

func (p *stage6ResumeProvider) Events(_ context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	session, ok := p.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("unknown session %q", sessionID)
	}
	return session.events, nil
}

func (*stage6ResumeProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}

func (*stage6ResumeProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func (p *stage6ResumeProvider) counts() (starts, resumes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starts, p.resumes
}

func TestRunWorkflowRosterRejectsSameBackendIdentityChangeBeforeProvider(t *testing.T) {
	oldNewProvider := newProvider
	provider := newStage6ResumeProvider()
	var backendsMu sync.Mutex
	var backends []string
	newProvider = func(_ runtime.StartOptions, backend string) (runtime.Provider, error) {
		backendsMu.Lock()
		backends = append(backends, backend)
		backendsMu.Unlock()
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{
		"first":{"backend":"gemini-acp","agent":"planner","model":"planner-model"},
		"second":{"backend":"gemini-acp","agent":"reviewer","model":"reviewer-model"}
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[
		{"name":"first","prompt":"first","roster_entry":"first"},
		{"name":"second","prompt":"second","roster_entry":"second","resume_from_previous":true}
	]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if got := run([]string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath}, func(string) string { return "" }, &stderr); got != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", got, stderr.String())
	}
	if !strings.Contains(stderr.String(), "cannot resume session") || !strings.Contains(stderr.String(), "agent") {
		t.Fatalf("stderr=%q, want same-backend identity conflict", stderr.String())
	}
	starts, resumes := provider.counts()
	if starts != 1 || resumes != 0 {
		t.Fatalf("starts=%d resumes=%d, want conflict rejected before second provider", starts, resumes)
	}
	backendsMu.Lock()
	defer backendsMu.Unlock()
	if len(backends) != 1 || backends[0] != "gemini-acp" {
		t.Fatalf("backends=%v, want only the first provider", backends)
	}
}

func TestRunWorkflowRosterRejectsCrossBackendResumeBeforeProvider(t *testing.T) {
	oldNewProvider := newProvider
	provider := newStage6ResumeProvider()
	var mu sync.Mutex
	var backends []string
	newProvider = func(_ runtime.StartOptions, backend string) (runtime.Provider, error) {
		mu.Lock()
		backends = append(backends, backend)
		mu.Unlock()
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{
		"first":{"backend":"gemini-acp","agent":"planner","model":"planner-model"},
		"second":{"backend":"agy","agent":"reviewer","model":"reviewer-model"}
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[
		{"name":"first","prompt":"first","roster_entry":"first"},
		{"name":"second","prompt":"second","roster_entry":"second","resume_from_previous":true}
	]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if got := run([]string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath, "--max-retries", "2"}, func(string) string { return "" }, &stderr); got != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", got, stderr.String())
	}
	if !strings.Contains(stderr.String(), "cannot resume session") || !strings.Contains(stderr.String(), "gemini-acp") || !strings.Contains(stderr.String(), "agy") {
		t.Fatalf("stderr=%q, want cross-backend resume error", stderr.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(backends) != 1 || backends[0] != "gemini-acp" {
		t.Fatalf("provider backends=%v, want no provider creation for rejected phase", backends)
	}
}

func TestRunWorkflowRosterAgentOnlyResolvesAgainstEffectiveBackend(t *testing.T) {
	oldNewProvider := newProvider
	provider := newStage6ResumeProvider()
	var gotBackend string
	var gotModel string
	newProvider = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		gotBackend = backend
		gotModel = opts.Model
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "opencode.json"), []byte(`{"agent":{"reviewer":{"model":"provider/reviewer"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"review":{"backend":"opencode-acp","agent":"reviewer"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[{"name":"review","prompt":"review","roster_entry":"review"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	getenv := func(key string) string {
		if key == "OPENCODE_CONFIG_DIR" {
			return configDir
		}
		return ""
	}
	if got := run([]string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath}, getenv, &stderr); got != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
	}
	if gotBackend != "opencode-acp" || gotModel != "provider/reviewer" {
		t.Fatalf("resolved identity backend=%q model=%q", gotBackend, gotModel)
	}
}

func TestSessionBackendMapRetryCleanupRejectsLateProvisionalAdoption(t *testing.T) {
	mapping := newSessionBackendMap()
	first := &cliSessionAttempt{}
	identity := cliSessionIdentity{backend: "gemini-acp", agent: "planner", model: "planner-model", agentProfile: "cloud"}
	if err := mapping.claim("provisional", identity, first, ""); err != nil {
		t.Fatal(err)
	}
	if !mapping.adopt("provisional", "authoritative", first) {
		t.Fatal("initial adoption was rejected")
	}
	mapping.finish("provisional", "authoritative", first)
	if _, ok := mapping.backend("provisional"); ok {
		t.Fatal("provisional alias survived attempt cleanup")
	}

	second := &cliSessionAttempt{}
	if err := mapping.claim("authoritative", identity, second, "authoritative"); err != nil {
		t.Fatalf("same-backend retry claim: %v", err)
	}
	if mapping.adopt("provisional", "late-authoritative", first) {
		t.Fatal("late callback from old attempt was accepted")
	}
	if got, ok := mapping.backend("authoritative"); !ok || got != "gemini-acp" {
		t.Fatalf("authoritative mapping = %q, %v; want retry backend", got, ok)
	}
}

func TestSessionBackendMapResumeCanReadoptMappedAuthoritativeID(t *testing.T) {
	mapping := newSessionBackendMap()
	identity := cliSessionIdentity{backend: "gemini-acp", agent: "planner", model: "planner-model"}
	first := &cliSessionAttempt{}
	if err := mapping.claim("authoritative", identity, first, ""); err != nil {
		t.Fatal(err)
	}
	mapping.finish("authoritative", "authoritative", first)

	resumed := &cliSessionAttempt{}
	if err := mapping.claim("resume-pending", identity, resumed, "authoritative"); err != nil {
		t.Fatal(err)
	}
	if !mapping.adopt("resume-pending", "authoritative", resumed) {
		t.Fatal("resume could not re-adopt its own mapped authoritative ID")
	}
	mapping.finish("resume-pending", "authoritative", resumed)
	if _, ok := mapping.identity("resume-pending"); ok {
		t.Fatal("resume provisional alias survived cleanup")
	}
	if got, ok := mapping.identity("authoritative"); !ok || got != identity {
		t.Fatalf("authoritative identity = %#v, ok=%v", got, ok)
	}
}

func TestSessionBackendMapRejectedResumePreservesAuthoritativeMapping(t *testing.T) {
	mapping := newSessionBackendMap()
	identity := cliSessionIdentity{backend: "gemini-acp", agent: "planner", model: "planner-model"}
	initial := &cliSessionAttempt{}
	if err := mapping.claim("resume-session", identity, initial, ""); err != nil {
		t.Fatal(err)
	}
	mapping.finish("resume-session", "resume-session", initial)

	collisionOwner := &cliSessionAttempt{}
	collisionIdentity := cliSessionIdentity{backend: "agy", agent: "other", model: "other-model"}
	if err := mapping.claim("occupied", collisionIdentity, collisionOwner, ""); err != nil {
		t.Fatal(err)
	}
	mapping.finish("occupied", "occupied", collisionOwner)

	resumed := &cliSessionAttempt{}
	if err := mapping.claim("resume-session", identity, resumed, "resume-session"); err != nil {
		t.Fatal(err)
	}
	if mapping.adopt("resume-session", "occupied", resumed) {
		t.Fatal("resume adopted another session's authoritative ID")
	}
	mapping.finish("resume-session", "resume-session", resumed)

	if got, ok := mapping.identity("resume-session"); !ok || got != identity {
		t.Fatalf("resume mapping = %#v, ok=%v; want preserved identity %#v", got, ok, identity)
	}
	if got, ok := mapping.identity("occupied"); !ok || got != collisionIdentity {
		t.Fatalf("collision mapping = %#v, ok=%v; want preserved identity %#v", got, ok, collisionIdentity)
	}
}

func TestSessionBackendMapRestoresCompleteIdentityAndRejectsSameBackendConflict(t *testing.T) {
	mapping := newSessionBackendMap()
	owner := &cliSessionAttempt{}
	want := cliSessionIdentity{backend: "gemini-acp", agent: "planner", model: "planner-model", agentProfile: "cloud"}
	if err := mapping.claim("session", want, owner, ""); err != nil {
		t.Fatal(err)
	}
	mapping.finish("session", "session", owner)

	got, err := mapping.resolveResume("session", cliSessionIdentity{backend: "gemini-acp"})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("restored identity = %#v, want %#v", got, want)
	}
	for name, supplied := range map[string]cliSessionIdentity{
		"agent":   {backend: "gemini-acp", agent: "reviewer"},
		"model":   {backend: "gemini-acp", model: "reviewer-model"},
		"profile": {backend: "gemini-acp", agentProfile: "local"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := mapping.resolveResume("session", supplied); err == nil {
				t.Fatal("expected complete identity conflict")
			}
		})
	}
}
