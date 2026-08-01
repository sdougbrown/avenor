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

func TestRunWorkflowRosterSameBackendResume(t *testing.T) {
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
	if got := run([]string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath}, func(string) string { return "" }, &stderr); got != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", got, stderr.String())
	}
	starts, resumes := provider.counts()
	if starts != 1 || resumes != 1 {
		t.Fatalf("starts=%d resumes=%d, want one start and one same-backend resume", starts, resumes)
	}
	backendsMu.Lock()
	defer backendsMu.Unlock()
	if len(backends) != 2 || backends[0] != "gemini-acp" || backends[1] != "gemini-acp" {
		t.Fatalf("backends=%v, want same roster backend", backends)
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
	if err := mapping.claim("provisional", "gemini-acp", first, ""); err != nil {
		t.Fatal(err)
	}
	if !mapping.adopt("provisional", "authoritative", "gemini-acp", first) {
		t.Fatal("initial adoption was rejected")
	}
	mapping.finish("provisional", "authoritative", "gemini-acp", first)
	if _, ok := mapping.backend("provisional"); ok {
		t.Fatal("provisional alias survived attempt cleanup")
	}

	second := &cliSessionAttempt{}
	if err := mapping.claim("authoritative", "gemini-acp", second, "authoritative"); err != nil {
		t.Fatalf("same-backend retry claim: %v", err)
	}
	if mapping.adopt("provisional", "late-authoritative", "gemini-acp", first) {
		t.Fatal("late callback from old attempt was accepted")
	}
	if got, ok := mapping.backend("authoritative"); !ok || got != "gemini-acp" {
		t.Fatalf("authoritative mapping = %q, %v; want retry backend", got, ok)
	}
}
