package stable

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestStableRosterSameBackendResume(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"first":{"backend":"gemini-acp","agent":"planner","model":"planner-model"},
		"second":{"backend":"gemini-acp","agent":"planner","model":"planner-model"}
	}`)
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"roster_file":"roster.json","pre":[
		{"name":"first","prompt":"first","roster_entry":"first"},
		{"name":"second","prompt":"second","roster_entry":"second","resume_from_previous":true}
	]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	provider := &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{
		{sessionID: "stable-stage6-session", events: []stableScriptedEvent{{event: events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "end_turn"}}}}},
		{sessionID: "stable-stage6-session", events: []stableScriptedEvent{{event: events.Event{Event: "session.end", Fields: map[string]any{"stop_reason": "end_turn"}}}}},
	}}
	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage6-stable-same.sock", MaxRuntimes: 1})
	var mu sync.Mutex
	var backends []string
	sup.newProviderFunc = func(_ runtime.StartOptions, backend string) (runtime.Provider, error) {
		mu.Lock()
		backends = append(backends, backend)
		mu.Unlock()
		return provider, nil
	}
	result, err := sup.spawn(SpawnParams{LoopFile: loopPath, Dir: dir, Backend: "gemini-acp"})
	if err != nil {
		t.Fatal(err)
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
	if len(backends) != 2 || backends[0] != "gemini-acp" || backends[1] != "gemini-acp" {
		t.Fatalf("backends=%v, want same-backend provider attempts", backends)
	}
}

func TestStableRosterRejectsCrossBackendResumeBeforeProvider(t *testing.T) {
	dir := t.TempDir()
	writeStage5Roster(t, dir, `{
		"first":{"backend":"gemini-acp","agent":"planner","model":"planner-model"},
		"second":{"backend":"agy","agent":"reviewer","model":"reviewer-model"}
	}`)
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"roster_file":"roster.json","pre":[
		{"name":"first","prompt":"first","roster_entry":"first"},
		{"name":"second","prompt":"second","roster_entry":"second","resume_from_previous":true}
	]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	sup := NewSupervisor(Config{ControlSocket: "/tmp/stage6-stable-cross.sock", MaxRuntimes: 1})
	var mu sync.Mutex
	var backends []string
	sequence := 0
	sup.newProviderFunc = func(_ runtime.StartOptions, backend string) (runtime.Provider, error) {
		mu.Lock()
		defer mu.Unlock()
		backends = append(backends, backend)
		sequence++
		return scriptedStage5Provider(fmt.Sprintf("stage6-cross-%d", sequence), "end_turn"), nil
	}
	result, err := sup.spawn(SpawnParams{LoopFile: loopPath, Dir: dir, Backend: "gemini-acp"})
	if err != nil {
		t.Fatal(err)
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
	if len(backends) != 1 || backends[0] != "gemini-acp" {
		t.Fatalf("provider backends=%v, want no provider for rejected cross-backend resume", backends)
	}
	if identity, ok := sup.sessionIdentity("stage6-cross-1"); !ok || identity.Backend != "gemini-acp" {
		t.Fatalf("prior authoritative identity=%#v ok=%v", identity, ok)
	}
}
