package stable

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// stubPooOnPath writes an executable named poo into a fresh directory and puts
// that directory alone on PATH, so a roster entry that names no command
// resolves poo from PATH.
func stubPooOnPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "poo")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	return path
}

// mcpLaunchFieldsSupervisor is a supervisor whose provider factory records
// every start option it is handed, so a refused spawn can prove no process was
// created and an allowed one can inspect the launch it carried.
func mcpLaunchFieldsSupervisor(t *testing.T) (*Supervisor, *[]runtime.StartOptions) {
	t.Helper()
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/mcp-launch-fields.sock", MaxRuntimes: 2})
	var created []runtime.StartOptions
	sup.newProviderFunc = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		created = append(created, opts)
		return scriptedStage5Provider("ses_poo", "end_turn"), nil
	}
	t.Cleanup(func() { sup.Shutdown("kill") })
	return sup, &created
}

// waitSeedFinished blocks until the seeded run has recorded its session's
// authoritative identity and released ownership of it, which is what a resume
// reads and claims.
func waitSeedFinished(t *testing.T, sup *Supervisor, result SpawnResult) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := sup.sessionIdentity(result.SessionID); ok {
			sup.sessionIdentityMu.Lock()
			_, owned := sup.sessionOwners[result.SessionID]
			sup.sessionIdentityMu.Unlock()
			if !owned {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("seed session %q never finished", result.SessionID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSpawnMCPRefusesRosterLaunchFields proves an MCP caller cannot smuggle a
// configured executable, arguments or settings through the roster it names:
// each launch field is refused before a provider, a runtime, or an idempotency
// record exists, and the refusal names the entry and the field.
func TestSpawnMCPRefusesRosterLaunchFields(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry string
		field string
	}{
		{"command", `{"backend":"poo","agent":"search","command":"/opt/bin/umpire-poo"}`, "command"},
		{"args", `{"backend":"poo","agent":"search","args":["serve","--verbose"]}`, "args"},
		{"settings", `{"backend":"poo","agent":"search","settings":"/etc/poo.yaml"}`, "settings"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubPooOnPath(t)
			sup, created := mcpLaunchFieldsSupervisor(t)
			dir := t.TempDir()
			rosterPath := writeStage5Roster(t, dir, `{"tool":`+tc.entry+`}`)
			raw := `{"dir":"` + dir + `","prompt":"hi","roster_file":"` + rosterPath +
				`","roster_entry":"tool","idempotency_key":"mcp-launch-fields"}`

			_, err := sup.SpawnMCP([]byte(raw))
			want := `roster entry "tool": launch fields are disabled for MCP spawns (` + tc.field + `)`
			if err == nil || err.Error() != want {
				t.Fatalf("SpawnMCP() error = %v, want %q", err, want)
			}
			if len(*created) != 0 {
				t.Fatal("provider was created for a refused launch")
			}
			if runtimeCount(sup) != 0 {
				t.Fatal("runtime was registered for a refused launch")
			}
			sup.idempotency.mu.Lock()
			recorded := len(sup.idempotency.entries) + len(sup.idempotency.inflight)
			sup.idempotency.mu.Unlock()
			if recorded != 0 {
				t.Fatal("idempotency record was created for a refused launch")
			}

			// The same key, against an entry that names no launch field, is a
			// spawn rather than a replay of the refusal.
			writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search"}}`)
			out, err := sup.SpawnMCP([]byte(raw))
			if err != nil {
				t.Fatalf("SpawnMCP() with no launch fields = %v, want a spawn", err)
			}
			result, ok := out.(SpawnResult)
			if !ok {
				t.Fatalf("SpawnMCP() = %T, want SpawnResult", out)
			}
			sup.cancelRuntime(result.RuntimeID)
		})
	}
}

// TestSpawnMCPAllowsRosterEntryWithoutLaunchFields proves the gate refuses only
// a caller-chosen launch: an entry that names none of the fields still spawns
// over MCP, with the backend's own default identity.
func TestSpawnMCPAllowsRosterEntryWithoutLaunchFields(t *testing.T) {
	stub := stubPooOnPath(t)
	sup, created := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search"}}`)

	out, err := sup.SpawnMCP([]byte(`{"dir":"` + dir + `","prompt":"hi","roster_file":"` + rosterPath + `","roster_entry":"tool"}`))
	if err != nil {
		t.Fatalf("SpawnMCP() = %v, want nil", err)
	}
	result, ok := out.(SpawnResult)
	if !ok {
		t.Fatalf("SpawnMCP() = %T, want SpawnResult", out)
	}
	sup.cancelRuntime(result.RuntimeID)

	if len(*created) != 1 {
		t.Fatalf("provider creations = %d, want 1", len(*created))
	}
	absStub, err := filepath.Abs(stub)
	if err != nil {
		t.Fatal(err)
	}
	want := backendlaunch.Spec{Command: absStub, Args: []string{"serve"}}
	if got := (*created)[0].Launch; !got.Equal(want) {
		t.Fatalf("start options launch = %+v, want the poo default %+v", got, want)
	}
}

// TestSpawnMCPRefusesNonDefaultLaunchOnResume proves the gate sees the
// inherited launch, mirroring the wrapper gate: a session started by the
// operator-trusted spawn surface with a configured executable cannot be
// continued over MCP.
func TestSpawnMCPRefusesNonDefaultLaunchOnResume(t *testing.T) {
	stubPooOnPath(t)
	sup, created := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/umpire-poo","args":["serve","--verbose"]}}`)

	seed, err := sup.spawn(SpawnParams{Dir: dir, Prompt: "hi", RosterFile: rosterPath, RosterEntry: "tool"})
	if err != nil {
		t.Fatalf("seed spawn: %v", err)
	}
	defer sup.cancelRuntime(seed.RuntimeID)
	waitSeedFinished(t, sup, seed)

	_, err = sup.SpawnMCP([]byte(`{"dir":"` + dir + `","prompt":"hi","session_id":"` + seed.SessionID +
		`","roster_file":"` + rosterPath + `","roster_entry":"tool"}`))
	want := `roster entry "tool": launch fields are disabled for MCP spawns (command)`
	if err == nil || err.Error() != want {
		t.Fatalf("SpawnMCP() error = %v, want %q", err, want)
	}
	if len(*created) != 1 {
		t.Fatalf("provider creations = %d, want only the seed's", len(*created))
	}
}

// TestSpawnMCPAllowsDefaultLaunchResume proves the refusal is about a
// caller-chosen launch, not about resuming: a session on the backend's default
// launch continues over MCP.
func TestSpawnMCPAllowsDefaultLaunchResume(t *testing.T) {
	stubPooOnPath(t)
	sup, _ := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search"}}`)

	seed, err := sup.spawn(SpawnParams{Dir: dir, Prompt: "hi", RosterFile: rosterPath, RosterEntry: "tool"})
	if err != nil {
		t.Fatalf("seed spawn: %v", err)
	}
	defer sup.cancelRuntime(seed.RuntimeID)
	waitSeedFinished(t, sup, seed)

	out, err := sup.SpawnMCP([]byte(`{"dir":"` + dir + `","prompt":"hi","session_id":"` + seed.SessionID +
		`","roster_file":"` + rosterPath + `","roster_entry":"tool"}`))
	if err != nil {
		t.Fatalf("SpawnMCP() resume = %v, want nil", err)
	}
	resumed, ok := out.(SpawnResult)
	if !ok {
		t.Fatalf("SpawnMCP() = %T, want SpawnResult", out)
	}
	sup.cancelRuntime(resumed.RuntimeID)
}

// TestSpawnWithRosterLaunchFieldsIsAllowed proves the operator-trusted
// control-socket spawn keeps accepting a roster entry's launch fields.
func TestSpawnWithRosterLaunchFieldsIsAllowed(t *testing.T) {
	sup, created := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/umpire-poo","settings":"/etc/poo.yaml"}}`)

	out, err := sup.Spawn([]byte(`{"dir":"` + dir + `","prompt":"hi","roster_file":"` + rosterPath + `","roster_entry":"tool"}`))
	if err != nil {
		t.Fatalf("Spawn() = %v, want nil", err)
	}
	result, ok := out.(SpawnResult)
	if !ok {
		t.Fatalf("Spawn() = %T, want SpawnResult", out)
	}
	sup.cancelRuntime(result.RuntimeID)

	want := backendlaunch.Spec{Command: "/opt/bin/umpire-poo", Args: []string{"serve"}, Settings: "/etc/poo.yaml"}
	if len(*created) != 1 || !(*created)[0].Launch.Equal(want) {
		t.Fatalf("start options launch = %+v, want %+v", *created, want)
	}
}

// TestSpawnMCPLaunchFieldsRefusalIsGeneric proves the refusal names the entry
// and the field, not a backend: a future backend that declares launch fields
// inherits the same gate.
func TestSpawnMCPLaunchFieldsRefusalIsGeneric(t *testing.T) {
	stubPooOnPath(t)
	sup, created := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","args":[]}}`)

	_, err := sup.SpawnMCP([]byte(`{"dir":"` + dir + `","prompt":"hi","roster_file":"` + rosterPath + `","roster_entry":"tool"}`))
	if err == nil {
		t.Fatal("SpawnMCP() with an explicit empty args list = nil, want a refusal")
	}
	if !strings.Contains(err.Error(), `roster entry "tool"`) || !strings.Contains(err.Error(), "(args)") {
		t.Fatalf("SpawnMCP() error = %q, want the entry and the args field named", err)
	}
	if len(*created) != 0 {
		t.Fatal("provider was created for a refused launch")
	}
}
