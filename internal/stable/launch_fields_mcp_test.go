package stable

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
	"github.com/sdougbrown/avenor/internal/messaging"
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

// TestSpawnMCPRefusesExplicitEmptyArgs proves an explicit empty args list is a
// declared launch, not an absent one: the presence rule that refuses it here is
// the one the ownership record uses to tell `"args": []` from no args at all.
func TestSpawnMCPRefusesExplicitEmptyArgs(t *testing.T) {
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

// waitForLaunch blocks until a provider creation carries the given command.
func waitForLaunch(t *testing.T, created *[]runtime.StartOptions, command string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, opts := range *created {
			if opts.Launch.Command == command {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a provider launch of %q; created = %+v", command, *created)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// writeWorkflowConfig writes a loop or team config beside the roster its phases
// select, and returns the config path. The roster path is relative to the
// config's own directory, which is how a workflow resolves it.
func writeWorkflowConfig(t *testing.T, dir, file string) string {
	t.Helper()
	body := `{"max_iterations":1,"roster_file":"roster.json","loop":[{"name":"phase","prompt":"go","roster_entry":"tool"}]}`
	if file == "team_file" {
		body = `{"roster_file":"roster.json","team":[{"name":"phase","prompt":"go","roster_entry":"tool"}]}`
	}
	path := filepath.Join(dir, file+".json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestSpawnMCPRefusesWorkflowSpawns proves the workflow exemption is gone: a
// loop or team config names templates and a roster whose paths the caller
// supplied, so their phases are as caller-authored as a roster entry. The gated
// verb refuses them before any reservation, runtime or idempotency record; the
// operator-trusted spawn verb is where a workflow starts.
func TestSpawnMCPRefusesWorkflowSpawns(t *testing.T) {
	for _, file := range []string{"loop_file", "team_file"} {
		t.Run(file, func(t *testing.T) {
			stubPooOnPath(t)
			sup, created := mcpLaunchFieldsSupervisor(t)
			dir := t.TempDir()
			writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/evil-poo"}}`)
			configPath := writeWorkflowConfig(t, dir, file)

			raw := `{"dir":"` + dir + `","` + file + `":"` + configPath + `","idempotency_key":"mcp-workflow"}`
			_, err := sup.SpawnMCP([]byte(raw))
			want := "spawn_mcp: loop_file and team_file are not accepted; workflow spawns use the spawn verb"
			if err == nil || err.Error() != want {
				t.Fatalf("SpawnMCP(%s) error = %v, want %q", file, err, want)
			}
			if len(*created) != 0 {
				t.Fatal("provider was created for a refused workflow spawn")
			}
			if runtimeCount(sup) != 0 {
				t.Fatal("runtime was registered for a refused workflow spawn")
			}
			sup.idempotency.mu.Lock()
			recorded := len(sup.idempotency.entries) + len(sup.idempotency.inflight)
			sup.idempotency.mu.Unlock()
			if recorded != 0 {
				t.Fatal("idempotency record was created for a refused workflow spawn")
			}
		})
	}
}

// TestSpawnAllowsWorkflowSpawn pins that the trusted spawn verb keeps starting
// workflows: the refusal is on the MCP-gated verb only, and a workflow's phases
// still resolve their roster entry's configured launch.
func TestSpawnAllowsWorkflowSpawn(t *testing.T) {
	for _, file := range []string{"loop_file", "team_file"} {
		t.Run(file, func(t *testing.T) {
			stubPooOnPath(t)
			sup, created := mcpLaunchFieldsSupervisor(t)
			dir := t.TempDir()
			writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/umpire-poo"}}`)
			configPath := writeWorkflowConfig(t, dir, file)

			out, err := sup.Spawn([]byte(`{"dir":"` + dir + `","` + file + `":"` + configPath + `"}`))
			if err != nil {
				t.Fatalf("Spawn(%s) error = %v, want the workflow to start", file, err)
			}
			result, ok := out.(SpawnResult)
			if !ok {
				t.Fatalf("Spawn() = %T, want SpawnResult", out)
			}
			defer sup.cancelRuntime(result.RuntimeID)

			waitForLaunch(t, created, "/opt/bin/umpire-poo")
		})
	}
}

// TestMCPToolSpawnIsRefusedByGatedVerb drives the parameter shape the MCP spawn
// tool builds — dir, label, the run's artifact paths and a caller-named roster
// entry, no wrapper — into the gated verb, which is now the only verb the tool
// uses. The caller-chosen command never reaches a provider.
func TestMCPToolSpawnIsRefusedByGatedVerb(t *testing.T) {
	stubPooOnPath(t)
	sup, created := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/evil-poo","args":["serve","--profile"],"settings":"/tmp/evil-settings.yaml"}}`)

	raw := `{"dir":"` + dir + `","label":"mcp-tool-run","prompt":"hi",` +
		`"sentinel_file":"` + filepath.Join(dir, "sentinel.env") + `",` +
		`"on_event":"` + filepath.Join(dir, "events.ndjson") + `",` +
		`"roster_file":"` + rosterPath + `","roster_entry":"tool"}`
	_, err := sup.SpawnMCP([]byte(raw))
	want := `roster entry "tool": launch fields are disabled for MCP spawns (command)`
	if err == nil || err.Error() != want {
		t.Fatalf("SpawnMCP() error = %v, want %q", err, want)
	}
	if len(*created) != 0 {
		t.Fatal("provider was created for the MCP tool's refused launch")
	}
}

// finishSeedForResume ends the seeded run the way a finished backend leaves it:
// the runtime stops, and its sentinel records the session the resume continues.
func finishSeedForResume(t *testing.T, sup *Supervisor, seed SpawnResult, sentinel string) {
	t.Helper()
	if err := sup.RuntimeCancel(seed.RuntimeID); err != nil {
		t.Fatalf("cancel seed: %v", err)
	}
	child := sup.runtimeByID(seed.RuntimeID)
	if child == nil {
		t.Fatalf("seed runtime %q is not registered", seed.RuntimeID)
	}
	select {
	case <-child.runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("seed run did not stop")
	}
	writeSentinel(t, sentinel, "DONE", seed.SessionID)
}

// TestMCPResumeRefusesConfiguredLaunch proves the cold-resume arm of an
// MCP-originated resume is gated: continuing a run whose configured launch is
// not the backend's own default would re-exec a caller-chosen executable, so
// the message tool's resume path refuses it exactly as spawn_mcp does. The
// refusal is the gate answering, not the unrelated selector conflict a
// roster-created resume still hits: it lands first, before the spawn validates
// the inherited selectors.
func TestMCPResumeRefusesConfiguredLaunch(t *testing.T) {
	stubPooOnPath(t)
	sup, created := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "seed.env")
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/evil-poo"}}`)

	seed, err := sup.spawn(SpawnParams{Dir: dir, Prompt: "hi", RosterFile: rosterPath, RosterEntry: "tool", SentinelFile: sentinel})
	if err != nil {
		t.Fatalf("seed spawn: %v", err)
	}
	defer sup.cancelRuntime(seed.RuntimeID)
	waitSeedFinished(t, sup, seed)
	finishSeedForResume(t, sup, seed, sentinel)

	_, err = sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: messaging.SupervisorRunID}, messaging.ResumeRequest{
		RunID:   resumeUUID1,
		Target:  seed.RuntimeID,
		Message: "continue",
		MCP:     true,
	})
	want := `roster entry "tool": launch fields are disabled for MCP spawns (command)`
	if err == nil || err.Error() != want {
		t.Fatalf("Resume() error = %v, want %q", err, want)
	}
	if len(*created) != 1 {
		t.Fatalf("provider creations = %d, want only the seed's", len(*created))
	}
}

// TestMCPResumeAllowsDefaultLaunch proves the resume gate is about the launch,
// not about resuming: an MCP-originated cold resume of a run on the backend's
// own default launch continues.
func TestMCPResumeAllowsDefaultLaunch(t *testing.T) {
	stubPooOnPath(t)
	sup, created := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "seed.env")

	seed, err := sup.spawn(SpawnParams{Dir: dir, Prompt: "hi", Backend: "poo", SentinelFile: sentinel})
	if err != nil {
		t.Fatalf("seed spawn: %v", err)
	}
	defer sup.cancelRuntime(seed.RuntimeID)
	waitSeedFinished(t, sup, seed)
	finishSeedForResume(t, sup, seed, sentinel)

	out, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: messaging.SupervisorRunID}, messaging.ResumeRequest{
		RunID:   resumeUUID1,
		Target:  seed.RuntimeID,
		Message: "continue",
		MCP:     true,
	})
	if err != nil {
		t.Fatalf("Resume() = %v, want the default launch to continue", err)
	}
	defer sup.cancelRuntime(out.RuntimeID)
	if len(*created) != 2 {
		t.Fatalf("provider creations = %d, want the seed and the resumed run", len(*created))
	}
}

// TestMCPGateIdentityIsWhatTheSpawnUses proves the gate and the spawn share one
// read of the roster file. The roster is rewritten between the two — the
// window a concurrent writer would use — and the spawn still starts with the
// launch the gate approved instead of the rewritten command.
func TestMCPGateIdentityIsWhatTheSpawnUses(t *testing.T) {
	stub := stubPooOnPath(t)
	sup, created := mcpLaunchFieldsSupervisor(t)
	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search"}}`)

	params, err := parseStableSpawn([]byte(`{"dir":"` + dir + `","prompt":"hi","roster_file":"` + rosterPath + `","roster_entry":"tool"}`))
	if err != nil {
		t.Fatalf("parse spawn params: %v", err)
	}
	gated, err := sup.authorizeMCPLaunchWrapper(params)
	if err != nil {
		t.Fatalf("gate refused an entry that declares no launch field: %v", err)
	}

	// Time of check is over; the caller's roster now names an executable.
	writeStage5Roster(t, dir, `{"tool":{"backend":"poo","agent":"search","command":"/opt/bin/evil-poo"}}`)

	out, err := sup.finishSpawn(gated)
	if err != nil {
		t.Fatalf("spawn after the rewrite: %v", err)
	}
	result, ok := out.(SpawnResult)
	if !ok {
		t.Fatalf("finishSpawn() = %T, want SpawnResult", out)
	}
	sup.cancelRuntime(result.RuntimeID)

	if len(*created) != 1 {
		t.Fatalf("provider creations = %d, want 1", len(*created))
	}
	if got := (*created)[0].Launch.Command; got != stub {
		t.Fatalf("launch command = %q, want the gated %q, not the rewritten roster's", got, stub)
	}
}
