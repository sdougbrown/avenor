package factory_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/factory"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

// writeStartRecorder installs a fake backend child on PATH. It appends its PID
// to startPath on every launch, records the allowlisted identity, and answers
// the backend's minimal protocol with respond.
func writeStartRecorder(t *testing.T, bin, startPath, obsPath, respond string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" +
		"printf '%s\\n' \"$$\" >> " + identitytest.ShellQuote(startPath) + "\n" +
		identitytest.Body(obsPath) + respond
	if err := os.WriteFile(filepath.Join(binDir, bin), []byte(body), 0o755); err != nil {
		t.Fatalf("write %s child: %v", bin, err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

const codexResponder = `
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  [ -n "$id" ] || continue
  case "$line" in
    *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
      printf '{"id":"%s","result":{"thread":{"id":"th_child"}}}\n' "$id"
      ;;
    *)
      printf '{"id":"%s","result":{}}\n' "$id"
      ;;
  esac
done
`

const acpResponder = `
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  [ -n "$id" ] || continue
  printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"sessionId":"ses_identity"}}\n' "$id"
done
`

// writePiRecorder installs a fake `pi` child that also records every request it
// receives, so a test can prove a resume sent no thinking RPC.
func writePiRecorder(t *testing.T, startPath, obsPath, rpcPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" + `
if [ "$1" = "--help" ]; then printf '%s\n' '--thinking <level>'; exit 0; fi
` +
		"printf '%s\\n' \"$$\" >> " + identitytest.ShellQuote(startPath) + "\n" +
		identitytest.Body(obsPath) + `
while IFS= read -r line; do
  printf '%s\n' "$line" >> ` + identitytest.ShellQuote(rpcPath) + `
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  [ -n "$id" ] || continue
  printf '{"type":"response","id":"%s","sessionId":"pi_child","success":true}\n' "$id"
done
`
	if err := os.WriteFile(filepath.Join(binDir, "pi"), []byte(body), 0o755); err != nil {
		t.Fatalf("write pi child: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// writeAgyRecorder installs a fake `agy` child that answers --version and then
// emits the print-mode init/result pair for the conversation it was given.
func writeAgyRecorder(t *testing.T, startPath, obsPath string) {
	t.Helper()
	binDir := t.TempDir()
	body := "#!/bin/sh\n" + `
if [ "$1" = "--version" ]; then printf '1.1.7\n'; exit 0; fi
printf '%s\n' "$$" >> ` + identitytest.ShellQuote(startPath) + `
` + identitytest.Body(obsPath) + `
conv=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "--conversation" ]; then conv="$arg"; fi
  prev="$arg"
done
[ -n "$conv" ] || conv="conv_child"
printf '{"event":"init","conversation_id":"%s"}\n' "$conv"
printf '{"event":"result","conversation_id":"%s","result":{"response":"ok","status":"SUCCESS"}}\n' "$conv"
`
	if err := os.WriteFile(filepath.Join(binDir, "agy"), []byte(body), 0o755); err != nil {
		t.Fatalf("write agy child: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func countStarts(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read start record: %v", err)
	}
	return len(strings.Fields(string(raw)))
}

func closeProvider(p runtime.Provider) {
	if closer, ok := p.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

// TestStartSessionResumeCarriesFreshIdentityToChild pins option-aware resume
// dispatch: a resume that starts a new process carries the caller's identity in
// full, including the cleared optional fields of the previous launch.
func TestStartSessionResumeCarriesFreshIdentityToChild(t *testing.T) {
	cases := []struct {
		backend string
		bin     string
		respond string
	}{
		{backend: "codex-app-server", bin: "codex", respond: codexResponder},
		{backend: "opencode-acp", bin: "opencode", respond: acpResponder},
		{backend: "gemini-acp", bin: "gemini", respond: acpResponder},
		{backend: "cursor-acp", bin: "agent", respond: acpResponder},
	}
	for _, tc := range cases {
		t.Run(tc.backend, func(t *testing.T) {
			identitytest.PoisonParent(t)
			obsPath := filepath.Join(t.TempDir(), "observed.env")
			startPath := filepath.Join(t.TempDir(), "starts")
			writeStartRecorder(t, tc.bin, startPath, obsPath, tc.respond)

			previous := identitytest.Provenance("rt_previous")
			first, err := factory.NewProvider(runtime.StartOptions{}, tc.backend)
			if err != nil {
				t.Fatalf("NewProvider: %v", err)
			}
			if _, err := first.Start(context.Background(), runtime.StartOptions{
				Dir:        t.TempDir(),
				Provenance: previous,
			}); err != nil {
				t.Fatalf("Start: %v", err)
			}
			closeProvider(first)

			fresh := &runtime.LaunchProvenance{RuntimeID: "rt_fresh", ExecutionID: "rt_fresh", Kind: "run", Attempt: 5}
			second, err := factory.NewProvider(runtime.StartOptions{}, tc.backend)
			if err != nil {
				t.Fatalf("NewProvider: %v", err)
			}
			defer closeProvider(second)
			if _, err := cli.StartSession(context.Background(), second, tc.backend, runtime.StartOptions{
				Dir:        t.TempDir(),
				Provenance: fresh,
			}, "ses_resume"); err != nil {
				t.Fatalf("StartSession resume: %v", err)
			}

			groups := identitytest.ReadGroups(t, obsPath, 2)
			identitytest.Assert(t, groups[1], fresh)
			identitytest.AssertBrokerPreserved(t, groups[1])
			if starts := countStarts(t, startPath); starts != 2 {
				t.Fatalf("launches = %d, want the fresh resume process", starts)
			}
		})
	}
}

// TestStartSessionResumeCarriesIdentityWithExplicitThinking pins the same
// fresh-launch dispatch when the caller also requests a supported thinking
// level: the identity is still the caller's, not a constructor default.
func TestStartSessionResumeCarriesIdentityWithExplicitThinking(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	startPath := filepath.Join(t.TempDir(), "starts")
	writeStartRecorder(t, "codex", startPath, obsPath, codexResponder)

	previous := identitytest.Provenance("rt_previous")
	first, err := factory.NewProvider(runtime.StartOptions{}, "codex-app-server")
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	if _, err := first.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: previous,
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	closeProvider(first)

	fresh := &runtime.LaunchProvenance{RuntimeID: "rt_thinking", ExecutionID: "rt_thinking", Kind: "team", Phase: "reviewer", Attempt: 2}
	second, err := factory.NewProvider(runtime.StartOptions{}, "codex-app-server")
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	defer closeProvider(second)
	if _, err := cli.StartSession(context.Background(), second, "codex-app-server", runtime.StartOptions{
		Dir:        t.TempDir(),
		Thinking:   "high",
		Provenance: fresh,
	}, "ses_resume"); err != nil {
		t.Fatalf("StartSession resume: %v", err)
	}

	groups := identitytest.ReadGroups(t, obsPath, 2)
	identitytest.Assert(t, groups[1], fresh)
	if starts := countStarts(t, startPath); starts != 2 {
		t.Fatalf("launches = %d, want the fresh resume process", starts)
	}
}

// TestStartSessionResumeKeepsLiveLaunchIdentity pins the other half: resuming a
// still-live process delivers nothing, so the child keeps its launch identity.
func TestStartSessionResumeKeepsLiveLaunchIdentity(t *testing.T) {
	cases := []struct {
		backend string
		bin     string
		respond string
	}{
		{backend: "codex-app-server", bin: "codex", respond: codexResponder},
		{backend: "opencode-acp", bin: "opencode", respond: acpResponder},
	}
	for _, tc := range cases {
		t.Run(tc.backend, func(t *testing.T) {
			identitytest.PoisonParent(t)
			obsPath := filepath.Join(t.TempDir(), "observed.env")
			startPath := filepath.Join(t.TempDir(), "starts")
			writeStartRecorder(t, tc.bin, startPath, obsPath, tc.respond)

			launched := identitytest.Provenance("rt_live")
			p, err := factory.NewProvider(runtime.StartOptions{}, tc.backend)
			if err != nil {
				t.Fatalf("NewProvider: %v", err)
			}
			defer closeProvider(p)
			session, err := p.Start(context.Background(), runtime.StartOptions{
				Dir:        t.TempDir(),
				Provenance: launched,
			})
			if err != nil {
				t.Fatalf("Start: %v", err)
			}

			if _, err := cli.StartSession(context.Background(), p, tc.backend, runtime.StartOptions{
				Dir:        t.TempDir(),
				Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_next", ExecutionID: "rt_next", Kind: "team", Phase: "next", Attempt: 4},
			}, session.SessionID); err != nil {
				t.Fatalf("StartSession live resume: %v", err)
			}

			if starts := countStarts(t, startPath); starts != 1 {
				t.Fatalf("launches = %d, want the single live process", starts)
			}
			observed := identitytest.Read(t, obsPath)
			identitytest.Assert(t, observed, launched)
			identitytest.AssertBrokerPreserved(t, observed)
		})
	}
}

// TestStartSessionPiResumeAppliesOnlyPerCallThinking pins the pi adapter at the
// caller level: an identity-only resume sends no thinking RPC and keeps the
// live launch, while an explicit supported level still applies.
func TestStartSessionPiResumeAppliesOnlyPerCallThinking(t *testing.T) {
	identitytest.PoisonParent(t)
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	startPath := filepath.Join(t.TempDir(), "starts")
	rpcPath := filepath.Join(t.TempDir(), "rpc.log")
	writePiRecorder(t, startPath, obsPath, rpcPath)

	launched := identitytest.Provenance("rt_pi")
	p, err := factory.NewProvider(runtime.StartOptions{Thinking: "high"}, "pi")
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	defer closeProvider(p)
	started, err := p.Start(context.Background(), runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: launched,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := os.Remove(rpcPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("clear rpc log: %v", err)
	}

	resumed, err := cli.StartSession(context.Background(), p, "pi", runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_next", ExecutionID: "rt_next", Kind: "run", Attempt: 3},
	}, started.SessionID)
	if err != nil {
		t.Fatalf("StartSession identity resume: %v", err)
	}
	if resumed.PID != started.PID {
		t.Fatalf("resume PID = %d, want the live PID %d", resumed.PID, started.PID)
	}
	if raw, err := os.ReadFile(rpcPath); err == nil && strings.Contains(string(raw), "set_thinking_level") {
		t.Fatalf("identity resume sent a thinking RPC: %s", raw)
	}
	if starts := countStarts(t, startPath); starts != 1 {
		t.Fatalf("launches = %d, want the single live process", starts)
	}
	observed := identitytest.Read(t, obsPath)
	identitytest.Assert(t, observed, launched)

	if _, err := cli.StartSession(context.Background(), p, "pi", runtime.StartOptions{
		Dir:      t.TempDir(),
		Thinking: "high",
	}, started.SessionID); err != nil {
		t.Fatalf("StartSession explicit thinking resume: %v", err)
	}
	raw, err := os.ReadFile(rpcPath)
	if err != nil {
		t.Fatalf("read rpc log: %v", err)
	}
	if !strings.Contains(string(raw), "set_thinking_level") {
		t.Fatalf("explicit thinking resume sent no thinking RPC: %s", raw)
	}
	if starts := countStarts(t, startPath); starts != 1 {
		t.Fatalf("launches = %d, want the explicit resume to keep the process", starts)
	}
}

// TestStartSessionAgyResumeSnapshotsCallerIdentity pins agy's deferred launch:
// the first Prompt starts the process with the identity supplied to the resume,
// not with a later caller mutation.
func TestStartSessionAgyResumeSnapshotsCallerIdentity(t *testing.T) {
	identitytest.PoisonParent(t)
	t.Setenv("AVENOR_AGY_TRANSPORT", "headless")
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	startPath := filepath.Join(t.TempDir(), "starts")
	writeAgyRecorder(t, startPath, obsPath)

	want := &runtime.LaunchProvenance{
		RuntimeID:   "rt_agy",
		ExecutionID: "rt_agy",
		Kind:        "loop",
		Phase:       "body",
		Iteration:   intPtr(2),
		Label:       "agy-label",
		Attempt:     4,
	}
	p, err := factory.NewProvider(runtime.StartOptions{Dir: t.TempDir()}, "agy")
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	defer closeProvider(p)
	session, err := cli.StartSession(context.Background(), p, "agy", runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: want,
	}, "conv_resume")
	if err != nil {
		t.Fatalf("StartSession resume: %v", err)
	}
	want.Attempt = 9
	want.RuntimeID = "rt_mutated"
	*want.Iteration = 9

	if err := p.Prompt(context.Background(), session.SessionID, "hi"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	observed := identitytest.Read(t, obsPath)
	original := &runtime.LaunchProvenance{
		RuntimeID:   "rt_agy",
		ExecutionID: "rt_agy",
		Kind:        "loop",
		Phase:       "body",
		Iteration:   intPtr(2),
		Label:       "agy-label",
		Attempt:     4,
	}
	identitytest.Assert(t, observed, original)
	identitytest.AssertBrokerPreserved(t, observed)

	// Reusing the owned session keeps its launch identity, and the next print
	// run is a same-attempt continuation rather than a new launch.
	if _, err := cli.StartSession(context.Background(), p, "agy", runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: &runtime.LaunchProvenance{RuntimeID: "rt_replacement", ExecutionID: "rt_replacement", Kind: "run", Attempt: 1},
	}, session.SessionID); err != nil {
		t.Fatalf("StartSession same-session reuse: %v", err)
	}
	if err := p.Prompt(context.Background(), session.SessionID, "again"); err != nil {
		t.Fatalf("Prompt continuation: %v", err)
	}
	groups := identitytest.ReadGroups(t, obsPath, 2)
	if len(groups) != 2 {
		t.Fatalf("launches = %d, want the initial print plus its continuation", len(groups))
	}
	for _, group := range groups {
		identitytest.Assert(t, group, original)
	}
}

// TestStartSessionAgyAutoTransportKeepsCallerIdentity pins agy's transport
// selection: the identity supplied to a resume survives the RPC probe and the
// headless fallback that starts the print process.
func TestStartSessionAgyAutoTransportKeepsCallerIdentity(t *testing.T) {
	identitytest.PoisonParent(t)
	t.Setenv("AVENOR_AGY_TRANSPORT", "auto")
	obsPath := filepath.Join(t.TempDir(), "observed.env")
	startPath := filepath.Join(t.TempDir(), "starts")
	writeAgyRecorder(t, startPath, obsPath)

	want := &runtime.LaunchProvenance{RuntimeID: "rt_auto", ExecutionID: "rt_auto", Kind: "run", Attempt: 2}
	p, err := factory.NewProvider(runtime.StartOptions{Dir: t.TempDir()}, "agy")
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	defer closeProvider(p)
	session, err := cli.StartSession(context.Background(), p, "agy", runtime.StartOptions{
		Dir:        t.TempDir(),
		Provenance: want,
	}, "conv_auto")
	if err != nil {
		t.Fatalf("StartSession resume: %v", err)
	}
	if err := p.Prompt(context.Background(), session.SessionID, "hi"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	groups := identitytest.ReadGroups(t, obsPath, 1)
	for _, observed := range groups {
		identitytest.Assert(t, observed, want)
	}
}

func intPtr(v int) *int { return &v }
