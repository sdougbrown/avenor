package fxacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

// writeArgvRecorder creates a shell script that appends its own argv ($0 plus
// arguments) to the file at path, one entry per line.
func writeArgvRecorder(t *testing.T, path string) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "recorder.sh")
	body := "printf '%s\\n' \"$0\" \"$@\" >> " + shellQuotePath(path) + "\n"
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatalf("write recorder script: %v", err)
	}
	return script
}

func shellQuotePath(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// waitForArgv polls until the recorder file lists exactly want. The wrapper
// and backend run concurrently with this test, so the file fills asynchronously.
func waitForArgv(t *testing.T, argvPath string, want []string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, err := os.ReadFile(argvPath)
		if err == nil {
			got := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
			if slices.Equal(got, want) {
				return
			}
		}
		if time.Now().After(deadline) {
			raw, _ := os.ReadFile(argvPath)
			t.Fatalf("argv file = %q, want %v", string(raw), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestProviderResumeLaunchesWithWrapperFromOpts pins the Opts handoff in
// NewWithOptions: a resume after construction launches the backend behind
// the retained launch wrapper, not bare. The recorder script is not an ACP
// server, so the handshake fails — the argv capture is the assertion; the
// error is expected.
func TestProviderResumeLaunchesWithWrapperFromOpts(t *testing.T) {
	argvPath := filepath.Join(t.TempDir(), "argv.txt")
	wrapper := writeArgvRecorder(t, argvPath)

	p := NewWithOptions(runtime.StartOptions{LaunchWrapper: []string{wrapper, "--flag"}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = p.Resume(ctx, "ses_resume_wrapper")

	// The full sequence proves the ordering: the wrapper runs first, and the
	// backend argv follows behind it.
	waitForArgv(t, argvPath, []string{wrapper, "--flag", "fx", "acp"})
}

// TestLaunchArgvCarriesModel pins the spawn-time model path: a named model
// rides the launch argv as --model <model>, and an unnamed launch omits it.
func TestLaunchArgvCarriesModel(t *testing.T) {
	argvPath := filepath.Join(t.TempDir(), "argv.txt")
	wrapper := writeArgvRecorder(t, argvPath)

	p := NewWithOptions(runtime.StartOptions{Model: "qwen", LaunchWrapper: []string{wrapper, "--flag"}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = p.Start(ctx, runtime.StartOptions{})

	waitForArgv(t, argvPath, []string{wrapper, "--flag", "fx", "acp", "--model", "qwen"})
}

// TestCapabilitiesPinsModelSelection pins that fx declares model selection:
// its model rides the launch, so a run's --model reaches the backend.
func TestCapabilitiesPinsModelSelection(t *testing.T) {
	p := New()
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if !caps.ModelSelection {
		t.Fatal("Capabilities.ModelSelection = false, want true for fx")
	}
}

// TestCapabilitiesPinsBackendID keeps the backend id string pinned: the id is
// the wire identity for backend selection, and a silent rename would break
// operators' configured backend values without any test failure.
func TestCapabilitiesPinsBackendID(t *testing.T) {
	p := New()
	caps, err := p.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.Backend != "fx" {
		t.Fatalf("Capabilities.Backend = %q, want fx", caps.Backend)
	}
}

// TestPromptFailedPolicyRecognizesOnlyStreamedInconsistentFinish pins the fx
// carve-out's decision rule: only fx's InconsistentFinishReason error on a
// turn that streamed reply content qualifies. The streamed-content true path
// is exercised end to end by the acp package's prompt-failure policy test; a
// zero-value Session cannot have streamed content, so it never qualifies.
func TestPromptFailedPolicyRecognizesOnlyStreamedInconsistentFinish(t *testing.T) {
	policy := PromptFailedPolicy()
	session := &acp.Session{}

	if policy(session, errors.New("json-rpc session/prompt failed: InconsistentFinishReason (-32603)")) {
		t.Fatal("policy tolerated an InconsistentFinishReason turn that streamed no content")
	}
	if policy(session, errors.New("json-rpc session/prompt failed: ProviderUnreachable (-32000)")) {
		t.Fatal("policy tolerated an unrelated prompt error")
	}
	if policy(session, nil) {
		t.Fatal("policy tolerated a nil error")
	}
}
