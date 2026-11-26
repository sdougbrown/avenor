package dshacp

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
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
// NewWithOptions: a resume after construction launches the backend behind the
// retained launch wrapper, not bare. The recorder script is not an ACP server,
// so the handshake fails — the argv capture is the assertion; the error is
// expected.
func TestProviderResumeLaunchesWithWrapperFromOpts(t *testing.T) {
	argvPath := filepath.Join(t.TempDir(), "argv.txt")
	wrapper := writeArgvRecorder(t, argvPath)

	p := NewWithOptions(runtime.StartOptions{LaunchWrapper: []string{wrapper, "--flag"}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = p.Resume(ctx, "ses_resume_wrapper")

	// The full sequence proves the ordering: the wrapper runs first, and the
	// backend argv follows behind it.
	waitForArgv(t, argvPath, []string{wrapper, "--flag", "dsh", "--profile", "acp"})
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
	if caps.Backend != "dsh" {
		t.Fatalf("Capabilities.Backend = %q, want dsh", caps.Backend)
	}
	if !caps.ModelSelection {
		t.Fatal("Capabilities.ModelSelection = false, want true")
	}
}

func TestDshModelValue(t *testing.T) {
	tests := []struct {
		name  string
		model string
		want  string
	}{
		{name: "provider and model are encoded", model: "deepseek-official/deepseek-v4-flash", want: `["deepseek-official","deepseek-v4-flash"]`},
		{name: "surrounding space is ignored", model: " deepseek-official/deepseek-v4-flash ", want: `["deepseek-official","deepseek-v4-flash"]`},
		{name: "an encoded value passes through", model: `["deepseek-official","deepseek-v4-flash"]`, want: `["deepseek-official","deepseek-v4-flash"]`},
		{name: "a bare model is forwarded", model: "deepseek-v4-flash", want: "deepseek-v4-flash"},
		{name: "a trailing slash is not a pair", model: "deepseek-official/", want: "deepseek-official/"},
		{name: "a leading slash is not a pair", model: "/deepseek-v4-flash", want: "/deepseek-v4-flash"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := dshModelValue(test.model); got != test.want {
				t.Fatalf("dshModelValue(%q) = %q, want %q", test.model, got, test.want)
			}
		})
	}
}
