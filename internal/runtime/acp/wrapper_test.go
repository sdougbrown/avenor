package acp

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

// TestNewClientWithoutWrapperPreservesArgv proves the no-wrapper launch is
// unchanged: the backend sees exactly the argv the client config describes.
func TestNewClientWithoutWrapperPreservesArgv(t *testing.T) {
	argvPath := filepath.Join(t.TempDir(), "argv.txt")
	bin := writeArgvRecorder(t, argvPath)
	args := []string{"acp", "--pure"}

	client, err := NewClient(context.Background(), ClientConfig{Bin: bin, Args: args})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	waitForArgv(t, argvPath, append([]string{bin}, args...))
}

// TestNewClientWrapperPrefixesBackendArgv pins the launched process's argv at
// the shared ACP launch site: the launched argv is the wrapper prefix
// followed by the backend command. The recorder wrapper stub exits without
// exec'ing the backend, so this test does not prove backend execution.
func TestNewClientWrapperPrefixesBackendArgv(t *testing.T) {
	argvPath := filepath.Join(t.TempDir(), "argv.txt")
	bin := writeArgvRecorder(t, argvPath)
	wrapper := writeArgvRecorder(t, argvPath)
	args := []string{"acp", "--pure"}

	client, err := NewClient(context.Background(), ClientConfig{
		Bin:     bin,
		Args:    args,
		Wrapper: []string{wrapper, "--flag"},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	waitForArgv(t, argvPath, []string{wrapper, "--flag", bin, "acp", "--pure"})
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

// TestProviderResumeLaunchesWithWrapperFromOpts proves the resume path uses
// the authoritative launch options retained on the provider: a resume after
// construction launches the backend behind the wrapper, not bare. The
// recorder script is not an ACP server, so the handshake fails — the argv
// capture is the assertion; the error is expected.
func TestProviderResumeLaunchesWithWrapperFromOpts(t *testing.T) {
	argvPath := filepath.Join(t.TempDir(), "argv.txt")
	bin := writeArgvRecorder(t, argvPath)
	wrapper := writeArgvRecorder(t, argvPath)
	args := []string{"acp", "--pure"}

	p := NewProvider(ProviderConfig{
		BackendID: "opencode-acp",
		Bin:       bin,
		Args:      args,
		Opts:      runtime.StartOptions{LaunchWrapper: []string{wrapper, "--flag"}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = p.Resume(ctx, "ses_resume_wrapper")

	// The full sequence proves the ordering: the wrapper runs first, and the
	// backend argv follows behind it.
	waitForArgv(t, argvPath, []string{wrapper, "--flag", bin, "acp", "--pure"})
}
