package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestFeedMissingCredentialsIsUsageError checks the command's environment defaults.
func TestFeedMissingCredentialsIsUsageError(t *testing.T) {
	for _, name := range []string{"AVENOR_BROKER_URL", "AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN"} {
		t.Setenv(name, "")
	}
	var out, err bytes.Buffer
	if code := runDeclared([]string{"feed", "broker"}, &out, &err); code != 2 || out.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, out.String(), err.String())
	}
}

// TestFeedClosedPipeProcess verifies that SIGPIPE becomes a clean write failure.
func TestFeedClosedPipeProcess(t *testing.T) {
	if os.Getenv("AVENOR_FEED_PIPE_CHILD") == "1" {
		os.Exit(runDeclared([]string{"feed", "broker", "--lock-dir", os.Getenv("AVENOR_FEED_LOCK_DIR")}, os.Stdout, os.Stderr))
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		fmt.Fprint(w, `[{"id":"one","payload":{"message":"hello"}}]`)
	}))
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFeedClosedPipeProcess$")
	cmd.Env = append(os.Environ(), "AVENOR_FEED_PIPE_CHILD=1", "AVENOR_BROKER_URL="+s.URL, "AVENOR_RUN_ID=r", "AVENOR_BROKER_TOKEN=t", "AVENOR_FEED_LOCK_DIR="+t.TempDir())
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = pipe.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("process: %v stderr=%q", err, stderr.String())
	}
}
