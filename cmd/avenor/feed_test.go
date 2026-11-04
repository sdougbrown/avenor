package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/askclaim"
)

// TestFeedAskDirectory checks both the default and explicit claim paths.
func TestFeedAskDirectory(t *testing.T) {
	if os.Getenv("AVENOR_FEED_ASK_CHILD") == "1" {
		args := []string{"feed", "broker", "--lock-dir", os.Getenv("AVENOR_FEED_LOCK_DIR"), "--request-timeout", "500ms"}
		if dir := os.Getenv("AVENOR_FEED_ASK_DIR"); dir != "" {
			args = append(args, "--asks-dir", dir)
		}
		os.Exit(runDeclared(args, os.Stdout, os.Stderr))
	}
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprint(custom), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sessions" {
					fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
					return
				}
				fmt.Fprint(w, `[{"id":"ask","from_run_id":"sender","payload":{"id":"ask","message":"question","expects_reply":true}}]`)
			}))
			defer s.Close()
			root := t.TempDir()
			asksDir := askclaim.DefaultDir(filepath.Join(root, "avenor"), s.URL, "r")
			customDir := ""
			if custom {
				customDir = filepath.Join(root, "custom")
				asksDir = customDir
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFeedAskDirectory$")
			cmd.Env = append(os.Environ(), "AVENOR_FEED_ASK_CHILD=1", "AVENOR_BROKER_URL="+s.URL, "AVENOR_RUN_ID=r", "AVENOR_BROKER_TOKEN=t", "AVENOR_FEED_LOCK_DIR="+filepath.Join(root, "locks"), "AVENOR_FEED_ASK_DIR="+customDir, "XDG_STATE_HOME="+root)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			scanner := bufio.NewScanner(stdout)
			if !scanner.Scan() {
				_ = cmd.Wait()
				t.Fatalf("no feed output: %q", stderr.String())
			}
			raw, err := os.ReadFile(filepath.Join(asksDir, "ask.json"))
			if err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatal(err)
			}
			var record struct {
				Asker string `json:"asker_run_id"`
			}
			if err := json.Unmarshal(raw, &record); err != nil || record.Asker != "sender" {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("record %q: %v", raw, err)
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				_ = cmd.Wait()
				t.Fatal(err)
			}
			// Drain stdout while the in-flight poll finishes.
			for scanner.Scan() {
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("exit: %v; stderr %q", err, stderr.String())
			}
		})
	}
}

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
