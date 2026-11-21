package miniswe

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestRunReturnsSubmissionAndRejectsFollowup(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "mini")
	script := `#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output" ]; then shift; output=$1; fi
  shift
done
printf '%s' '{"info":{"exit_status":"Submitted","submission":"OK"}}' > "$output"
`
	if err := os.WriteFile(bin, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	p := NewWithOptions(runtime.StartOptions{})
	s, err := p.Start(context.Background(), runtime.StartOptions{Dir: dir, Model: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := p.Events(ctx, s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Prompt(ctx, s.SessionID, "hello"); err != nil {
		t.Fatal(err)
	}
	var got bool
	for !got {
		select {
		case ev := <-ch:
			if ev.Event == "session.end" {
				if ev.Fields["stop_reason"] != "end_turn" || ev.Fields["final_output"] != "OK" {
					t.Fatalf("bad event: %#v", ev)
				}
				got = true
			}
		case <-time.After(time.Second):
			t.Fatal("no terminal event")
		}
	}
	if err := p.Prompt(ctx, s.SessionID, "again"); err == nil {
		t.Fatal("follow-up silently discarded context")
	}
}

func TestUnsuccessfulTrajectoryIsFailure(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "mini")
	script := `#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output" ]; then shift; output=$1; fi
  shift
done
printf '%s' '{"info":{"exit_status":"LimitsExceeded","submission":""}}' > "$output"
`
	if err := os.WriteFile(bin, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	p := NewWithOptions(runtime.StartOptions{})
	s, err := p.Start(context.Background(), runtime.StartOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Prompt(context.Background(), s.SessionID, "hello"); err == nil {
		t.Fatal("limit reported as success")
	}
}
