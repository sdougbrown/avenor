package poo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/askclaim"
	avenorfeed "github.com/sdougbrown/avenor/internal/feed"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	poofeed "github.com/sdougbrown/poo/feed"
	"github.com/sdougbrown/poo/tool/cliregistry"
)

func TestMessagingManifests(t *testing.T) {
	root := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	content, err := renderManifests(root, "http://example.com:80", "run")
	if err != nil {
		t.Fatal(err)
	}
	paths, err := publishManifests(root, content)
	if err != nil {
		t.Fatal(err)
	}
	asks, err := askclaim.DefaultDir(root, "http://example.com:80", "run")
	if err != nil {
		t.Fatal(err)
	}
	feeds, err := poofeed.Load(paths.feeds)
	if err != nil {
		t.Fatal(err)
	}
	specs := feeds.Specs()
	if len(specs) != 1 {
		t.Fatalf("feeds: %+v", specs)
	}
	want := []string{exe, "feed", "broker", "--lock-dir=" + filepath.Join(root, "feed-locks"), "--asks-dir=" + asks}
	if !reflect.DeepEqual(specs[0].Command, want) || specs[0].Name != "avenor-inbox" || !specs[0].Restart || specs[0].OnIdle != "wake" {
		t.Fatalf("feed: %+v", specs[0])
	}
	catalog, err := cliregistry.Load(paths.registry)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Len() != 1 {
		t.Fatalf("catalog: %v", catalog.Names())
	}
	for _, tc := range []struct {
		args  map[string]any
		flags []string
	}{
		{map[string]any{"action": "tell", "target": "-peer", "message": "-hello\nworld"}, []string{"--action=tell", "--message=-hello\nworld", "--target=-peer"}},
		{map[string]any{"action": "ask", "timeout_ms": 600000}, []string{"--action=ask", "--timeout-ms=600000"}},
		{map[string]any{"action": "reply", "reply_to": "id", "message": "..."}, []string{"--action=reply", "--message=...", "--reply-to=id"}},
		{map[string]any{"action": "withdraw", "message_id": "id"}, []string{"--action=withdraw", "--message-id=id"}},
	} {
		plan, err := catalog.Plan("avenor_message", tc.args)
		if err != nil {
			t.Fatal(err)
		}
		want := append([]string{exe, "message", "--asks-dir=" + asks}, tc.flags...)
		if !reflect.DeepEqual(plan.Argv, want) || plan.Timeout != messaging.MaxWait+30*time.Second {
			t.Errorf("plan: %+v want %q", plan, want)
		}
		for _, binding := range plan.Env {
			if !binding.Reference || binding.Name != binding.From {
				t.Fatalf("registry environment: %+v", plan.Env)
			}
		}
		if !reflect.DeepEqual(plan.Env, specs[0].Env) || len(plan.Env) != 3 || plan.Env[0].Name != "AVENOR_BROKER_TOKEN" || plan.Env[1].Name != "AVENOR_BROKER_URL" || plan.Env[2].Name != "AVENOR_RUN_ID" {
			t.Fatalf("environment: %+v / %+v", plan.Env, specs[0].Env)
		}
	}
	for _, args := range []map[string]any{{}, {"action": "bad"}, {"action": "ask", "timeout_ms": -1}, {"action": "ask", "timeout_ms": 600001}, {"action": "ask", "timeout_ms": 1.5}, {"action": "tell", "extra": "x"}} {
		if _, err := catalog.Plan("avenor_message", args); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
}

func TestPublishManifestsConcurrent(t *testing.T) {
	root := t.TempDir()
	content, err := renderManifests(root, "http://example.com", "run")
	if err != nil {
		t.Fatal(err)
	}
	const n = 16
	paths := make(chan manifestPaths, n)
	errors := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p, e := publishManifests(root, content); paths <- p; errors <- e }()
	}
	wg.Wait()
	close(paths)
	close(errors)
	for e := range errors {
		if e != nil {
			t.Fatal(e)
		}
	}
	var first manifestPaths
	for p := range paths {
		if first.feeds == "" {
			first = p
		}
		if p != first {
			t.Fatalf("different paths: %v %v", p, first)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "poo", "manifests"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries %v: %v", entries, err)
	}
	other, err := renderManifests(root, "http://example.com", "another")
	if err != nil {
		t.Fatal(err)
	}
	p, err := publishManifests(root, other)
	if err != nil || p == first {
		t.Fatalf("digest paths: %v %v", p, err)
	}
}

type lineWriter chan []byte

func (w lineWriter) Write(p []byte) (int, error) { w <- append([]byte(nil), p...); return len(p), nil }

func TestFeedInstructionMatchesManifest(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	token, _, err := b.EnsureRun("receiver", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.EnsureRun("sender", ""); err != nil {
		t.Fatal(err)
	}
	if err = b.SendTo("sender", "receiver", "agent_message", json.RawMessage(`{"id":"ask-id","message":"question","expects_reply":true}`), "ask-id"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	content, err := renderManifests(root, b.URL(), "receiver")
	if err != nil {
		t.Fatal(err)
	}
	paths, err := publishManifests(root, content)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := cliregistry.Load(paths.registry)
	if err != nil {
		t.Fatal(err)
	}
	output := make(lineWriter, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- (avenorfeed.Broker{RunID: "receiver", Token: token, BrokerURL: b.URL(), LockDir: filepath.Join(root, "locks"), AsksDir: filepath.Join(root, "asks"), Out: output, Log: os.Stderr, RequestTimeout: 100 * time.Millisecond}).Run(ctx)
	}()
	defer func() { cancel(); <-done }()
	var line []byte
	select {
	case line = <-output:
	case <-time.After(5 * time.Second):
		t.Fatal("feed did not deliver")
	}
	var msg struct {
		Text string `json:"text"`
	}
	if err = json.Unmarshal(line, &msg); err != nil {
		t.Fatal(err)
	}
	start := strings.Index(msg.Text, `{"name":`)
	end := strings.LastIndex(msg.Text, "}.")
	if start < 0 || end < start {
		t.Fatalf("instruction %q", msg.Text)
	}
	var call struct {
		Name string         `json:"name"`
		Args map[string]any `json:"args"`
	}
	if err = json.Unmarshal([]byte(msg.Text[start:end+1]), &call); err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.Plan(call.Name, call.Args)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(plan.Argv, "\n"); !strings.Contains(got, fmt.Sprintf("--reply-to=%s", "ask-id")) || !strings.Contains(got, "--message=...") || !strings.Contains(got, "--action=reply") {
		t.Fatal(plan.Argv)
	}
}
