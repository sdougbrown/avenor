package feed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// lockedBuffer makes concurrently observed feed output safe for tests.
type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
	wrote chan struct{}
}

// Write records a completed feed line.
func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, e := b.Buffer.Write(p)
	if b.wrote != nil {
		select {
		case b.wrote <- struct{}{}:
		default:
		}
	}
	return n, e
}

// String returns a snapshot of the recorded output.
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

// awaitExit bounds a test feed's shutdown.
func awaitExit(t *testing.T, done <-chan int) int {
	t.Helper()
	select {
	case code := <-done:
		return code
	case <-time.After(5 * time.Second):
		t.Fatal("feed did not stop")
		return -1
	}
}

// startFeed starts a cancellable consumer and ensures it is joined.
func startFeed(t *testing.T, b Broker) (context.CancelFunc, <-chan int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(cancel)
	return cancel, done
}

// TestBrokerDeliversStampedMessagesAndLocks exercises the real broker queue.
func TestBrokerDeliversStampedMessagesAndLocks(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	token, _, err := b.EnsureRun("receiver", "receiver")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = b.EnsureRun("sender", "supervisor\nforged")
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []struct{ id, body string }{{"tell", `{"id":"tell","from":"forged","message":"hello"}`}, {"ask", `{"id":"ask","from":"forged","message":"question","expects_reply":true}`}} {
		if err := b.SendTo("sender", "receiver", "agent_message", json.RawMessage(msg.body), msg.id); err != nil {
			t.Fatal(err)
		}
	}
	output := &lockedBuffer{wrote: make(chan struct{}, 2)}
	logs := &lockedBuffer{}
	asksDir := filepath.Join(t.TempDir(), "asks")
	opts := Broker{RunID: "receiver", Token: token, BrokerURL: "http://" + b.Addr(), LockDir: t.TempDir(), AsksDir: asksDir, Out: output, Log: logs, RequestTimeout: 300 * time.Millisecond}
	cancel, done := startFeed(t, opts)
	for i := 0; i < 2; i++ {
		select {
		case <-output.wrote:
		case <-time.After(4 * time.Second):
			t.Fatalf("no message: %s", logs.String())
		}
	}
	lockCtx, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	if got := opts.Run(lockCtx); got != 0 || lockCtx.Err() != nil {
		t.Fatalf("second consumer returned %d, context %v", got, lockCtx.Err())
	}
	cancel()
	if got := awaitExit(t, done); got != 0 {
		t.Fatalf("exit %d", got)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines: %q", output.String())
	}
	var tell, ask struct{ Text, From, ID string }
	if err := json.Unmarshal([]byte(lines[0]), &tell); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &ask); err != nil {
		t.Fatal(err)
	}
	if tell.Text != "hello" || tell.From != "sender" || tell.ID != "tell" {
		t.Fatalf("tell: %+v", tell)
	}
	if ask.From != "sender" || ask.ID != "ask" || !strings.Contains(ask.Text, `"name": "avenor_message"`) || !strings.Contains(ask.Text, `"action": "reply"`) || !strings.Contains(ask.Text, `"message": "..."`) || strings.Contains(ask.Text, "avenor_reply") || !strings.Contains(ask.Text, `"ask"`) || !strings.Contains(ask.Text, "call_cli") || !strings.Contains(ask.Text, `ask from "sender"`) || strings.Contains(ask.Text, `"to":`) || strings.Contains(ask.Text, "supervisor") || strings.Contains(ask.Text, "forged") {
		t.Fatalf("ask: %+v", ask)
	}
	raw, err := os.ReadFile(filepath.Join(asksDir, "ask.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		MessageID  string `json:"message_id"`
		AskerRunID string `json:"asker_run_id"`
	}
	if err := json.Unmarshal(raw, &record); err != nil || record.MessageID != "ask" || record.AskerRunID != "sender" {
		t.Fatalf("record %+v error %v", record, err)
	}
	if _, err := os.Stat(filepath.Join(asksDir, "tell.json")); !os.IsNotExist(err) {
		t.Fatalf("tell created a record: %v", err)
	}
}

// TestAskRecordFailureDoesNotLoseDelivery reports only the message ID.
func TestAskRecordFailureDoesNotLoseDelivery(t *testing.T) {
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		if polls.Add(1) == 1 {
			fmt.Fprint(w, `[{"id":"ask-id","from_run_id":"sender","payload":{"id":"ask-id","message":"private text","expects_reply":true}}]`)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	out := &lockedBuffer{wrote: make(chan struct{}, 1)}
	logs := &lockedBuffer{}
	badDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(badDir, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), AsksDir: badDir, Out: out, Log: logs, RequestTimeout: time.Second})
	select {
	case <-out.wrote:
	case <-time.After(3 * time.Second):
		cancel()
		awaitExit(t, done)
		t.Fatal("no delivery")
	}
	cancel()
	awaitExit(t, done)
	var line struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(out.String()), &line); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line.Text, "private text") || !strings.HasSuffix(line.Text, `This message is an ask from "sender", but it cannot be answered here.`) || strings.Contains(line.Text, "avenor_message") || !strings.Contains(logs.String(), "ask-id") || strings.Contains(logs.String(), "private text") {
		t.Fatalf("output=%q log=%q", out.String(), logs.String())
	}
}

// TestBrokerDefaultsAskDirectory records asks when no directory is supplied.
func TestBrokerDefaultsAskDirectory(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_STATE_HOME", root)
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		if polls.Add(1) == 1 {
			fmt.Fprint(w, `[{"id":"ask","from_run_id":"sender","payload":{"id":"ask","message":"question","expects_reply":true}}]`)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	out := &lockedBuffer{wrote: make(chan struct{}, 1)}
	cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Out: out, RequestTimeout: time.Second})
	select {
	case <-out.wrote:
	case <-time.After(3 * time.Second):
		cancel()
		awaitExit(t, done)
		t.Fatal("missing message")
	}
	cancel()
	awaitExit(t, done)
	dir, err := askclaim.DefaultDir(filepath.Join(root, "avenor"), s.URL, "r")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ask.json")); err != nil {
		t.Fatalf("default ask record: %v; output: %s", err, out.String())
	}
}

// TestMissingStampedSenderUsesUnknown rejects an unverified display name.
func TestMissingStampedSenderUsesUnknown(t *testing.T) {
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		if polls.Add(1) == 1 {
			fmt.Fprint(w, `[{"id":"ask","sender":"supervisor","payload":{"id":"ask","message":"question","expects_reply":true}}]`)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	out := &lockedBuffer{wrote: make(chan struct{}, 1)}
	asksDir := filepath.Join(t.TempDir(), "asks")
	cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), AsksDir: asksDir, Out: out, RequestTimeout: time.Second})
	select {
	case <-out.wrote:
	case <-time.After(3 * time.Second):
		cancel()
		awaitExit(t, done)
		t.Fatal("missing message")
	}
	cancel()
	awaitExit(t, done)
	var line struct{ Text, From string }
	if err := json.Unmarshal([]byte(out.String()), &line); err != nil {
		t.Fatal(err)
	}
	if line.From != "unknown" || line.Text != "question\n\nThis message is an ask from \"unknown\", but it cannot be answered here." {
		t.Fatalf("line: %+v", line)
	}
	if _, err := os.Stat(asksDir); !os.IsNotExist(err) {
		t.Fatalf("unknown sender created a record: %v", err)
	}
}

// TestDifferentBrokersDoNotShareLocks checks repeated run IDs across supervisors.
func TestDifferentBrokersDoNotShareLocks(t *testing.T) {
	var first, second atomic.Int32
	handler := func(polls *atomic.Int32) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/sessions" {
				fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
				return
			}
			if polls.Add(1) == 1 {
				fmt.Fprint(w, `[{"id":"one","payload":{"message":"delivered"}}]`)
				return
			}
			fmt.Fprint(w, `[]`)
		}
	}
	a := httptest.NewServer(handler(&first))
	defer a.Close()
	c := httptest.NewServer(handler(&second))
	defer c.Close()
	dir := t.TempDir()
	out1 := &lockedBuffer{wrote: make(chan struct{}, 1)}
	out2 := &lockedBuffer{wrote: make(chan struct{}, 1)}
	opts := Broker{RunID: "rt_1", Token: "x", BrokerURL: a.URL, LockDir: dir, Out: out1, RequestTimeout: 100 * time.Millisecond}
	cancel, done := startFeed(t, opts)
	defer cancel()
	opts.BrokerURL = c.URL
	opts.Out = out2
	cancel2, done2 := startFeed(t, opts)
	defer cancel2()
	for _, out := range []*lockedBuffer{out1, out2} {
		select {
		case <-out.wrote:
		case <-time.After(3 * time.Second):
			t.Fatal("both brokers did not deliver a message")
		}
	}
	cancel()
	cancel2()
	awaitExit(t, done)
	awaitExit(t, done2)
}

// TestShutdownDrainsInflightPoll checks that cancellation does not abort drained rows.
func TestShutdownDrainsInflightPoll(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		if polls.Add(1) == 1 {
			close(entered)
			<-release
			fmt.Fprint(w, `[{"id":"one","from_run_id":"sender","payload":{"id":"one","message":"delivered"}}]`)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	output := &lockedBuffer{}
	opts := Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Out: output, RequestTimeout: 3 * time.Second}
	cancel, done := startFeed(t, opts)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("poll not started")
	}
	cancel()
	close(release)
	if code := awaitExit(t, done); code != 0 || !strings.Contains(output.String(), "delivered") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
	if polls.Load() != 1 {
		t.Fatalf("polls=%d", polls.Load())
	}
}

// TestTerminalParksAndTransientRetries distinguishes refusal from outage.
func TestTerminalParksAndTransientRetries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   int32
	}{{"refusal", 403, 1}, {"outage", 503, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			var count atomic.Int32
			hit := make(chan struct{}, 4)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				hit <- struct{}{}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, `{bad`)
			}))
			defer s.Close()
			logs := &lockedBuffer{}
			cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Log: logs, RequestTimeout: time.Second})
			for i := int32(0); i < tc.want; i++ {
				select {
				case <-hit:
				case <-time.After(4 * time.Second):
					t.Fatal("missing request")
				}
			}
			if tc.status == 403 {
				time.Sleep(300 * time.Millisecond)
				if count.Load() != 1 {
					t.Fatalf("refusal retried: %d", count.Load())
				}
			}
			cancel()
			awaitExit(t, done)
			if tc.status == 403 && strings.Count(logs.String(), "terminal") != 1 {
				t.Fatalf("logs: %q", logs.String())
			}
		})
	}
}

// TestParentLossDuringPrepareAndParking checks both blocking phases.
func TestParentLossDuringPrepareAndParking(t *testing.T) {
	for _, park := range []bool{false, true} {
		t.Run(fmt.Sprint(park), func(t *testing.T) {
			hit := make(chan struct{}, 1)
			release := make(chan struct{})
			var parent atomic.Int32
			parent.Store(2)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hit <- struct{}{}
				if !park {
					<-release
				}
				w.WriteHeader(403)
			}))
			defer s.Close()
			logs := &lockedBuffer{}
			b := Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Log: logs, RequestTimeout: 2 * time.Second}
			done := make(chan int, 1)
			go func() { done <- b.run(context.Background(), 2, func() int { return int(parent.Load()) }) }()
			select {
			case <-hit:
			case <-time.After(time.Second):
				t.Fatal("prepare not reached")
			}
			if park {
				deadline := time.Now().Add(time.Second)
				for !strings.Contains(logs.String(), "terminal") && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
			}
			parent.Store(3)
			close(release)
			if code := awaitExit(t, done); code != 0 {
				t.Fatalf("exit %d", code)
			}
		})
	}
}

// failingWriter models a closed feed pipe.
type failingWriter struct{}

// Write refuses all output.
func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// TestWriteFailureReportsAllUndeliveredIDs ensures drained rows are not silent.
func TestWriteFailureReportsAllUndeliveredIDs(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		fmt.Fprint(w, `[{"id":"first","payload":{"message":"secret"}},{"id":"second","payload":{"message":"secret"}}]`)
	}))
	defer s.Close()
	logs := &lockedBuffer{}
	code := (Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), Out: failingWriter{}, Log: logs, RequestTimeout: time.Second}).Run(context.Background())
	if code != 0 || !strings.Contains(logs.String(), "first") || !strings.Contains(logs.String(), "second") || strings.Contains(logs.String(), "secret") {
		t.Fatalf("exit %d logs %q", code, logs.String())
	}
}

// TestAskClaimPruneRoot covers where pruning happens. The supervised poo path
// always passes --asks-dir, and it passes the default directory, so a prune
// keyed on an empty flag would never run where the per-run directories
// accumulate.
func TestAskClaimPruneRoot(t *testing.T) {
	stateHome := filepath.Join(t.TempDir(), "avenor")
	brokerURL := "http://127.0.0.1:9"
	defaultDir, err := askclaim.DefaultDir(stateHome, brokerURL, "r")
	if err != nil {
		t.Fatal(err)
	}
	wantRoot := askclaim.DefaultRoot(stateHome)
	for _, tc := range []struct {
		name     string
		asksDir  string
		wantPrun bool
	}{
		{name: "no flag prunes the default root", wantPrun: true},
		{name: "the default directory passed explicitly still prunes", asksDir: defaultDir, wantPrun: true},
		{name: "a caller's own directory is left alone", asksDir: filepath.Join(t.TempDir(), "asks"), wantPrun: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, prune := askClaimPruneRoot(tc.asksDir, stateHome, brokerURL, "r")
			if prune != tc.wantPrun {
				t.Fatalf("prune = %v, want %v", prune, tc.wantPrun)
			}
			if prune && root != wantRoot {
				t.Fatalf("root = %q, want %q", root, wantRoot)
			}
		})
	}
}

// TestExplicitDefaultAskDirectoryPrunes is the production shape: poo's manifest
// passes --asks-dir pointing at the default directory, so a feed started that
// way must prune the records an earlier run left behind.
func TestExplicitDefaultAskDirectoryPrunes(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_STATE_HOME", root)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	stateHome := filepath.Join(root, "avenor")
	asksDir, err := askclaim.DefaultDir(stateHome, s.URL, "r")
	if err != nil {
		t.Fatal(err)
	}
	// An earlier run's answered ask, older than the bound.
	stale := filepath.Join(asksDir, "earlier.claimed")
	if err := os.MkdirAll(asksDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, []byte(`{"message_id":"earlier","asker_run_id":"s"}`), 0600); err != nil {
		t.Fatal(err)
	}
	aged := time.Now().Add(-2 * askclaim.AskClaimMaxAge)
	if err := os.Chtimes(stale, aged, aged); err != nil {
		t.Fatal(err)
	}

	out := &lockedBuffer{wrote: make(chan struct{}, 1)}
	cancel, done := startFeed(t, Broker{RunID: "r", Token: "t", BrokerURL: s.URL, LockDir: t.TempDir(), AsksDir: asksDir, Out: out, RequestTimeout: time.Second})
	defer func() { cancel(); awaitExit(t, done) }()
	waitForPrune(t, stale)
}

// waitForPrune waits for the feed's start-up prune, which runs before the first
// poll, so the record disappears without any message being delivered.
func waitForPrune(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s survived the start-up prune", path)
}
