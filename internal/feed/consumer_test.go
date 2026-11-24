package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// awaitConsume bounds a test consumer's shutdown.
func awaitConsume(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
		return nil
	}
}

// TestAcquireLockReportsContention covers the advisory lock one inbox needs.
func TestAcquireLockReportsContention(t *testing.T) {
	lockDir := t.TempDir()
	brokerURL, runID := "http://127.0.0.1:9", "r"
	unlock, held, err := AcquireLock(lockDir, brokerURL, runID)
	if err != nil || held {
		t.Fatalf("first lock: held=%v err=%v", held, err)
	}
	if _, held, err = AcquireLock(lockDir, brokerURL, runID); err != nil || !held {
		t.Fatalf("second lock: held=%v err=%v", held, err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	unlock, held, err = AcquireLock(lockDir, brokerURL, runID)
	if err != nil || held {
		t.Fatalf("lock after release: held=%v err=%v", held, err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
}

// TestAcquireLockRejectsInvalidBrokerURL checks the sentinel a caller keys on.
func TestAcquireLockRejectsInvalidBrokerURL(t *testing.T) {
	if _, _, err := AcquireLock(t.TempDir(), "not a broker url", "r"); !errors.Is(err, ErrInvalidBrokerURL) {
		t.Fatalf("err = %v", err)
	}
}

// TestConsumeCancelInFlightAbortsBlockedPoll distinguishes a surface that wants
// its own cancellation to reach the wire (MCP) from one that drains (CLI).
func TestConsumeCancelInFlightAbortsBlockedPoll(t *testing.T) {
	entered := make(chan struct{}, 1)
	aborted := make(chan struct{}, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		// The server watches for a client disconnect only once the request body
		// has been consumed, so an undrained poll would hide the abort.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		select {
		case aborted <- struct{}{}:
		default:
		}
	}))
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var delivered atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- Consume(ctx, ConsumerConfig{
			Surface:        messaging.SurfaceFeed,
			RunID:          "r",
			Token:          "t",
			BrokerURL:      s.URL,
			AsksDir:        t.TempDir(),
			LogPrefix:      "test consumer",
			RequestTimeout: 10 * time.Second,
			CancelInFlight: true,
			OnRow:          func(Message) error { delivered.Add(1); return nil },
		})
	}()
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		cancel()
		awaitConsume(t, done)
		t.Fatal("poll not started")
	}
	cancel()
	if err := awaitConsume(t, done); err != nil {
		t.Fatalf("consume returned %v", err)
	}
	select {
	case <-aborted:
	case <-time.After(2 * time.Second):
		t.Fatal("the blocked poll was not cancelled with the consumer")
	}
	if got := delivered.Load(); got != 0 {
		t.Fatalf("delivered %d rows from an abandoned poll", got)
	}
}

// TestConsumeBeforePollPausesPolling checks that a surface can hold the inbox
// closed without stopping the consumer.
func TestConsumeBeforePollPausesPolling(t *testing.T) {
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		switch polls.Add(1) {
		case 1:
			fmt.Fprint(w, `[{"id":"first","payload":{"message":"first"}}]`)
		case 2:
			fmt.Fprint(w, `[{"id":"second","payload":{"message":"second"}}]`)
		default:
			fmt.Fprint(w, `[]`)
		}
	}))
	defer s.Close()
	reached := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	delivered := make(chan string, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Consume(ctx, ConsumerConfig{
			Surface:   messaging.SurfaceFeed,
			RunID:     "r",
			Token:     "t",
			BrokerURL: s.URL,
			AsksDir:   t.TempDir(),
			LogPrefix: "test consumer",
			BeforePoll: func(context.Context) error {
				if calls.Add(1) == 2 {
					close(reached)
					<-release
				}
				return nil
			},
			OnRow: func(m Message) error { delivered <- m.Text; return nil },
		})
	}()
	select {
	case text := <-delivered:
		if text != "first" {
			cancel()
			awaitConsume(t, done)
			t.Fatalf("first text %q", text)
		}
	case <-time.After(4 * time.Second):
		cancel()
		awaitConsume(t, done)
		t.Fatal("no delivery before the pause")
	}
	select {
	case <-reached:
	case <-time.After(4 * time.Second):
		cancel()
		awaitConsume(t, done)
		t.Fatal("BeforePoll was not reached")
	}
	if got := polls.Load(); got != 1 {
		cancel()
		awaitConsume(t, done)
		t.Fatalf("polled %d times while paused", got)
	}
	select {
	case text := <-delivered:
		cancel()
		awaitConsume(t, done)
		t.Fatalf("delivered %q while paused", text)
	default:
	}
	close(release)
	select {
	case text := <-delivered:
		if text != "second" {
			cancel()
			awaitConsume(t, done)
			t.Fatalf("second text %q", text)
		}
	case <-time.After(4 * time.Second):
		cancel()
		awaitConsume(t, done)
		t.Fatal("polling did not resume")
	}
	cancel()
	if err := awaitConsume(t, done); err != nil {
		t.Fatalf("consume returned %v", err)
	}
}

// TestConsumeRowFailureReportsRemainingRows pins what a failed delivery does to
// the rest of the drained batch: the remaining rows are reported by raw ID, no
// claim is recorded for them, and the batch is not announced as complete.
func TestConsumeRowFailureReportsRemainingRows(t *testing.T) {
	errBoom := errors.New("boom")
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		if polls.Add(1) == 1 {
			fmt.Fprint(w, `[
				{"id":"row1","from_run_id":"sender","payload":{"id":"row1","message":"one","expects_reply":false}},
				{"id":"row2","from_run_id":"sender","payload":{"id":"row2","message":"two","expects_reply":false}},
				{"id":"row3","from_run_id":"sender","payload":{"id":"row3","message":"three","expects_reply":true}}
			]`)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	asksDir := t.TempDir()
	logs := &lockedBuffer{}
	var delivered []Message
	var batches atomic.Int32
	err := Consume(context.Background(), ConsumerConfig{
		Surface:   messaging.SurfaceFeed,
		RunID:     "r",
		Token:     "t",
		BrokerURL: s.URL,
		AsksDir:   asksDir,
		Log:       logs,
		LogPrefix: "test consumer",
		OnRow: func(m Message) error {
			if m.ID == "row2" {
				return errBoom
			}
			delivered = append(delivered, m)
			return nil
		},
		OnBatchDone: func() { batches.Add(1) },
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("consume returned %v", err)
	}
	if len(delivered) != 1 || delivered[0].ID != "row1" || delivered[0].Text != "one" {
		t.Fatalf("delivered %+v", delivered)
	}
	if got := batches.Load(); got != 0 {
		t.Fatalf("OnBatchDone ran %d times for a failed batch", got)
	}
	if got := polls.Load(); got != 1 {
		t.Fatalf("polled %d times after the failure", got)
	}
	if _, statErr := os.Stat(filepath.Join(asksDir, "row3.json")); !os.IsNotExist(statErr) {
		t.Fatalf("an undelivered ask was claimed: %v", statErr)
	}
	logged := logs.String()
	if !strings.Contains(logged, `"row2"`) || !strings.Contains(logged, `"row3"`) {
		t.Fatalf("lost rows were not reported by ID: %q", logged)
	}
	if strings.Contains(logged, "two") || strings.Contains(logged, "three") {
		t.Fatalf("the log carried message text: %q", logged)
	}
}

// TestConsumeParksOnTerminalRefusalWithoutToken pins the no-restart-storm
// contract and the secret half of the log boundary.
func TestConsumeParksOnTerminalRefusalWithoutToken(t *testing.T) {
	const token = "s3cr3t-token-value"
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		polls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer s.Close()
	logs := &lockedBuffer{wrote: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Consume(ctx, ConsumerConfig{
			Surface:   messaging.SurfaceFeed,
			RunID:     "r",
			Token:     token,
			BrokerURL: s.URL,
			AsksDir:   t.TempDir(),
			Log:       logs,
			LogPrefix: "test consumer",
		})
	}()
	deadline := time.After(4 * time.Second)
	for !strings.Contains(logs.String(), "terminal") {
		select {
		case <-logs.wrote:
		case <-deadline:
			cancel()
			awaitConsume(t, done)
			t.Fatalf("no terminal diagnostic: %q", logs.String())
		}
	}
	select {
	case err := <-done:
		t.Fatalf("a refused consumer returned %v instead of parking", err)
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	if err := awaitConsume(t, done); err != nil {
		t.Fatalf("consume returned %v", err)
	}
	logged := logs.String()
	if !strings.Contains(logged, "test consumer: terminal broker refusal (HTTP 401)") {
		t.Fatalf("diagnostic %q", logged)
	}
	if strings.Contains(logged, token) {
		t.Fatalf("the log carried the token: %q", logged)
	}
	if got := polls.Load(); got != 1 {
		t.Fatalf("polls=%d, want exactly the one refused poll", got)
	}
}

// TestConsumeShrinksLimitAfterOversizedBatch checks the bounded recovery poll
// and the reset that follows a successful receive.
func TestConsumeShrinksLimitAfterOversizedBatch(t *testing.T) {
	limits := make(chan int, 4)
	var polls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sessions" {
			fmt.Fprint(w, `{"messaging_protocol":1,"generation":"g"}`)
			return
		}
		var body struct {
			Limit int `json:"limit"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		select {
		case limits <- body.Limit:
		default:
		}
		if polls.Add(1) == 1 {
			fmt.Fprint(w, strings.Repeat("x", (1<<20)+1))
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer s.Close()
	logs := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Consume(ctx, ConsumerConfig{
			Surface:   messaging.SurfaceFeed,
			RunID:     "r",
			Token:     "t",
			BrokerURL: s.URL,
			AsksDir:   t.TempDir(),
			Limit:     4,
			Log:       logs,
			LogPrefix: "test consumer",
		})
	}()
	for _, want := range []int{4, 1, 4} {
		select {
		case got := <-limits:
			if got != want {
				cancel()
				awaitConsume(t, done)
				t.Fatalf("limit=%d want %d", got, want)
			}
		case <-time.After(4 * time.Second):
			cancel()
			awaitConsume(t, done)
			t.Fatal("poll missing")
		}
	}
	cancel()
	if err := awaitConsume(t, done); err != nil {
		t.Fatalf("consume returned %v", err)
	}
	if !strings.Contains(logs.String(), "drained batch exceeded response cap") {
		t.Fatalf("log %q", logs.String())
	}
}

// TestConsumeRequiresLogPrefix proves the shared consumer refuses to fall back
// to one surface's name when a caller omits LogPrefix and classifies missing
// required fields as ErrInvalidProviderConfig.
func TestConsumeRequiresLogPrefix(t *testing.T) {
	cases := []struct {
		name string
		cfg  ConsumerConfig
		want string
	}{
		{
			name: "empty prefix",
			cfg:  ConsumerConfig{Surface: messaging.SurfaceFeed, RunID: "r", Token: "t", BrokerURL: "http://127.0.0.1:9", AsksDir: t.TempDir(), LogPrefix: ""},
			want: "log prefix is required",
		},
		{
			name: "whitespace prefix",
			cfg:  ConsumerConfig{Surface: messaging.SurfaceFeed, RunID: "r", Token: "t", BrokerURL: "http://127.0.0.1:9", AsksDir: t.TempDir(), LogPrefix: "   "},
			want: "log prefix is required",
		},
		{
			name: "missing run id",
			cfg:  ConsumerConfig{Surface: messaging.SurfaceFeed, Token: "t", BrokerURL: "http://127.0.0.1:9", AsksDir: t.TempDir(), LogPrefix: "test"},
			want: "run id, token, and broker URL are required",
		},
		{
			name: "missing token",
			cfg:  ConsumerConfig{Surface: messaging.SurfaceFeed, RunID: "r", BrokerURL: "http://127.0.0.1:9", AsksDir: t.TempDir(), LogPrefix: "test"},
			want: "run id, token, and broker URL are required",
		},
		{
			name: "missing broker url",
			cfg:  ConsumerConfig{Surface: messaging.SurfaceFeed, RunID: "r", Token: "t", AsksDir: t.TempDir(), LogPrefix: "test"},
			want: "run id, token, and broker URL are required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Consume(context.Background(), tc.cfg)
			if !errors.Is(err, ErrInvalidProviderConfig) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ErrInvalidProviderConfig containing %q", err, tc.want)
			}
		})
	}
}
