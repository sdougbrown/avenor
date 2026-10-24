package claudecore

import (
	"context"
	"testing"
	"time"
)

// fakeStartupClock swaps the package clock for a manual one whose sleeps
// advance it and are recorded.
func fakeStartupClock(t *testing.T) (now *time.Time, slept *[]time.Duration) {
	t.Helper()
	savedNow, savedSleep := startupNow, startupSleep
	clock := time.Unix(1000, 0)
	var sleeps []time.Duration
	startupNow = func() time.Time { return clock }
	startupSleep = func(ctx context.Context, d time.Duration) bool {
		if ctx.Err() != nil {
			return false
		}
		sleeps = append(sleeps, d)
		clock = clock.Add(d)
		return true
	}
	t.Cleanup(func() { startupNow, startupSleep = savedNow, savedSleep })
	return &clock, &sleeps
}

func newWaitSession(t *testing.T) *Session {
	t.Helper()
	s := NewSession(context.Background(), SessionOptions{SessionID: "ses-wait", EventsBuf: 4})
	t.Cleanup(s.CancelFn)
	return s
}

func TestStartupWaitUnsetDoesNotWait(t *testing.T) {
	_, slept := fakeStartupClock(t)
	s := newWaitSession(t)
	s.SetStartupWait(0)

	_, configured, ok := s.WaitForStartupMCP(s.Ctx)
	if configured || !ok || len(*slept) != 0 {
		t.Fatalf("configured=%v ok=%v sleeps=%v, want no wait", configured, ok, *slept)
	}
}

func TestStartupWaitCountsFromLaunch(t *testing.T) {
	clock, slept := fakeStartupClock(t)
	s := newWaitSession(t)
	s.SetStartupWait(4 * time.Second)
	*clock = clock.Add(1500 * time.Millisecond)

	waited, configured, ok := s.WaitForStartupMCP(s.Ctx)
	if !configured || !ok || waited != 2500*time.Millisecond {
		t.Fatalf("waited=%v configured=%v ok=%v, want 2.5s", waited, configured, ok)
	}
	if len(*slept) != 1 || (*slept)[0] != 2500*time.Millisecond {
		t.Fatalf("sleeps = %v, want [2.5s]", *slept)
	}
}

func TestStartupWaitSkippedWhenReadinessOutlastedIt(t *testing.T) {
	clock, slept := fakeStartupClock(t)
	s := newWaitSession(t)
	s.SetStartupWait(2 * time.Second)
	*clock = clock.Add(5 * time.Second)

	waited, configured, ok := s.WaitForStartupMCP(s.Ctx)
	if !configured || !ok || waited != 0 || len(*slept) != 0 {
		t.Fatalf("waited=%v configured=%v ok=%v sleeps=%v, want configured zero wait", waited, configured, ok, *slept)
	}
}

func TestStartupWaitAppliesToFirstPromptOnly(t *testing.T) {
	_, slept := fakeStartupClock(t)
	s := newWaitSession(t)
	s.SetStartupWait(3 * time.Second)

	if _, configured, _ := s.WaitForStartupMCP(s.Ctx); !configured {
		t.Fatal("first prompt did not wait")
	}
	if _, configured, ok := s.WaitForStartupMCP(s.Ctx); configured || !ok {
		t.Fatalf("second prompt configured=%v ok=%v, want no wait", configured, ok)
	}
	if len(*slept) != 1 {
		t.Fatalf("sleeps = %v, want exactly one", *slept)
	}
}

func TestStartupWaitStopsWhenContextEnds(t *testing.T) {
	fakeStartupClock(t)
	s := newWaitSession(t)
	s.SetStartupWait(3 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, ok := s.WaitForStartupMCP(ctx); ok {
		t.Fatal("wait reported ok after its context ended")
	}
}

func TestStartupSleepReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- startupSleep(ctx, time.Hour) }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("sleep completed instead of canceling")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sleep did not return after cancel")
	}
}
