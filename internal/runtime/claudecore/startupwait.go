package claudecore

import (
	"context"
	"time"
)

var (
	startupNow   = time.Now
	startupSleep = func(ctx context.Context, d time.Duration) bool {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
)

// SetStartupWait holds the first prompt until wait after this call, which the
// provider makes as soon as the terminal has launched. Zero leaves the first
// prompt unheld.
func (s *Session) SetStartupWait(wait time.Duration) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.startupWait = wait
	s.launchedAt = startupNow()
}

// SetStartupSleep replaces how this session's startup wait sleeps, so a caller
// can control the wait without a wall clock. fn reports false when ctx ended
// first.
func (s *Session) SetStartupSleep(fn func(ctx context.Context, d time.Duration) bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.startupSleepFn = fn
}

// WaitForStartupMCP holds the session's first prompt until the configured wait
// since launch has passed, so MCP servers Claude Code connects in the
// background are ready for the first turn. Time already spent reaching
// readiness counts toward the wait. Later calls return at once. It returns the
// extra time waited and whether a wait was configured; ok is false when ctx
// ended first.
func (s *Session) WaitForStartupMCP(ctx context.Context) (waited time.Duration, configured, ok bool) {
	s.Mu.Lock()
	wait, launchedAt, done := s.startupWait, s.launchedAt, s.startupWaitDone
	sleep := s.startupSleepFn
	if sleep == nil {
		sleep = startupSleep
	}
	if wait > 0 {
		s.startupWaitDone = true
	}
	s.Mu.Unlock()
	if wait <= 0 || done {
		return 0, false, true
	}
	start := startupNow()
	remaining := wait - start.Sub(launchedAt)
	if remaining <= 0 {
		return 0, true, true
	}
	if !sleep(ctx, remaining) {
		return 0, true, false
	}
	return startupNow().Sub(start), true, true
}
