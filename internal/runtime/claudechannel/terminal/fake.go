package terminal

import (
	"context"
	"sync"
)

// FakeSession is a terminal.Session for tests with controllable behavior.
type FakeSession struct {
	mu         sync.Mutex
	kind       string
	name       string
	pid        int
	captures   []string
	capIdx     int
	capErr     error
	alive      bool
	pasteCalls [][]string
	pasteErr   error
	sendCalls  [][]string
	sendNotify chan []string
	sendErr    error
	killErr    error
}

// NewFakeSession creates a FakeSession with the given initial capture text.
// Defaults Kind() to "tmux"; override via SetKind for PTY-mode tests.
func NewFakeSession(name string, pid int, capture string) *FakeSession {
	return &FakeSession{
		kind:     "tmux",
		name:     name,
		pid:      pid,
		captures: []string{capture},
		alive:    true,
	}
}

// NewFakeSessionQueue creates a FakeSession that drains a queue of capture snapshots.
func NewFakeSessionQueue(name string, pid int, captures []string) *FakeSession {
	return &FakeSession{
		kind:     "tmux",
		name:     name,
		pid:      pid,
		captures: captures,
		alive:    true,
	}
}

// Kind reports the configured backend identifier.
func (f *FakeSession) Kind() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kind
}

// SetKind overrides the backend identifier returned by Kind().
func (f *FakeSession) SetKind(k string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kind = k
}

// Capture returns the next queued capture text, or the error set via SetCapErr.
// Once the queue is drained the last value sticks — this mirrors a real
// terminal where Capture returns whatever is currently on screen, and avoids
// SUTs silently observing "" if they poll faster than test setup expected.
func (f *FakeSession) Capture(_ context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.capErr != nil {
		return "", f.capErr
	}
	if len(f.captures) == 0 {
		return "", nil
	}
	if f.capIdx >= len(f.captures) {
		return f.captures[len(f.captures)-1], nil
	}
	c := f.captures[f.capIdx]
	f.capIdx++
	return c, nil
}

// SetCapErr sets the error returned from Capture.
func (f *FakeSession) SetCapErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.capErr = err
}

// QueueCapture adds a capture snapshot to the queue.
func (f *FakeSession) QueueCapture(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.captures = append(f.captures, s)
}

// Alive returns the configured alive status.
func (f *FakeSession) Alive(_ context.Context) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.alive
}

// SetAlive controls the Alive() result.
func (f *FakeSession) SetAlive(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alive = v
}

// Name returns the session name.
func (f *FakeSession) Name() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.name
}

// PID returns the session PID.
func (f *FakeSession) PID() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pid
}

// PasteAndEnter records the paste call and returns the configured error.
func (f *FakeSession) PasteAndEnter(_ context.Context, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pasteCalls = append(f.pasteCalls, []string{text})
	return f.pasteErr
}

// PasteCalls returns the recorded PasteAndEnter calls.
func (f *FakeSession) PasteCalls() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.pasteCalls))
	for i, c := range f.pasteCalls {
		out[i] = append([]string{}, c...)
	}
	return out
}

// SetPasteErr sets the error returned from PasteAndEnter.
func (f *FakeSession) SetPasteErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pasteErr = err
}

// SendKeys records the key call and returns the configured error.
func (f *FakeSession) SendKeys(_ context.Context, keys ...Key) error {
	f.mu.Lock()
	strs := make([]string, len(keys))
	for i, k := range keys {
		strs[i] = string(k)
	}
	f.sendCalls = append(f.sendCalls, strs)
	notify := f.sendNotify
	err := f.sendErr
	f.mu.Unlock()

	if notify != nil {
		select {
		case notify <- append([]string{}, strs...):
		default:
		}
	}

	return err
}

// SendCalls returns the recorded SendKeys calls.
func (f *FakeSession) SendCalls() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.sendCalls))
	for i, c := range f.sendCalls {
		out[i] = append([]string{}, c...)
	}
	return out
}

// SetSendErr sets the error returned from SendKeys.
func (f *FakeSession) SetSendErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendErr = err
}

// SetSendNotify sets an optional channel notified on each SendKeys call.
func (f *FakeSession) SetSendNotify(ch chan []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendNotify = ch
}

// Kill returns the configured error.
func (f *FakeSession) Kill(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.killErr
}

// SetKillErr sets the error returned from Kill.
func (f *FakeSession) SetKillErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killErr = err
}

// SetName changes the session name.
func (f *FakeSession) SetName(n string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.name = n
}

// SetPID changes the session PID.
func (f *FakeSession) SetPID(pid int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pid = pid
}

var _ Session = (*FakeSession)(nil)
