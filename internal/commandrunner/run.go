//go:build unix

// Package commandrunner runs one supervisor-side child program under the
// safeguards its callers share. Every run uses no shell, an explicit
// environment, bounded stdout and stderr, and a deadline. The child's process
// group is killed on timeout, cancellation, output overflow, or child exit.
// Supervisor code that runs an external program under a declared contract uses
// this runner, so its safeguards cannot drift between those callers. Other
// supervisor code still calls git directly; this package makes no claim about
// call sites it does not serve.
//
// The child runs as the caller's user with the caller's privileges. The runner
// bounds and contains a process group. It does not isolate the child from the
// user that started it, and callers must not claim otherwise. A descendant that
// escapes the process group (its own session) is beyond the runner's reach, so
// it is not killed. The runner instead bounds its own wait for the child's
// output pipes and reports that the captured streams were cut short.
package commandrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Sentinel errors returned by Run.
var (
	// ErrTimeout is returned when the child outlives its deadline.
	ErrTimeout = errors.New("command timed out")
	// ErrCanceled is returned when the caller's context is canceled.
	ErrCanceled = errors.New("command canceled")
	// ErrOutputTooLarge is returned when stdout or stderr exceeds its limit.
	ErrOutputTooLarge = errors.New("command output exceeded limit")
)

// Limits bounds the captured stdout and stderr byte counts. A stream that
// would exceed its limit fails and kills the child's process group, so a
// runaway child cannot run until its deadline.
type Limits struct {
	Stdout int64
	Stderr int64
}

// Spec is one child program invocation.
type Spec struct {
	// Path is the resolved executable. It is exec'd directly: no shell, no
	// PATH lookup.
	Path string
	// Args is the full argv, including argv[0].
	Args []string
	// Dir is the child's working directory.
	Dir string
	// Env is the child's exact environment. It is never inherited: an empty
	// Env runs the child with an empty environment.
	Env []string
	// Stdin is written to the child's stdin when non-empty.
	Stdin []byte
	// Limits bounds the captured streams.
	Limits Limits
	// Timeout bounds the child's lifetime. Zero means no timer unless
	// TimeoutC supplies the deadline.
	Timeout time.Duration
	// TimeoutC, when non-nil, replaces the Timeout timer: the caller owns the
	// deadline signal. A caller that shares a scheduler passes its own channel.
	// A test uses it to fire the deadline deterministically rather than race a
	// wall-clock timer against process startup.
	TimeoutC <-chan time.Time
	// Redact lists values replaced with [REDACTED] in the captured stderr.
	Redact []string
}

// Result is one child run's captured outcome. Stdout and Stderr hold the bytes
// captured before the child ended, with stderr already redacted. A non-zero
// ExitCode carries the child's own exit error in ExitErr, not a runner error. A
// run that ended under an error still returns what it captured, so a caller can
// evidence a timed-out or killed child. StreamsHeld reports that the child
// exited while a descendant which escaped its process group still held the
// output pipes. The runner closed those pipes after pipeDrainDelay, so the
// captured streams may be incomplete and that descendant is still running.
type Result struct {
	Stdout      []byte
	Stderr      string
	ExitCode    int
	ExitErr     error
	StreamsHeld bool
}

// pipeDrainDelay bounds how long Wait keeps waiting on a child's output pipes
// after the child itself has exited. A descendant that escaped the process group
// can hold those pipes open indefinitely, and no caller may block on one.
const pipeDrainDelay = 500 * time.Millisecond

// Run executes spec and returns its bounded result. A child that ran and
// exited — zero or not — is a result, not an error. A deadline, cancellation,
// output overflow, or start failure returns an error wrapping one of this
// package's sentinels.
func Run(ctx context.Context, spec Spec) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrCanceled, err)
	}
	cmd := &exec.Cmd{
		Path: spec.Path,
		Args: spec.Args,
		Dir:  spec.Dir,
		Env:  envOrEmpty(spec.Env),
		// Start the child in its own process group so the whole group can be
		// killed on timeout, cancellation, or an output-limit violation.
		SysProcAttr: &syscall.SysProcAttr{Setpgid: true},
	}
	stdoutBuf := &boundedBuffer{limit: spec.Limits.Stdout}
	stderrBuf := &boundedBuffer{limit: spec.Limits.Stderr}
	// The child's process-group id is only known after Start, so the buffers'
	// kill hook waits for it. Start never waits on child output, so that wait
	// cannot deadlock. A kill before Start succeeded is a no-op (pgid stays
	// zero), and SIGKILL to a vanished group is not an error. Repeated kills
	// from stdout and stderr racing to the limit are therefore safe.
	pgidReady := make(chan struct{})
	var pgid int
	var killOnce sync.Once
	kill := func() {
		<-pgidReady
		if pgid == 0 {
			return
		}
		killOnce.Do(func() { killProcessGroup(pgid) })
	}
	stdoutBuf.kill = kill
	stderrBuf.kill = kill

	// The runner owns the child's output pipes rather than letting exec
	// interpose its own. exec's pipes make Wait block until every writer closes
	// them, which an escaped descendant can prevent forever, and they hide
	// whether that happened. With our own pipes, exec hands the descriptors
	// straight to the child and the drain below is ours to bound.
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		return Result{}, err
	}
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		stdoutRead.Close()
		stdoutWrite.Close()
		return Result{}, err
	}
	cmd.Stdout = stdoutWrite
	cmd.Stderr = stderrWrite

	if err := ctx.Err(); err != nil {
		close(pgidReady)
		stdoutRead.Close()
		stdoutWrite.Close()
		stderrRead.Close()
		stderrWrite.Close()
		return Result{}, fmt.Errorf("%w: %w", ErrCanceled, err)
	}
	var stdin io.WriteCloser
	if len(spec.Stdin) > 0 {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			stdoutRead.Close()
			stdoutWrite.Close()
			stderrRead.Close()
			stderrWrite.Close()
			return Result{}, err
		}
	}
	if err := cmd.Start(); err != nil {
		close(pgidReady)
		stdoutRead.Close()
		stdoutWrite.Close()
		stderrRead.Close()
		stderrWrite.Close()
		return Result{}, fmt.Errorf("start: %w", err)
	}
	pgid = cmd.Process.Pid
	close(pgidReady)
	// The child holds its own duplicates now: keeping ours open would withhold
	// EOF from the drain.
	stdoutWrite.Close()
	stderrWrite.Close()

	streams := &streamDrain{done: make(chan struct{})}
	go streams.drain(stdoutRead, stderrRead, stdoutBuf, stderrBuf)
	if stdin != nil {
		go func() {
			// A child that exits early makes this write fail with EPIPE; that
			// is the child's outcome, not a runner error.
			_, _ = stdin.Write(spec.Stdin)
			_ = stdin.Close()
		}()
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	return wait(ctx, spec, waitCh, stdoutBuf, stderrBuf, pgid, streams, stdoutRead, stderrRead)
}

// streamDrain reads the child's output pipes into the bounded buffers. Its done
// channel closes once both copies have returned.
type streamDrain struct {
	done chan struct{}
}

// drain copies both pipes to their buffers and closes done. A closed pipe alone
// does not stop the copies: a caller at the drain bound closes the read ends,
// which makes both return.
func (d *streamDrain) drain(stdoutRead, stderrRead *os.File, stdoutBuf, stderrBuf *boundedBuffer) {
	defer close(d.done)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// A copy error (a stream over its limit, or a read end closed at the
		// drain bound) is classified from the buffers, never from here.
		_, _ = io.Copy(stdoutBuf, stdoutRead)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(stderrBuf, stderrRead)
	}()
	wg.Wait()
}

// finish bounds the drain after the child is gone. It waits pipeDrainDelay for
// the streams to close, then closes the read ends, so an escaped descendant
// cannot pin the runner. It reports whether it had to force the close, in which
// case the captured streams may be incomplete.
func (d *streamDrain) finish(stdoutRead, stderrRead *os.File) (held bool) {
	select {
	case <-d.done:
	case <-time.After(pipeDrainDelay):
		held = true
		// Closing the read ends unblocks the copies. A writer that still holds
		// the pipe gets EPIPE on its next write, which is the escaped
		// descendant's concern, not the runner's.
		stdoutRead.Close()
		stderrRead.Close()
		<-d.done
	}
	stdoutRead.Close()
	stderrRead.Close()
	return held
}

// wait selects the child's exit, the deadline, or the context's cancellation,
// kills the child's process group on the two deadline paths, and classifies
// the outcome.
func wait(ctx context.Context, spec Spec, waitCh <-chan error, stdoutBuf, stderrBuf *boundedBuffer, pgid int, streams *streamDrain, stdoutRead, stderrRead *os.File) (Result, error) {
	var timeoutCh <-chan time.Time
	switch {
	case spec.TimeoutC != nil:
		timeoutCh = spec.TimeoutC
	case spec.Timeout > 0:
		timer := time.NewTimer(spec.Timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	var (
		waitErr  error
		runErr   error
		canceled bool
		timedOut bool
	)
	select {
	case waitErr = <-waitCh:
	case <-timeoutCh:
		timedOut = true
		killProcessGroup(pgid)
		waitErr = <-waitCh
	case <-ctx.Done():
		canceled = true
		killProcessGroup(pgid)
		waitErr = <-waitCh
	}

	// Reap any orphaned group members left behind by the child.
	killProcessGroup(pgid)

	// The drain is bounded from here, whatever ended the child. An escaped
	// descendant may still hold the pipes, and the captured bytes stop where
	// that bound falls.
	held := streams.finish(stdoutRead, stderrRead)

	// Only now may the buffers be inspected. The drain goroutines own buf and
	// exceeded until finish has joined them, so classifying a stream before the
	// join would race an in-flight copy. A cancellation keeps its own
	// classification, an over-limit stream outranks the deadline, and the
	// deadline outranks a plain exit.
	switch tooLarge := tooLargeError(spec, stdoutBuf, stderrBuf); {
	case canceled || ctx.Err() != nil:
		runErr = fmt.Errorf("%w: %w", ErrCanceled, ctx.Err())
	case tooLarge != nil:
		runErr = tooLarge
	case timedOut:
		runErr = fmt.Errorf("%w: exceeded %s", ErrTimeout, spec.Timeout)
	}

	result := Result{
		Stdout:      stdoutBuf.buf.Bytes(),
		Stderr:      redactValues(spec.Redact, stderrBuf.buf.String()),
		StreamsHeld: held,
	}
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			result.ExitCode = exitErr.ExitCode()
			result.ExitErr = waitErr
		} else if runErr == nil {
			runErr = waitErr
		}
	}
	return result, runErr
}

// envOrEmpty returns the child's exact environment. A nil Env runs the child
// with no environment at all; it never falls back to inheriting the parent's.
func envOrEmpty(env []string) []string {
	if env == nil {
		return []string{}
	}
	return env
}

// killProcessGroup sends SIGKILL to the whole process group. A vanished group
// is not an error.
func killProcessGroup(pgid int) {
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
}

// boundedBuffer captures a stream up to limit bytes. A write that would exceed
// the limit fails the stream, flags the buffer, and runs kill. The still-writing
// child is then killed instead of running until its deadline.
type boundedBuffer struct {
	buf      bytes.Buffer
	limit    int64
	exceeded bool
	kill     func()
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if int64(b.buf.Len())+int64(len(p)) > b.limit {
		b.exceeded = true
		if b.kill != nil {
			b.kill()
		}
		return 0, fmt.Errorf("%w: %d bytes exceeds the %d byte limit", ErrOutputTooLarge, int64(b.buf.Len())+int64(len(p)), b.limit)
	}
	return b.buf.Write(p)
}

func tooLargeError(spec Spec, stdout, stderr *boundedBuffer) error {
	if stdout.exceeded {
		return fmt.Errorf("%w: stdout exceeded %d bytes", ErrOutputTooLarge, spec.Limits.Stdout)
	}
	if stderr.exceeded {
		return fmt.Errorf("%w: stderr exceeded %d bytes", ErrOutputTooLarge, spec.Limits.Stderr)
	}
	return nil
}

func redactValues(values []string, stderr string) string {
	for _, value := range values {
		if value == "" {
			continue
		}
		stderr = strings.ReplaceAll(stderr, value, "[REDACTED]")
	}
	return stderr
}
