//go:build unix

package commandrunner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// script writes an executable POSIX shell script fixture into dir and returns
// its absolute path. These fixtures are the child programs under test; the
// runner never interprets them through a shell of its own.
func script(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func TestRunPassesExactArgvAndEnvironment(t *testing.T) {
	dir := t.TempDir()
	path := script(t, dir, "dump.sh", `printf '%s\n' "$@" > argv.txt; env > env.txt`)

	res, err := Run(context.Background(), Spec{
		Path: path,
		Args: []string{path, "one two", "$HOME", "a;b"},
		Dir:  dir,
		Env:  []string{"ONLY_VAR=kept"},
		Limits: Limits{
			Stdout: 65536,
			Stderr: 65536,
		},
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 || res.ExitErr != nil {
		t.Fatalf("exit = %d/%v, want a clean exit", res.ExitCode, res.ExitErr)
	}
	argv, err := os.ReadFile(filepath.Join(dir, "argv.txt"))
	if err != nil {
		t.Fatalf("read argv: %v", err)
	}
	if got, want := string(argv), "one two\n$HOME\na;b\n"; got != want {
		t.Fatalf("argv = %q, want %q (no splitting, no expansion)", got, want)
	}
	// The shell itself adds PWD, SHLVL, and _; nothing else may appear.
	env, err := os.ReadFile(filepath.Join(dir, "env.txt"))
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(env)), "\n") {
		switch {
		case line == "ONLY_VAR=kept":
		case strings.HasPrefix(line, "PWD="), strings.HasPrefix(line, "SHLVL="), strings.HasPrefix(line, "_="):
		default:
			t.Fatalf("child environment = %q, want only the supplied variable plus the shell's own", env)
		}
	}
}

func TestRunNeverInheritsTheParentEnvironment(t *testing.T) {
	t.Setenv("RUNNER_PARENT_SECRET", "supervisor-credential")
	dir := t.TempDir()
	path := script(t, dir, "env.sh", `env`)

	res, err := Run(context.Background(), Spec{
		Path:    path,
		Args:    []string{path},
		Dir:     dir,
		Limits:  Limits{Stdout: 65536, Stderr: 65536},
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, banned := range []string{"supervisor-credential", "RUNNER_PARENT_SECRET=", "PATH=", "HOME="} {
		if strings.Contains(string(res.Stdout), banned) {
			t.Fatalf("child environment %q contains %s", res.Stdout, banned)
		}
	}
}

func TestRunDoesNotStartWithCanceledContext(t *testing.T) {
	dir := t.TempDir()
	path := script(t, dir, "check.sh", `echo ran > started`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(ctx, Spec{Path: path, Args: []string{path}, Dir: dir, Limits: Limits{Stdout: 1024, Stderr: 1024}, Timeout: time.Second})
	if !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want ErrCanceled and context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "started")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("check ran or stat failed: %v", err)
	}
	_, err = Run(ctx, Spec{Path: filepath.Join(dir, "missing"), Args: []string{"missing"}})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("missing executable error = %v, want ErrCanceled before start", err)
	}
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	_, err = Run(expired, Spec{Path: path, Args: []string{path}, Dir: dir})
	if !errors.Is(err, ErrCanceled) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired context error = %v, want both cancellation identities", err)
	}
}

func TestRunReportsExitCodeWithoutError(t *testing.T) {
	dir := t.TempDir()
	path := script(t, dir, "fail.sh", `printf 'checked\n'; printf 'bad state\n' >&2; exit 7`)

	res, err := Run(context.Background(), Spec{
		Path:    path,
		Args:    []string{path},
		Dir:     dir,
		Limits:  Limits{Stdout: 65536, Stderr: 65536},
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 7 || res.ExitErr == nil {
		t.Fatalf("exit = %d/%v, want code 7 with an exit error", res.ExitCode, res.ExitErr)
	}
	if string(res.Stdout) != "checked\n" || res.Stderr != "bad state\n" {
		t.Fatalf("captured stdout/stderr = %q/%q", res.Stdout, res.Stderr)
	}
}

func TestRunRedactsStderrValues(t *testing.T) {
	dir := t.TempDir()
	path := script(t, dir, "secret.sh", `printf 'token=%s\n' "$TOKEN" >&2; exit 1`)

	res, err := Run(context.Background(), Spec{
		Path:    path,
		Args:    []string{path},
		Dir:     dir,
		Env:     []string{"TOKEN=super-secret"},
		Redact:  []string{"super-secret"},
		Limits:  Limits{Stdout: 65536, Stderr: 65536},
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.Contains(res.Stderr, "super-secret") || !strings.Contains(res.Stderr, "[REDACTED]") {
		t.Fatalf("stderr = %q, want the secret redacted", res.Stderr)
	}
}

func TestRunBoundsStdoutAndKillsTheGroup(t *testing.T) {
	dir := t.TempDir()
	path := script(t, dir, "spew.sh", `echo $$ > shell.pid; exec yes`)
	start := time.Now()
	_, err := Run(context.Background(), Spec{
		Path:    path,
		Args:    []string{path},
		Dir:     dir,
		Limits:  Limits{Stdout: 1024, Stderr: 1024},
		Timeout: 30 * time.Second,
	})
	if !errors.Is(err, ErrOutputTooLarge) || !strings.Contains(err.Error(), "stdout") {
		t.Fatalf("error = %v, want ErrOutputTooLarge for stdout", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Run returned after %v, want an immediate kill", elapsed)
	}
	assertGroupGone(t, dir, "shell.pid")
}

// TestRunTimeoutKillsTheProcessGroup also proves a killed child's captured bytes
// come back with the error. The deadline is fired through the caller's channel
// once the child is running. The kill is then observed against a live child
// rather than racing a wall-clock timer against process startup.
func TestRunTimeoutKillsTheProcessGroup(t *testing.T) {
	dir := t.TempDir()
	// The stdout and stderr markers precede the pid file the test waits on, so
	// firing the deadline never precedes the output it asserts.
	path := script(t, dir, "hang.sh", `printf 'waiting\n'; printf 'still here\n' >&2; echo $$ > shell.pid; sleep 30`)
	timeoutC := make(chan time.Time, 1)
	done := make(chan runOutcome, 1)
	go func() {
		res, err := Run(context.Background(), Spec{
			Path:     path,
			Args:     []string{path},
			Dir:      dir,
			Limits:   Limits{Stdout: 65536, Stderr: 65536},
			Timeout:  30 * time.Second,
			TimeoutC: timeoutC,
		})
		done <- runOutcome{res, err}
	}()
	waitForPidFile(t, filepath.Join(dir, "shell.pid"))
	timeoutC <- time.Now()
	out := waitForRun(t, done)
	if !errors.Is(out.err, ErrTimeout) {
		t.Fatalf("error = %v, want ErrTimeout", out.err)
	}
	if string(out.res.Stdout) != "waiting\n" || out.res.Stderr != "still here\n" {
		t.Fatalf("captured stdout/stderr = %q/%q, want the bytes written before the deadline", out.res.Stdout, out.res.Stderr)
	}
	assertGroupGone(t, dir, "shell.pid")
}

// runOutcome is one Run call's result, carried off the run goroutine.
type runOutcome struct {
	res Result
	err error
}

// waitForRun waits for one Run call, failing the test instead of hanging when
// the runner does not return.
func waitForRun(t *testing.T, done <-chan runOutcome) runOutcome {
	t.Helper()
	select {
	case out := <-done:
		return out
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return; the runner is blocked on a child it cannot kill")
		return runOutcome{}
	}
}

// escapedDescendantScript is a child program whose leader starts a descendant in
// its own session. The descendant inherits the leader's stdout/stderr, so it keeps
// the pipes open after the leader's process group is gone. That is the case a
// process-group kill cannot reach.
const escapedDescendantScript = `
echo $$ > shell.pid
setsid sh -c 'echo $$ > escaped.pid; sleep 60' &
printf 'before deadline\n'
while [ ! -f escaped.pid ]; do sleep 0.01; done
`

// startEscapedDescendant runs body as a child that leaves an escaped descendant
// holding its output pipes, and returns the Run call's outcome channel.
func startEscapedDescendant(t *testing.T, dir, body string, spec Spec) <-chan runOutcome {
	t.Helper()
	path := script(t, dir, "escape.sh", body)
	spec.Path = path
	spec.Args = []string{path}
	spec.Dir = dir
	spec.Limits = Limits{Stdout: 65536, Stderr: 65536}
	done := make(chan runOutcome, 1)
	go func() {
		res, err := Run(context.Background(), spec)
		done <- runOutcome{res, err}
	}()
	return done
}

// escapedPID reads the escaped descendant's pid and registers its cleanup, so a
// failing assertion never leaves the process behind. The runner cannot kill it:
// it left the child's process group, which is exactly what these tests pin.
func escapedPID(t *testing.T, dir string) int {
	t.Helper()
	pid := readPidFile(t, filepath.Join(dir, "escaped.pid"))
	if pid <= 1 {
		t.Fatalf("escaped descendant recorded an implausible pid %d", pid)
	}
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })
	return pid
}

// TestRunTimeoutBoundsAnEscapedDescendant proves a deadline still returns when a
// descendant escaped the child's process group and holds its output pipes. The
// runner bounds its wait instead of blocking, keeps the bytes it captured, and
// flags the held streams. It leaves the escaped process to its owner: it is not
// killed, and the runner does not claim to kill it.
func TestRunTimeoutBoundsAnEscapedDescendant(t *testing.T) {
	dir := t.TempDir()
	timeoutC := make(chan time.Time, 1)
	done := startEscapedDescendant(t, dir, escapedDescendantScript+`sleep 60`+"\n", Spec{
		Timeout:  60 * time.Second,
		TimeoutC: timeoutC,
	})
	waitForPidFile(t, filepath.Join(dir, "shell.pid"))
	escaped := escapedPID(t, dir)
	timeoutC <- time.Now()

	out := waitForRun(t, done)
	if !errors.Is(out.err, ErrTimeout) {
		t.Fatalf("error = %v, want ErrTimeout", out.err)
	}
	if !strings.Contains(string(out.res.Stdout), "before deadline\n") {
		t.Fatalf("captured stdout = %q, want the bytes written before the deadline", out.res.Stdout)
	}
	if !out.res.StreamsHeld {
		t.Fatal("result does not report the escaped descendant holding its streams")
	}
	assertGroupGone(t, dir, "shell.pid")
	if err := syscall.Kill(escaped, 0); err != nil {
		t.Fatalf("escaped descendant %d is gone (%v); the runner must not claim to kill it", escaped, err)
	}
}

// TestRunCancellationDuringDrainDoesNotReportSuccess proves a cancellation that
// lands after the child exited cleanly, while an escaped descendant still holds
// the output pipes, is reported as ErrCanceled rather than as a clean result.
func TestRunCancellationDuringDrainDoesNotReportSuccess(t *testing.T) {
	dir := t.TempDir()
	path := script(t, dir, "check.sh", `echo $$ > shell.pid
setsid sh -c 'echo $$ > escaped.pid; sleep 30' &
while [ ! -f escaped.pid ]; do :; done
printf 'ready\n'
exit 0
`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan runOutcome, 1)
	go func() {
		res, err := Run(ctx, Spec{Path: path, Args: []string{path}, Dir: dir, Limits: Limits{Stdout: 65536, Stderr: 65536}, Timeout: 60 * time.Second})
		done <- runOutcome{res, err}
	}()
	escapedPID(t, dir)
	leaderPID := readPidFile(t, filepath.Join(dir, "shell.pid"))
	waitForPidGone(t, leaderPID)
	cancel()
	out := waitForRun(t, done)
	if !errors.Is(out.err, ErrCanceled) || !errors.Is(out.err, context.Canceled) {
		t.Fatalf("error = %v, want ErrCanceled and context.Canceled", out.err)
	}
	if !strings.Contains(string(out.res.Stdout), "ready\n") {
		t.Fatalf("stdout = %q, want captured output", out.res.Stdout)
	}
}

// TestRunCapturesBytesWrittenDuringTheDrainWindow proves the bound is a drain,
// not a cutoff. Bytes that arrive after the child exited but before the bound
// falls are captured, and the result still reports the streams as held. The
// ordering is observable, not timed. The escaped writer waits for a gate file the
// test creates only once the child's pid is gone, so the drain window is open. With the pipes closed at exit, the marker could never reach the capture.
func TestRunCapturesBytesWrittenDuringTheDrainWindow(t *testing.T) {
	dir := t.TempDir()
	body := `echo $$ > shell.pid
setsid sh -c 'echo $$ > escaped.pid; while [ ! -f go ]; do :; done; printf "late bytes\n"; sleep 30' &
while [ ! -f escaped.pid ]; do :; done
printf 'leader output\n'
exit 0
`
	done := startEscapedDescendant(t, dir, body, Spec{Timeout: 60 * time.Second})
	escaped := escapedPID(t, dir)
	leaderPID := readPidFile(t, filepath.Join(dir, "shell.pid"))
	waitForPidGone(t, leaderPID)
	// The child is gone and its output pipes are still being drained: this write
	// can only be captured inside the drain window.
	if err := os.WriteFile(filepath.Join(dir, "go"), nil, 0o600); err != nil {
		t.Fatalf("release the escaped writer: %v", err)
	}

	out := waitForRun(t, done)
	if out.err != nil {
		t.Fatalf("error = %v, want the child's own clean exit", out.err)
	}
	if out.res.ExitCode != 0 || out.res.ExitErr != nil {
		t.Fatalf("exit = %d/%v, want a clean exit", out.res.ExitCode, out.res.ExitErr)
	}
	if !out.res.StreamsHeld {
		t.Fatal("result does not report the escaped descendant holding its streams")
	}
	for _, want := range []string{"leader output\n", "late bytes\n"} {
		if !strings.Contains(string(out.res.Stdout), want) {
			t.Fatalf("captured stdout = %q, want the bytes written before and during the drain window (%q)", out.res.Stdout, want)
		}
	}
	assertGroupGone(t, dir, "shell.pid")
	if err := syscall.Kill(escaped, 0); err != nil {
		t.Fatalf("escaped descendant %d is gone (%v); the runner must not claim to kill it", escaped, err)
	}
}

// waitForPidGone blocks until pid is no longer signalable: the child was reaped,
// so its output pipes are only still open because something else holds them.
func waitForPidGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %d was never reaped", pid)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestRunBoundsANormalExitWithAnEscapedDescendant proves the same bound on the
// non-deadline path. The child exits on its own while an escaped descendant
// holds its pipes. Run then reports the child's own result, flags the cut-short
// streams, and returns instead of waiting for a pipe that may never close.
func TestRunBoundsANormalExitWithAnEscapedDescendant(t *testing.T) {
	dir := t.TempDir()
	body := `echo $$ > shell.pid
setsid sh -c 'echo $$ > escaped.pid; sleep 60' &
printf 'leader output\n'
while [ ! -f escaped.pid ]; do sleep 0.01; done
exit 0
`
	done := startEscapedDescendant(t, dir, body, Spec{Timeout: 60 * time.Second})
	escaped := escapedPID(t, dir)

	start := time.Now()
	out := waitForRun(t, done)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Run returned after %v, want a return bounded by the pipe drain delay", elapsed)
	}
	if out.err != nil {
		t.Fatalf("error = %v, want the child's own clean exit", out.err)
	}
	if out.res.ExitCode != 0 || out.res.ExitErr != nil {
		t.Fatalf("exit = %d/%v, want a clean exit", out.res.ExitCode, out.res.ExitErr)
	}
	if !out.res.StreamsHeld {
		t.Fatal("result does not report the escaped descendant holding its streams")
	}
	if !strings.Contains(string(out.res.Stdout), "leader output\n") {
		t.Fatalf("captured stdout = %q, want the child's own output", out.res.Stdout)
	}
	assertGroupGone(t, dir, "shell.pid")
	if err := syscall.Kill(escaped, 0); err != nil {
		t.Fatalf("escaped descendant %d is gone (%v); the runner must not claim to kill it", escaped, err)
	}
}

// TestRunConcurrentDeadlineAndOutputStreams proves a deadline that lands while a
// stream is still being copied neither races the drain goroutine nor hangs. The
// topology makes both events happen in one run, deterministically. The leader is
// gated so the deadline channel is the branch that fires, and a descendant that
// escaped the process group keeps writing stderr afterwards. It crosses its
// limit while the runner still holds the drain, so classifying the streams before
// the join races that live copy.
func TestRunConcurrentDeadlineAndOutputStreams(t *testing.T) {
	dir := t.TempDir()
	goFile := filepath.Join(dir, "go")
	body := `echo $$ > shell.pid
setsid sh -c 'echo $$ > escaped.pid; while :; do printf "escaped stderr flood line\n" >&2; sleep 0.01; done' >/dev/null &
while [ ! -f escaped.pid ]; do sleep 0.01; done
while [ ! -f go ]; do sleep 0.01; done
exec yes 'leader stdout flood'
`
	path := script(t, dir, "flood.sh", body)
	timeoutC := make(chan time.Time, 1)
	done := make(chan runOutcome, 1)
	go func() {
		res, err := Run(context.Background(), Spec{
			Path:     path,
			Args:     []string{path},
			Dir:      dir,
			Limits:   Limits{Stdout: 4096, Stderr: 64},
			Timeout:  30 * time.Second,
			TimeoutC: timeoutC,
		})
		done <- runOutcome{res, err}
	}()
	waitForPidFile(t, filepath.Join(dir, "shell.pid"))
	escaped := escapedPID(t, dir)
	// The escaped writer is producing; nothing has overflowed yet. The deadline
	// fires first, so the deadline branch is the one that classifies the streams
	// while that stderr copy is still live.
	timeoutC <- time.Now()
	if err := os.WriteFile(goFile, nil, 0o600); err != nil {
		t.Fatalf("release the leader: %v", err)
	}

	out := waitForRun(t, done)
	if !errors.Is(out.err, ErrTimeout) && !errors.Is(out.err, ErrOutputTooLarge) {
		t.Fatalf("error = %v, want the deadline (or the over-limit stream it uncovers)", out.err)
	}
	if int64(len(out.res.Stdout)) > 4096 || int64(len(out.res.Stderr)) > 64 {
		t.Fatalf("captured %d/%d bytes, want them capped at 4096/64", len(out.res.Stdout), len(out.res.Stderr))
	}
	assertGroupGone(t, dir, "shell.pid")
	// The escaped writer is outside the killed group, and nothing here claims the
	// runner killed it. It stops only because the closed read end turns its next
	// write into EPIPE. The two escaped-descendant tests whose writers stay quiet
	// cover the liveness claim.
	_ = escaped
}

func TestRunCancellationKillsTheProcessGroup(t *testing.T) {
	dir := t.TempDir()
	path := script(t, dir, "cancel.sh", `echo $$ > shell.pid; sleep 30`)
	ctx, cancel := context.WithCancel(context.Background())
	readyPath := filepath.Join(dir, "shell.pid")
	done := make(chan error, 1)
	go func() {
		_, err := Run(ctx, Spec{
			Path:    path,
			Args:    []string{path},
			Dir:     dir,
			Limits:  Limits{Stdout: 65536, Stderr: 65536},
			Timeout: 30 * time.Second,
		})
		done <- err
	}()
	waitForPidFile(t, readyPath)
	cancel()
	if err := <-done; !errors.Is(err, ErrCanceled) {
		t.Fatalf("error = %v, want ErrCanceled", err)
	}
	assertGroupGone(t, dir, "shell.pid")
}

func TestRunReportsStartFailure(t *testing.T) {
	_, err := Run(context.Background(), Spec{
		Path:    filepath.Join(t.TempDir(), "absent"),
		Args:    []string{filepath.Join(t.TempDir(), "absent")},
		Limits:  Limits{Stdout: 1024, Stderr: 1024},
		Timeout: time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "start") {
		t.Fatalf("error = %v, want a start failure", err)
	}
}

func waitForPidFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if data, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(data)) != "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never appeared; the child never got past startup", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readPidFile reads a pid file a fixture wrote, retrying while the file is still
// empty (the shell creates it before writing the pid).
func readPidFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if data, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(data)) != "" {
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				t.Fatalf("bad pid %q in %s: %v", data, path, err)
			}
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never received a pid", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertGroupGone proves the child's whole process group is reaped: the leader
// pid the fixture recorded is no longer signalable.
func assertGroupGone(t *testing.T, dir, pidFile string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, pidFile))
	if err != nil {
		t.Fatalf("read %s: %v", pidFile, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("bad pid %q: %v", data, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group leader %d still alive", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
