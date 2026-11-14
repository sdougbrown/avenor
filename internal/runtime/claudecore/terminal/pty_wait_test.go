//go:build linux || darwin || freebsd

package terminal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestPTYSessionWaitReapsAndIsIdempotent(t *testing.T) {
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{Command: "exec sh -c 'exit 7'"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := session.PID()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := session.Wait(ctx)
	if first == nil {
		t.Fatal("Wait succeeded for exit status 7")
	}
	second := session.Wait(ctx)
	if second == nil || second.Error() != first.Error() {
		t.Fatalf("second Wait = %v, want same result as %v", second, first)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("PID %d remains after Wait: %v", pid, err)
	}
}

func TestPTYSessionInterruptThenWaitReaps(t *testing.T) {
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{Command: `exec sh -c 'trap "exit 0" INT; while :; do :; done'`})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := session.PID()
	time.Sleep(20 * time.Millisecond)
	interrupter, ok := session.(InterruptSession)
	if !ok {
		t.Fatal("PTY session has no Interrupt capability")
	}
	if err := interrupter.Interrupt(context.Background()); err != nil {
		t.Fatalf("Interrupt: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(ctx); err != nil {
		t.Fatalf("Wait after Interrupt: %v", err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("PID %d remains after Interrupt/Wait: %v", pid, err)
	}
}

func TestPTYSessionKillThenWaitReaps(t *testing.T) {
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{Command: "exec sleep 30"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := session.PID()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	const waiters = 8
	results := make(chan error, waiters)
	var ready sync.WaitGroup
	ready.Add(waiters)
	for range waiters {
		go func() {
			ready.Done()
			results <- session.Wait(ctx)
		}()
	}
	ready.Wait()
	if err := session.Kill(ctx); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("Kill: %v", err)
	}
	for range waiters {
		if err := <-results; err == nil {
			t.Fatal("Wait succeeded after SIGKILL")
		}
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("PID %d remains after Kill/Wait: %v", pid, err)
	}
}

// wrapperTreeCommand hosts a stand-in launch wrapper: a shell that runs its
// payload as a child and cleans up when it is hung up, the way a wrapper that
// owns a VM or container does. It records the payload pid in pidFile and
// touches hupMarker from its hangup handler.
func wrapperTreeCommand(pidFile, hupMarker string) string {
	script := `trap "touch ` + hupMarker + `; kill \$child; wait \$child; exit 0" HUP; sleep 60 & child=$!; echo $child > ` + pidFile + `; wait`
	return "exec sh -c '" + script + "'"
}

// hangupTestGrace is short enough to keep a teardown test fast and long enough
// for the wrapper to act before the escalation.
const hangupTestGrace = 250 * time.Millisecond

// processGone reports whether pid has no process left to signal. A zombie
// counts as gone: it answers kill(pid, 0) until its parent reaps it, and a
// container whose init does not reap leaves orphans visible that way.
func processGone(pid int) bool {
	if err := syscall.Kill(pid, 0); err != nil {
		return true
	}
	if stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat")); err == nil {
		if fields := strings.Fields(string(stat)); len(fields) > 2 && fields[2] == "Z" {
			return true
		}
	}
	return false
}

func waitGone(t *testing.T, pid int, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !processGone(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("%s %d is still alive", what, pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// scriptCommand launches a script file from dir behind an exec, so the tracked
// pid is the script's own process.
func scriptCommand(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return "exec sh '" + path + "'"
}

// pidFromFile reads a pid a script recorded once it was running.
func pidFromFile(t *testing.T, path string) int {
	t.Helper()
	var pid int
	if _, err := fmt.Sscan(string(waitForFile(t, path)), &pid); err != nil {
		t.Fatalf("pid from %s: %v", path, err)
	}
	return pid
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func waitForFile(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
			return b
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never appeared", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A wrapped runtime is torn down the way terminal hangup tears it down at
// supervisor shutdown: the wrapper hears SIGHUP and gets to clean up its own
// tree. Killing only the hosted process outright leaves that tree running.
func TestPTYSessionTeardownLetsWrapperClean(t *testing.T) {
	stop := map[string]func(ctx context.Context, s Session, cancel context.CancelFunc){
		"kill":   func(ctx context.Context, s Session, _ context.CancelFunc) { _ = s.Kill(ctx) },
		"cancel": func(_ context.Context, _ Session, cancel context.CancelFunc) { cancel() },
	}
	for name, stopFn := range stop {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			pidFile, marker := dir+"/child.pid", dir+"/hup"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			session, err := (PTYLauncher{}).Start(ctx, StartOptions{Command: wrapperTreeCommand(pidFile, marker), HangupGrace: hangupTestGrace})
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			childPID := pidFromFile(t, pidFile)
			stopFn(context.Background(), session, cancel)

			waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer waitCancel()
			_ = session.Wait(waitCtx)
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("wrapper never heard SIGHUP: %v", err)
			}
			waitGone(t, childPID, "wrapped child")
		})
	}
}

// A wrapper that exits on the hangup can leave a member of its group running:
// a VM or container helper, or a tool child of a program the wrapper does not
// own, ignores SIGHUP. Kill must kill that leftover after the wrapper itself
// was already reaped.
func TestPTYSessionKillEscalatesToLeftoverGroupMembers(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "payload.pid")
	cmd := scriptCommand(t, dir, "wrapper.sh", `trap 'exit 0' HUP
sh -c 'trap "" HUP; echo $$ > "$0"; exec sleep 60' '`+pidFile+`' &
wait
`)
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{Command: cmd, HangupGrace: hangupTestGrace})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	wrapperPID, payloadPID := session.PID(), pidFromFile(t, pidFile)

	if err := session.Kill(context.Background()); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	waitGone(t, wrapperPID, "wrapper")
	waitGone(t, payloadPID, "payload that ignored SIGHUP")
}

// A wrapper that ignores the hangup leaves the whole tree in place. Kill must
// give it the grace and then kill the group, rather than reap only the leader
// ahead of the escalation.
func TestPTYSessionKillEscalatesWhenWrapperIgnoresHangup(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "wrapper.pid")
	cmd := scriptCommand(t, dir, "stubborn.sh", `trap '' HUP
echo $$ > "`+pidFile+`"
exec sleep 60
`)
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{Command: cmd, HangupGrace: hangupTestGrace})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	wrapperPID := pidFromFile(t, pidFile)

	start := time.Now()
	if err := session.Kill(context.Background()); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if elapsed := time.Since(start); elapsed < hangupTestGrace {
		t.Fatalf("Kill returned after %s, before the %s hangup grace", elapsed, hangupTestGrace)
	} else if elapsed > 3*time.Second {
		t.Fatalf("Kill took %s for a %s grace; the session's own grace did not drive the kill", elapsed, hangupTestGrace)
	}
	waitGone(t, wrapperPID, "wrapper that ignored SIGHUP")
}

// A cancelled ctx and the Kill that follows it are the same teardown: the
// group is hung up once, so a wrapper that is not idempotent does not run its
// teardown twice.
func TestPTYSessionCancelThenKillHangsUpOnce(t *testing.T) {
	dir := t.TempDir()
	hupLog, pidFile := filepath.Join(dir, "hangups"), filepath.Join(dir, "wrapper.pid")
	cmd := scriptCommand(t, dir, "counting.sh", `trap 'echo hup >> "`+hupLog+`"' HUP
echo $$ > "`+pidFile+`"
while :; do sleep 0.05; done
`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session, err := (PTYLauncher{}).Start(ctx, StartOptions{Command: cmd, HangupGrace: hangupTestGrace})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	wrapperPID := pidFromFile(t, pidFile)
	cancel()
	waitForFile(t, hupLog)

	if err := session.Kill(context.Background()); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	waitGone(t, wrapperPID, "wrapper")
	if got := strings.Count(readFileString(t, hupLog), "hup"); got != 1 {
		t.Fatalf("wrapper saw %d hangups, want 1", got)
	}
}

// A session without a hangup grace keeps the plain teardown: the hosted process
// is killed outright, without a hangup its wrapper could act on.
func TestPTYSessionKillWithoutGraceKillsOnlyTheHostedProcess(t *testing.T) {
	dir := t.TempDir()
	marker, pidFile := filepath.Join(dir, "hup"), filepath.Join(dir, "wrapper.pid")
	cmd := scriptCommand(t, dir, "wrapper.sh", `trap 'touch "`+marker+`"' HUP
echo $$ > "`+pidFile+`"
sleep 5 &
wait
`)
	session, err := (PTYLauncher{}).Start(context.Background(), StartOptions{Command: cmd})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The trap is in place only once the wrapper has run past it.
	wrapperPID := pidFromFile(t, pidFile)

	if err := session.Kill(context.Background()); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	waitGone(t, wrapperPID, "wrapper")
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("wrapper heard SIGHUP without a hangup grace")
	}
}
