//go:build linux || darwin || freebsd

package terminal

import (
	"context"
	"errors"
	"os"
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
