//go:build unix

package durablefile

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestTryLock observes contention and kernel release after a process is killed.
func TestTryLock(t *testing.T) {
	if path := os.Getenv("AVENOR_TEST_LOCK_PATH"); path != "" {
		unlock, err := Lock(path)
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		fmt.Println("locked")
		time.Sleep(30 * time.Second)
		return
	}
	path := filepath.Join(t.TempDir(), "lock")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTryLock$")
	cmd.Env = append(os.Environ(), "AVENOR_TEST_LOCK_PATH="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	if !bufio.NewScanner(stdout).Scan() {
		t.Fatal("child did not acquire lock")
	}
	unlock, held, err := TryLock(path)
	if err != nil || !held || unlock != nil {
		t.Fatalf("TryLock while held = %v, %v, %v", unlock != nil, held, err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	unlock, held, err = TryLock(path)
	if err != nil || held || unlock == nil {
		t.Fatalf("TryLock after kill = %v, %v, %v", unlock != nil, held, err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
}
