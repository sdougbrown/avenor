package supervisor

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestStartDefaultSocketKeepsTheHomeRoot pins the MCP server's existing default:
// an autostarted supervisor with no socket named binds under ~/.avenor/sockets,
// and AVENOR_HOME does not move it. A conversation's derived socket is the one
// that follows AVENOR_HOME, so the two roots must not be conflated.
func TestStartDefaultSocketKeepsTheHomeRoot(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("AVENOR_HOME", filepath.Join(home, "elsewhere"))

	var args []string
	origExec := ExecCommand
	ExecCommand = func(_ string, childArgs ...string) *exec.Cmd {
		args = childArgs
		return exec.Command("/nonexistent/avenor-test-stub")
	}
	t.Cleanup(func() { ExecCommand = origExec })
	origTimeout := StartupTimeout
	StartupTimeout = 100 * time.Millisecond
	t.Cleanup(func() { StartupTimeout = origTimeout })

	if _, err := Start("", 0, "", Options{}); err == nil {
		t.Fatal("Start with a missing binary = nil error, want a startup failure")
	}
	want := filepath.Join(home, ".avenor", "sockets", fmt.Sprintf("avenor-mcp-%d.sock", os.Getpid()))
	if !strings.Contains(strings.Join(args, " "), want) {
		t.Fatalf("supervisor args = %v, want the default socket %s", args, want)
	}
}
