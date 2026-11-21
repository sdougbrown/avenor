package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/supervisor"
)

// conversationDrive is one isolated conversation session under test: a scratch
// home, sockets root and state root, and the environment every process it starts
// runs in.
type conversationDrive struct {
	bin  string
	root string
	env  []string
}

// newConversationDrive prepares a scratch session. The directory is short because
// the supervisor's unix socket path has to fit the socket name limit.
func newConversationDrive(t *testing.T) *conversationDrive {
	t.Helper()
	bin := buildAvenorBinary(t)
	root, err := os.MkdirTemp("/tmp", "avconv-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	home := filepath.Join(root, "home")
	avenorHome := filepath.Join(root, "avenor")
	state := filepath.Join(root, "state")
	for _, dir := range []string{home, avenorHome, state} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// The test derives the same sockets a session does, from the same root.
	t.Setenv("AVENOR_HOME", avenorHome)
	return &conversationDrive{
		bin:  bin,
		root: root,
		env: []string{
			"PATH=" + os.Getenv("PATH"),
			"HOME=" + home,
			"AVENOR_HOME=" + avenorHome,
			"XDG_STATE_HOME=" + state,
			"TMPDIR=" + root,
		},
	}
}

// feedProcess is one running feed and the files its output went to.
type feedProcess struct {
	cmd    *exec.Cmd
	stderr string
	stdout string
}

// startFeed runs one conversation feed as a real process, exactly as a host's feed
// manifest would, and returns it before the supervisor it starts is ready.
func (d *conversationDrive) startFeed(t *testing.T, session string) *feedProcess {
	t.Helper()
	lockDir := filepath.Join(d.root, "locks")
	stdout := filepath.Join(d.root, "feed-"+session+".out")
	stderr := filepath.Join(d.root, "feed-"+session+".err")
	out, err := os.Create(stdout)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	log, err := os.Create(stderr)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(d.bin, "feed", "broker",
		"--bind-host", "poo", "--bind-session", session,
		"--lock-dir", lockDir, "--request-timeout", "500ms")
	cmd.Env = d.env
	cmd.Stdout, cmd.Stderr = out, log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start feed %s: %v", session, err)
	}
	feed := &feedProcess{cmd: cmd, stdout: stdout, stderr: stderr}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return feed
}

// exit waits for the feed to end and returns its exit code.
func (f *feedProcess) exit(t *testing.T) int {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f.cmd.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			return 0
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		t.Fatalf("wait for feed: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatalf("feed did not exit within 20s; stderr: %s", f.logs(t))
	}
	return -1
}

// logs reads back what the feed wrote, for a failure message.
func (f *feedProcess) logs(t *testing.T) string {
	t.Helper()
	var b bytes.Buffer
	for _, name := range []string{f.stdout, f.stderr} {
		raw, _ := os.ReadFile(name)
		if len(raw) > 0 {
			b.WriteString(filepath.Base(name) + ": " + string(raw) + "\n")
		}
	}
	return b.String()
}

// conversationSocket is the socket the session's feed and tools derive.
func (d *conversationDrive) conversationSocket(t *testing.T, session string) string {
	t.Helper()
	socket, err := supervisor.ConversationSocket("poo", session)
	if err != nil {
		t.Fatalf("ConversationSocket: %v", err)
	}
	return socket
}

// awaitConversationSupervisor waits until the session's supervisor is live and
// answers the conversation's binding, and returns the owner PID behind it. The
// wait is the launch the feed performs; a command that arrives earlier is refused,
// never served by a supervisor of its own.
func (d *conversationDrive) awaitConversationSupervisor(t *testing.T, session string) (string, int) {
	t.Helper()
	socket := d.conversationSocket(t, session)
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		cl, err := client.AttachOwner(socket)
		if err == nil {
			_, lookupErr := cl.MessageBindLookupContext(context.Background(), messaging.MessageBindRequest{Host: "poo", SessionID: session})
			_ = cl.Close()
			if lookupErr == nil {
				record, err := control.LoadOwnerRecord(socket)
				if err != nil {
					t.Fatalf("owner record for %s: %v", socket, err)
				}
				return socket, record.OwnerPID
			}
			lastErr = lookupErr
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("conversation %s never got a live supervisor at %s: %v", session, socket, lastErr)
	return "", 0
}

// supervisorLive reports whether a supervisor still answers on socket.
func supervisorLive(socket string) bool {
	cl, err := client.AttachOwner(socket)
	if err != nil {
		return false
	}
	defer cl.Close()
	_, err = cl.Identity()
	return err == nil
}

// waitForSupervisorGone waits until nothing serves socket any more.
func waitForSupervisorGone(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !supervisorLive(socket) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("supervisor at %s is still running", socket)
}

// TestConversationFeedsOwnDistinctSupervisors is the per-session proof: two
// simultaneous feeds with different native sessions each start their own
// supervisor, stopping one feed stops only its own supervisor, and stopping the
// other stops the second.
func TestConversationFeedsOwnDistinctSupervisors(t *testing.T) {
	drive := newConversationDrive(t)

	feedA := drive.startFeed(t, "ses_feed_a")
	feedB := drive.startFeed(t, "ses_feed_b")
	socketA, pidA := drive.awaitConversationSupervisor(t, "ses_feed_a")
	socketB, pidB := drive.awaitConversationSupervisor(t, "ses_feed_b")

	if socketA == socketB {
		t.Fatalf("both conversations derived %s, want one supervisor per session", socketA)
	}
	if pidA == pidB {
		t.Fatalf("both conversations are served by supervisor pid %d, want distinct processes", pidA)
	}
	if dir := filepath.Dir(socketA); dir != filepath.Join(os.Getenv("AVENOR_HOME"), "sockets") {
		t.Fatalf("derived socket %s is outside the sockets root %s", socketA, dir)
	}

	// One feed stops: its own supervisor ends with it, the other is untouched.
	if err := feedA.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("stop feed A: %v", err)
	}
	if code := feedA.exit(t); code != 0 {
		t.Fatalf("feed A exit = %d, want a clean stop; %s", code, feedA.logs(t))
	}
	waitForSupervisorGone(t, socketA)
	if !supervisorLive(socketB) {
		t.Fatalf("supervisor %s stopped when the other conversation's feed exited; %s", socketB, feedB.logs(t))
	}
	cl, err := client.AttachOwner(socketB)
	if err != nil {
		t.Fatalf("attach the surviving supervisor: %v", err)
	}
	if _, err := cl.MessageBindLookupContext(context.Background(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_feed_b"}); err != nil {
		t.Fatalf("the surviving supervisor no longer serves its conversation: %v", err)
	}
	_ = cl.Close()

	// The last feed stops: its supervisor ends too.
	if err := feedB.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("stop feed B: %v", err)
	}
	if code := feedB.exit(t); code != 0 {
		t.Fatalf("feed B exit = %d, want a clean stop; %s", code, feedB.logs(t))
	}
	waitForSupervisorGone(t, socketB)
}

// TestConversationFeedKeepsAnExplicitSocketAttachOnly pins the configured path in a
// real process: with an explicit socket and nothing listening, the feed refuses
// and starts no supervisor.
func TestConversationFeedKeepsAnExplicitSocketAttachOnly(t *testing.T) {
	drive := newConversationDrive(t)
	socket := filepath.Join(drive.root, "configured.sock")
	stderr := filepath.Join(drive.root, "explicit.err")
	log, err := os.Create(stderr)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(drive.bin, "feed", "broker",
		"--socket", socket, "--bind-host", "poo", "--bind-session", "ses_explicit",
		"--lock-dir", filepath.Join(drive.root, "explicit-locks"))
	cmd.Env = drive.env
	cmd.Stdout, cmd.Stderr = nil, log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start feed: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		exitErr, _ := err.(*exec.ExitError)
		if exitErr == nil || exitErr.ExitCode() != 1 {
			t.Fatalf("feed exit = %v, want the attach refusal (exit 1)", err)
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("feed did not exit; stderr: %s", mustRead(t, stderr))
	}
	if reported := mustRead(t, stderr); !strings.Contains(reported, "attach the configured supervisor at "+socket) {
		t.Fatalf("stderr = %q, want the configured supervisor named", reported)
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("stat %s = %v, want no supervisor started for a configured socket", socket, err)
	}
	derived, err := supervisor.ConversationSocket("poo", "ses_explicit")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(derived); !os.IsNotExist(err) {
		t.Fatalf("stat %s = %v, want no derived supervisor either", derived, err)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
