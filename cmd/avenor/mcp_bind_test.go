package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sdougbrown/avenor/internal/mcpserver"
	"golang.org/x/sys/unix"
)

func TestMCPBindFlagError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transport string
		host      string
		session   string
		fd        int
		hostSet   bool
		sessSet   bool
		fdSet     bool
		wantErr   string
	}{
		{name: "undeclared", transport: "stdio"},
		{name: "declared", transport: "stdio", host: "poo", session: "ses_1", fd: 3, hostSet: true, sessSet: true, fdSet: true},
		{name: "host alone", transport: "stdio", host: "poo", hostSet: true, wantErr: "supplied together or not at all"},
		{name: "session alone", transport: "stdio", session: "ses_1", sessSet: true, wantErr: "supplied together or not at all"},
		{name: "descriptor alone", transport: "stdio", fd: 3, fdSet: true, wantErr: "supplied together or not at all"},
		{name: "missing descriptor", transport: "stdio", host: "poo", session: "ses_1", hostSet: true, sessSet: true, wantErr: "supplied together or not at all"},
		{name: "http transport", transport: "http", host: "poo", session: "ses_1", fd: 3, hostSet: true, sessSet: true, fdSet: true, wantErr: "only supported with --transport stdio"},
		{name: "empty host", transport: "stdio", session: "ses_1", fd: 3, hostSet: true, sessSet: true, fdSet: true, wantErr: "require non-empty values"},
		{name: "empty session", transport: "stdio", host: "poo", fd: 3, hostSet: true, sessSet: true, fdSet: true, wantErr: "require non-empty values"},
		{name: "stdin", transport: "stdio", host: "poo", session: "ses_1", fd: 0, hostSet: true, sessSet: true, fdSet: true, wantErr: "other than 0, 1 and 2"},
		{name: "stdout", transport: "stdio", host: "poo", session: "ses_1", fd: 1, hostSet: true, sessSet: true, fdSet: true, wantErr: "other than 0, 1 and 2"},
		{name: "stderr", transport: "stdio", host: "poo", session: "ses_1", fd: 2, hostSet: true, sessSet: true, fdSet: true, wantErr: "other than 0, 1 and 2"},
		{name: "negative", transport: "stdio", host: "poo", session: "ses_1", fd: -3, hostSet: true, sessSet: true, fdSet: true, wantErr: "other than 0, 1 and 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := mcpBindFlagError(tc.transport, tc.host, tc.session, tc.fd, tc.hostSet, tc.sessSet, tc.fdSet)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("mcpBindFlagError = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("mcpBindFlagError = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// TestValidateBindHandoff pins what the launch accepts as its handoff: a live
// pipe, never a file or a descriptor the host did not inherit.
func TestValidateBindHandoff(t *testing.T) {
	t.Run("pipe", func(t *testing.T) {
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer read.Close()
		// A host hands over a descriptor that is not already close-on-exec.
		if _, err := unix.FcntlInt(write.Fd(), unix.F_SETFD, 0); err != nil {
			t.Fatalf("clear descriptor flags: %v", err)
		}
		handoff, err := validateBindHandoff(int(write.Fd()))
		if err != nil {
			t.Fatalf("validateBindHandoff(pipe) = %v, want nil", err)
		}
		if handoff == nil {
			t.Fatal("validateBindHandoff(pipe) = nil writer")
		}
		// Nothing this launch starts may keep the write end, or the host would
		// wait for an EOF that never comes.
		file, ok := handoff.(*os.File)
		if !ok {
			t.Fatalf("handoff = %T, want the descriptor", handoff)
		}
		flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
		if err != nil {
			t.Fatalf("read descriptor flags: %v", err)
		}
		if flags&unix.FD_CLOEXEC == 0 {
			t.Fatal("the handoff descriptor is not close-on-exec")
		}
		if err := handoff.Close(); err != nil {
			t.Fatalf("close handoff: %v", err)
		}
	})

	t.Run("read-only fifo", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "handoff.fifo")
		if err := unix.Mkfifo(path, 0o600); err != nil {
			t.Fatalf("mkfifo: %v", err)
		}
		fifo, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
		if err != nil {
			t.Fatalf("open fifo: %v", err)
		}
		defer fifo.Close()
		if _, err := validateBindHandoff(int(fifo.Fd())); err == nil || !strings.Contains(err.Error(), "not writable") {
			t.Fatalf("validateBindHandoff(read-only fifo) = %v, want the not-writable refusal", err)
		}
	})

	t.Run("regular file", func(t *testing.T) {
		file, err := os.Create(filepath.Join(t.TempDir(), "handoff"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validateBindHandoff(int(file.Fd())); err == nil || !strings.Contains(err.Error(), "not a pipe") {
			t.Fatalf("validateBindHandoff(file) = %v, want the not-a-pipe refusal", err)
		}
	})

	t.Run("closed descriptor", func(t *testing.T) {
		file, err := os.Create(filepath.Join(t.TempDir(), "closed"))
		if err != nil {
			t.Fatal(err)
		}
		fd := int(file.Fd())
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := validateBindHandoff(fd); err == nil || !strings.Contains(err.Error(), "not an inherited open descriptor") {
			t.Fatalf("validateBindHandoff(closed) = %v, want the closed-descriptor refusal", err)
		}
	})
}

// TestMCPBindDeclarationReachesTheServer pins what handleMCP forwards: the
// declared conversation and the inherited pipe it hands the identity over on.
func TestMCPBindDeclarationReachesTheServer(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	var captured mcpserver.Options
	orig := newMCPServerFn
	newMCPServerFn = func(opts mcpserver.Options) (*mcpserver.Server, error) {
		captured = opts
		return nil, errors.New("capture only")
	}
	t.Cleanup(func() { newMCPServerFn = orig })

	exit := runDeclared([]string{
		"mcp", "--transport", "stdio",
		"--bind-host", "poo", "--bind-session", "ses_cli",
		"--bind-output-fd", strconv.Itoa(int(write.Fd())),
	}, io.Discard, io.Discard)
	if exit != 1 {
		t.Fatalf("runDeclared = %d, want 1 from the capture constructor", exit)
	}
	if captured.BindHost != "poo" || captured.BindSession != "ses_cli" {
		t.Fatalf("bind declaration = (%q, %q), want the supplied conversation", captured.BindHost, captured.BindSession)
	}
	if captured.BindHandoff == nil {
		t.Fatal("the server was not given the handoff pipe")
	}
	if line, _ := io.ReadAll(read); len(line) != 0 {
		t.Fatalf("handoff = %q, want nothing before a server serves", line)
	}
}

// TestMCPBindFlagRejections drives the rejected declarations through the declared
// command, so a launch mistake exits with the usage code and a message that
// names the flag rather than starting a server.
func TestMCPBindFlagRejections(t *testing.T) {
	t.Setenv("MCP_AUTH_TOKEN", "")
	for _, tc := range []struct {
		name   string
		args   []string
		errMsg string
	}{
		{
			name:   "incomplete declaration",
			args:   []string{"mcp", "--bind-host", "poo", "--bind-output-fd", "3"},
			errMsg: "supplied together or not at all",
		},
		{
			name:   "http transport",
			args:   []string{"mcp", "--transport", "http", "--bind-host", "poo", "--bind-session", "ses_1", "--bind-output-fd", "3"},
			errMsg: "only supported with --transport stdio",
		},
		{
			name:   "stdout descriptor",
			args:   []string{"mcp", "--bind-host", "poo", "--bind-session", "ses_1", "--bind-output-fd", "1"},
			errMsg: "other than 0, 1 and 2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			if exit := runDeclared(tc.args, io.Discard, &stderr); exit != 2 {
				t.Fatalf("runDeclared = %d, want the usage exit code 2; stderr: %s", exit, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.errMsg) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.errMsg)
			}
		})
	}
}

// bindDriveEnv is the isolated environment a launched supervisor and MCP server
// run in: the scratch home keeps an installed config or credential out of the
// drive.
func bindDriveEnv(scratch string) []string {
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + filepath.Join(scratch, "home"),
		"AVENOR_HOME=" + filepath.Join(scratch, "avenor"),
		"TMPDIR=" + scratch,
	}
}

// startBindDriveSupervisor starts the real supervisor on socket and waits until
// it accepts connections.
func startBindDriveSupervisor(t *testing.T, bin string, env []string, socket string) {
	t.Helper()
	cmd := exec.Command(bin, "stable", "--control-socket", socket)
	cmd.Env = env
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	deadline := time.Now().Add(20 * time.Second)
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("supervisor never listened on %s: %s", socket, logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMCPSubprocessHandsOverTheConversationIdentity drives the real binary over
// real stdio with the identity on an inherited private pipe: the launch binds the
// conversation against a real supervisor, writes one JSON line on the pipe, and
// closes it before it answers a single MCP frame. The credential stays on that
// pipe — not on the child's stderr, not in a tool result — and the protocol
// frames on stdout stay intact.
func TestMCPSubprocessHandsOverTheConversationIdentity(t *testing.T) {
	bin := buildAvenorBinary(t)
	// A short scratch directory keeps the supervisor's unix socket path under
	// the socket length limit.
	scratch, err := os.MkdirTemp("/tmp", "avmcp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	env := bindDriveEnv(scratch)
	for _, dir := range []string{filepath.Join(scratch, "home"), filepath.Join(scratch, "avenor")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	socket := filepath.Join(scratch, "stable.sock")
	startBindDriveSupervisor(t, bin, env, socket)

	handoffRead, handoffWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer handoffRead.Close()
	cmd := exec.Command(bin, "mcp", "--transport", "stdio",
		"--supervisor-socket", socket, "--no-autostart",
		"--bind-host", "poo", "--bind-session", "ses_subprocess",
		"--bind-output-fd", "3")
	cmd.Env = env
	// The inherited pipe is the first extra file, so it is descriptor 3 in the
	// child — a descriptor MCP does not read or write.
	cmd.ExtraFiles = []*os.File{handoffWrite}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	// The child holds the only writer now, so the pipe reaches EOF when the
	// launch closes its copy — which is what a host reads.
	if err := handoffWrite.Close(); err != nil {
		t.Fatalf("close the parent's handoff copy: %v", err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	// The identity must arrive before any MCP frame is exchanged: nothing has
	// been written to stdin yet.
	if err := handoffRead.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := io.ReadAll(handoffRead)
	if err != nil {
		t.Fatalf("read handoff: %v (stderr: %s)", err, stderr.String())
	}
	if len(line) == 0 {
		t.Fatalf("the launch handed over no identity: %s", stderr.String())
	}
	var identity map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(line), &identity); err != nil {
		t.Fatalf("handoff line %q: %v", line, err)
	}
	if !bytes.HasSuffix(line, []byte("\n")) || bytes.Count(line, []byte("\n")) != 1 {
		t.Fatalf("handoff = %q, want exactly one newline-terminated line", line)
	}
	for _, field := range []string{"broker_url", "run_id", "run_token", "generation"} {
		if value, _ := identity[field].(string); value == "" {
			t.Fatalf("handoff %q = %v, want a non-empty %s", field, identity, field)
		}
	}
	if identity["run_id"] == "supervisor" || identity["created"] != true {
		t.Fatalf("handoff identity = %v, want the conversation's own minted run", identity)
	}
	token, _ := identity["run_token"].(string)
	if strings.Contains(stderr.String(), token) {
		t.Fatal("the launched server reported the credential on stderr")
	}

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "bind-drive", Version: "1"}, nil)
	cs, err := client.Connect(ctx, &mcpsdk.IOTransport{Reader: stdout, Writer: stdin}, nil)
	if err != nil {
		t.Fatalf("connect over stdio: %v (stderr: %s)", err, stderr.String())
	}
	defer func() { _ = cs.Close() }()

	// A real round trip proves the protocol framing on the child's stdout is
	// intact beside the handoff pipe, and that no tool result carries the
	// credential.
	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list over stdio: %v (stderr: %s)", err, stderr.String())
	}
	var names []string
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
	}
	for _, want := range []string{"spawn", "result", "message"} {
		if !strings.Contains(strings.Join(names, ","), want) {
			t.Fatalf("tools/list = %v, want it to include %q", names, want)
		}
	}
	encoded, err := json.Marshal(tools)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), token) {
		t.Fatal("a tool listing carried the conversation credential")
	}

	// Closing the client closes stdin, which ends the server: the pipe was
	// already closed after the handoff, so nothing here waits on it.
	if err := cs.Close(); err != nil {
		t.Fatalf("close session: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("mcp exited with %v; stderr: %s", err, stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the server did not exit after stdin closed")
	}
	if strings.Contains(stderr.String(), token) {
		t.Fatal("the launched server reported the credential on stderr")
	}
}

// TestMCPSubprocessRefusesAClosedHandoff pins the launch's failure path: a
// consumer that is already gone fails the command with a message that names the
// pipe and leaks no credential, instead of serving a conversation nobody holds.
func TestMCPSubprocessRefusesAClosedHandoff(t *testing.T) {
	bin := buildAvenorBinary(t)
	scratch, err := os.MkdirTemp("/tmp", "avmcp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	env := bindDriveEnv(scratch)
	for _, dir := range []string{filepath.Join(scratch, "home"), filepath.Join(scratch, "avenor")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	socket := filepath.Join(scratch, "stable.sock")
	startBindDriveSupervisor(t, bin, env, socket)

	// The host opens the pipe and drops its read end, so the handoff write has
	// nowhere to land.
	handoffRead, handoffWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := handoffRead.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "mcp", "--transport", "stdio",
		"--supervisor-socket", socket, "--no-autostart",
		"--bind-host", "poo", "--bind-session", "ses_closed",
		"--bind-output-fd", "3")
	cmd.Env = env
	cmd.ExtraFiles = []*os.File{handoffWrite}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the launch succeeded with no consumer for the handoff pipe")
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the launch hung on a handoff pipe with no reader")
	}
	got := stderr.String()
	if !strings.Contains(got, "hand off to the launch pipe") {
		t.Fatalf("stderr = %q, want the handoff refusal", got)
	}
	if strings.Contains(got, "run_token") {
		t.Fatalf("stderr = %q, want no credential", got)
	}
}

// TestMCPSubprocessRefusesANonPipeHandoff pins the descriptor rule: a launch
// pointed at something other than an inherited pipe refuses before binding, so a
// credential can never be written to a file.
func TestMCPSubprocessRefusesANonPipeHandoff(t *testing.T) {
	bin := buildAvenorBinary(t)
	scratch, err := os.MkdirTemp("/tmp", "avmcp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	env := bindDriveEnv(scratch)

	target, err := os.Create(filepath.Join(scratch, "handoff.out"))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	cmd := exec.Command(bin, "mcp", "--transport", "stdio",
		"--supervisor-socket", filepath.Join(scratch, "absent.sock"), "--no-autostart",
		"--bind-host", "poo", "--bind-session", "ses_file",
		"--bind-output-fd", "3")
	cmd.Env = env
	cmd.ExtraFiles = []*os.File{target}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("the launch accepted a regular file as its handoff")
	}
	if !strings.Contains(stderr.String(), "is not a pipe") {
		t.Fatalf("stderr = %q, want the not-a-pipe refusal", stderr.String())
	}
	info, err := target.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("the handoff file holds %d bytes; a credential must never reach a file", info.Size())
	}
}

// fullPipe prepares a pipe whose write end is blocking (the mode a host hands
// over) and full, so a launch that writes to it without a bound waits forever.
// The reader stays open and never drains it.
func fullPipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close() })
	if err := write.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("set fill deadline: %v", err)
	}
	if _, err := write.Write(make([]byte, 1<<20)); err == nil {
		t.Fatal("filling the pipe did not stop the fill write")
	}
	if err := write.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear fill deadline: %v", err)
	}
	// os.Pipe hands back a non-blocking descriptor; a host's pipe is blocking.
	if err := unix.SetNonblock(int(write.Fd()), false); err != nil {
		t.Fatalf("restore blocking mode: %v", err)
	}
	return read, write
}

// TestMCPSubprocessWithAFullBlockingHandoffPipeExitsBounded pins the launch's
// bound on a host that is not draining its pipe: the inherited descriptor is
// blocking (Go's os.NewFile does not register a blocking descriptor with the
// runtime poller), so a launch that cannot bound the write would wait forever.
// It must instead exit on its own with the handoff refusal.
func TestMCPSubprocessWithAFullBlockingHandoffPipeExitsBounded(t *testing.T) {
	bin := buildAvenorBinary(t)
	scratch, err := os.MkdirTemp("/tmp", "avmcp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	env := bindDriveEnv(scratch)
	for _, dir := range []string{filepath.Join(scratch, "home"), filepath.Join(scratch, "avenor")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	socket := filepath.Join(scratch, "stable.sock")
	startBindDriveSupervisor(t, bin, env, socket)

	// Descriptor 3 carries a placeholder so the handoff lands on 4, where an
	// autostarted child's own extra files cannot displace it.
	placeholder, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer placeholder.Close()
	_, handoffWrite := fullPipe(t)
	cmd := exec.Command(bin, "mcp", "--transport", "stdio",
		"--supervisor-socket", socket, "--no-autostart",
		"--bind-host", "poo", "--bind-session", "ses_full",
		"--bind-output-fd", "4")
	cmd.Env = env
	cmd.ExtraFiles = []*os.File{placeholder, handoffWrite}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	// The child holds the only writer now; the reader never drains it.
	if err := handoffWrite.Close(); err != nil {
		t.Fatalf("close the parent's handoff copy: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the launch succeeded with a full pipe nobody drains")
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the launch waited on a host that never drained its handoff pipe")
	}
	got := stderr.String()
	if !strings.Contains(got, "hand off to the launch pipe") || !strings.Contains(got, "not reading the handoff pipe") {
		t.Fatalf("stderr = %q, want the handoff refusal naming the unread pipe", got)
	}
	if strings.Contains(got, "run_token") {
		t.Fatalf("stderr = %q, want no credential", got)
	}
}

// TestMCPSubprocessAutostartsAndHandsOff pins the normal launch a host uses when
// it lets Avenor own the supervisor: the identity arrives on the inherited pipe,
// the pipe reaches EOF — so the autostarted supervisor did not keep a write end —
// and the protocol still round-trips on stdout.
func TestMCPSubprocessAutostartsAndHandsOff(t *testing.T) {
	bin := buildAvenorBinary(t)
	scratch, err := os.MkdirTemp("/tmp", "avmcp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	env := bindDriveEnv(scratch)
	for _, dir := range []string{filepath.Join(scratch, "home"), filepath.Join(scratch, "avenor")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	socket := filepath.Join(scratch, "autostart.sock")
	// The supervisor this launch starts belongs to the drive, not the operator.
	t.Cleanup(func() {
		stop := exec.Command(bin, "control", "--socket", socket, "shutdown")
		stop.Env = env
		_ = stop.Run()
	})

	placeholder, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer placeholder.Close()
	handoffRead, handoffWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer handoffRead.Close()
	cmd := exec.Command(bin, "mcp", "--transport", "stdio",
		"--control-socket", socket,
		"--bind-host", "poo", "--bind-session", "ses_autostart",
		"--bind-output-fd", "4")
	cmd.Env = env
	cmd.ExtraFiles = []*os.File{placeholder, handoffWrite}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	if err := handoffWrite.Close(); err != nil {
		t.Fatalf("close the parent's handoff copy: %v", err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	if err := handoffRead.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := io.ReadAll(handoffRead)
	if err != nil {
		t.Fatalf("read handoff to EOF: %v (stderr: %s)", err, stderr.String())
	}
	var identity map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(line), &identity); err != nil {
		t.Fatalf("handoff line %q: %v", line, err)
	}
	if runID, _ := identity["run_id"].(string); runID == "" || runID == "supervisor" {
		t.Fatalf("handoff identity = %v, want the conversation's own run", identity)
	}

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "bind-drive", Version: "1"}, nil)
	cs, err := client.Connect(ctx, &mcpsdk.IOTransport{Reader: stdout, Writer: stdin}, nil)
	if err != nil {
		t.Fatalf("connect over stdio: %v (stderr: %s)", err, stderr.String())
	}
	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list over stdio: %v (stderr: %s)", err, stderr.String())
	}
	if len(tools.Tools) == 0 {
		t.Fatal("tools/list returned no tools")
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("close session: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("mcp exited with %v; stderr: %s", err, stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the server did not exit after stdin closed")
	}
}
