package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
	"github.com/sdougbrown/avenor/internal/durablefile"
	"github.com/sdougbrown/avenor/internal/supervisor"
)

// conversationScratch points the sockets root at a short scratch directory and
// clears the coordinates a developer's environment may carry, so a test derives
// the one path it expects. The name is short because a unix socket path has to fit
// a fixed buffer well below the filesystem's own limit.
func conversationScratch(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "avconv-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	t.Setenv("AVENOR_HOME", root)
	for _, key := range []string{envControlSocket, envBindHost, envBindSession, "AVENOR_BROKER_URL", "AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN"} {
		t.Setenv(key, "")
	}
	return root
}

// derivedSocketFor is the socket a conversation's feed and commands derive in the
// scratch root.
func derivedSocketFor(t *testing.T, host, session string) string {
	t.Helper()
	socket, err := supervisor.ConversationSocket(host, session)
	if err != nil {
		t.Fatalf("ConversationSocket: %v", err)
	}
	return socket
}

// startConversationSupervisorRecorder replaces the conversation launcher for one
// test and reports every launch the code under test asked for.
type supervisorLaunches struct {
	mu    sync.Mutex
	paths []string
}

func (l *supervisorLaunches) record(socketPath string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.paths = append(l.paths, socketPath)
}

func (l *supervisorLaunches) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.paths)
}

func (l *supervisorLaunches) recorded() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.paths...)
}

// fakeStartConversationSupervisor substitutes the launcher for one test. started
// answers with a lifecycle whose connection records the shutdown it was asked for,
// so a test sees what the code did without a real supervisor process.
func fakeStartConversationSupervisor(t *testing.T, launches *supervisorLaunches, lifecycle *supervisor.Lifecycle) {
	t.Helper()
	orig := startConversationSupervisor
	startConversationSupervisor = func(socketPath string, idleTimeout time.Duration, policy string, opts supervisor.Options) (*supervisor.Lifecycle, error) {
		launches.record(socketPath)
		return lifecycle, nil
	}
	t.Cleanup(func() { startConversationSupervisor = orig })
}

// lifecycleRecorder is the supervisor connection a lifecycle fake holds.
type lifecycleRecorder struct {
	mu        sync.Mutex
	shutdowns []string
	closes    int
}

func (r *lifecycleRecorder) Shutdown(mode string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shutdowns = append(r.shutdowns, mode)
	return nil
}

func (r *lifecycleRecorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closes++
	return nil
}

// TestBoundCommandsDeriveTheConversationSocket pins the default a session needs no
// configuration for: with the bind coordinates alone, spawn and result reach the
// supervisor derived for that conversation, and no command starts one.
func TestBoundCommandsDeriveTheConversationSocket(t *testing.T) {
	conversationScratch(t)
	socket := derivedSocketFor(t, "poo", "ses_derived")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	handler := &boundHandler{bound: liveBinding()}
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	srv.SetStableHandler(handler)
	startOwnerControlServerAt(t, srv, socket)

	launches := &supervisorLaunches{}
	fakeStartConversationSupervisor(t, launches, nil)

	var out, stderr bytes.Buffer
	if code := runDeclared([]string{"spawn", "--bind-host", "poo", "--bind-session", "ses_derived", "--prompt", "work", "--dir", t.TempDir()}, &out, &stderr); code != 0 {
		t.Fatalf("spawn exit = %d, stderr = %s", code, &stderr)
	}
	if code := runDeclared([]string{"result", "--bind-host", "poo", "--bind-session", "ses_derived", "--runtime", "rt_child"}, &out, &stderr); code != 0 {
		t.Fatalf("result exit = %d, stderr = %s", code, &stderr)
	}
	lookups, binds, spawned := handler.calls()
	if lookups != 2 || binds != 0 || spawned != 1 {
		t.Fatalf("lookups/binds/spawns = %d/%d/%d, want two reads, no bind and one spawn", lookups, binds, spawned)
	}
	if launches.count() != 0 {
		t.Fatalf("commands started the conversation supervisor %d times, want none", launches.count())
	}
}

// TestBoundCommandsRefuseWhenTheDerivedSupervisorIsNotRunning pins the startup
// race and the attach-only rule: a conversation whose feed has not started its
// supervisor is refused with the derived path named, and the refusal starts
// nothing.
func TestBoundCommandsRefuseWhenTheDerivedSupervisorIsNotRunning(t *testing.T) {
	conversationScratch(t)
	socket := derivedSocketFor(t, "poo", "ses_idle")
	launches := &supervisorLaunches{}
	fakeStartConversationSupervisor(t, launches, nil)

	for _, args := range [][]string{
		{"spawn", "--bind-host", "poo", "--bind-session", "ses_idle", "--prompt", "work", "--dir", t.TempDir()},
		{"status", "--bind-host", "poo", "--bind-session", "ses_idle", "--runtime", "rt_child"},
		{"result", "--bind-host", "poo", "--bind-session", "ses_idle", "--runtime", "rt_child"},
		{"message", "--bind-host", "poo", "--bind-session", "ses_idle", "--action=tell", "--target=x", "--message=x"},
	} {
		var out, stderr bytes.Buffer
		if code := runDeclared(args, &out, &stderr); code != 1 {
			t.Fatalf("%v exit = %d, stderr = %s, want a refusal", args, code, &stderr)
		}
		if !strings.Contains(stderr.String(), socket) || !strings.Contains(stderr.String(), "its inbox feed starts it") {
			t.Fatalf("%v stderr = %s, want the derived socket and its owner named", args, &stderr)
		}
		if out.Len() != 0 {
			t.Fatalf("%v stdout = %s, want no result", args, &out)
		}
	}
	if launches.count() != 0 {
		t.Fatalf("commands started the conversation supervisor %d times, want none", launches.count())
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("stat %s = %v, want nothing started at the derived path", socket, err)
	}
}

// TestBoundCommandsKeepAnExplicitSocketAttachOnly pins the configured path: an
// explicit socket that is not there is an error, never a launch, and its refusal
// still names the configured supervisor.
func TestBoundCommandsKeepAnExplicitSocketAttachOnly(t *testing.T) {
	conversationScratch(t)
	socket := filepath.Join(t.TempDir(), "configured.sock")
	launches := &supervisorLaunches{}
	fakeStartConversationSupervisor(t, launches, nil)

	var out, stderr bytes.Buffer
	args := []string{"spawn", "--socket", socket, "--bind-host", "poo", "--bind-session", "ses_configured", "--prompt", "work", "--dir", t.TempDir()}
	if code := runDeclared(args, &out, &stderr); code != 1 {
		t.Fatalf("spawn exit = %d, stderr = %s", code, &stderr)
	}
	if !strings.Contains(stderr.String(), "attach the configured supervisor at "+socket) {
		t.Fatalf("stderr = %s, want the configured supervisor named", &stderr)
	}
	if launches.count() != 0 {
		t.Fatalf("configured socket started %d supervisors, want none", launches.count())
	}
}

// TestFeedBrokerStartsAndStopsTheDerivedSupervisor pins the feed's ownership: it
// starts the supervisor a derived socket names and shuts it down when the feed
// stops serving.
func TestFeedBrokerStartsAndStopsTheDerivedSupervisor(t *testing.T) {
	root := conversationScratch(t)
	socket := derivedSocketFor(t, "poo", "ses_owned")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	bound := liveBinding()
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	srv.SetStableHandler(&boundHandler{bound: bound})
	startOwnerControlServerAt(t, srv, socket)

	recorder := &lifecycleRecorder{}
	launches := &supervisorLaunches{}
	fakeStartConversationSupervisor(t, launches, &supervisor.Lifecycle{SocketPath: socket, Client: recorder})

	// A held consumer lock ends the feed after the bind, without a broker to
	// drain.
	lockDir := filepath.Join(root, "locks")
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatal(err)
	}
	key, err := askclaim.BrokerKey(bound.BrokerURL, bound.RunID)
	if err != nil {
		t.Fatal(err)
	}
	unlock, held, err := durablefile.TryLock(filepath.Join(lockDir, key+".lock"))
	if err != nil || held {
		t.Fatalf("hold the consumer lock: held=%v err=%v", held, err)
	}
	defer func() { _ = unlock() }()

	var out, stderr bytes.Buffer
	args := []string{"feed", "broker", "--bind-host", "poo", "--bind-session", "ses_owned", "--lock-dir", lockDir, "--request-timeout", "100ms"}
	if code := runDeclared(args, &out, &stderr); code != 0 {
		t.Fatalf("feed exit = %d, stderr = %s", code, &stderr)
	}
	if got := launches.recorded(); len(got) != 1 || got[0] != socket {
		t.Fatalf("launches = %v, want one at the derived socket %s", got, socket)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.shutdowns) != 1 {
		t.Fatalf("shutdowns = %v, want the feed to stop the supervisor it started", recorder.shutdowns)
	}
}

// TestFeedBrokerLeavesASupervisorItOnlyAttachedTo pins the other half of the
// ownership rule: a feed that attached to a supervisor another process started
// closes its connection and leaves it running.
func TestFeedBrokerLeavesASupervisorItOnlyAttachedTo(t *testing.T) {
	root := conversationScratch(t)
	socket := derivedSocketFor(t, "poo", "ses_shared")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	bound := liveBinding()
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	srv.SetStableHandler(&boundHandler{bound: bound})
	startOwnerControlServerAt(t, srv, socket)

	recorder := &lifecycleRecorder{}
	launches := &supervisorLaunches{}
	fakeStartConversationSupervisor(t, launches, &supervisor.Lifecycle{SocketPath: socket, Client: recorder, Shared: true})

	lockDir := filepath.Join(root, "locks")
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatal(err)
	}
	key, err := askclaim.BrokerKey(bound.BrokerURL, bound.RunID)
	if err != nil {
		t.Fatal(err)
	}
	unlock, held, err := durablefile.TryLock(filepath.Join(lockDir, key+".lock"))
	if err != nil || held {
		t.Fatalf("hold the consumer lock: held=%v err=%v", held, err)
	}
	defer func() { _ = unlock() }()

	var out, stderr bytes.Buffer
	args := []string{"feed", "broker", "--bind-host", "poo", "--bind-session", "ses_shared", "--lock-dir", lockDir, "--request-timeout", "100ms"}
	if code := runDeclared(args, &out, &stderr); code != 0 {
		t.Fatalf("feed exit = %d, stderr = %s", code, &stderr)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.shutdowns) != 0 {
		t.Fatalf("shutdowns = %v, want a shared supervisor left running", recorder.shutdowns)
	}
	if recorder.closes != 1 {
		t.Fatalf("closes = %d, want the feed to release its connection", recorder.closes)
	}
}

// TestFeedBrokerRefusesAnUnservedConversationWithoutLeavingASupervisor pins that a
// bind the supervisor refuses still stops the supervisor the feed started, so a
// mistyped host leaves nothing behind.
func TestFeedBrokerRefusesAnUnservedConversationWithoutLeavingASupervisor(t *testing.T) {
	root := conversationScratch(t)
	socket := derivedSocketFor(t, "claude", "ses_unserved")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	handler := &boundHandler{bound: liveBinding(), bindErr: &bindRefusal{}}
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	srv.SetStableHandler(handler)
	startOwnerControlServerAt(t, srv, socket)

	recorder := &lifecycleRecorder{}
	launches := &supervisorLaunches{}
	fakeStartConversationSupervisor(t, launches, &supervisor.Lifecycle{SocketPath: socket, Client: recorder})

	var out, stderr bytes.Buffer
	args := []string{"feed", "broker", "--bind-host", "claude", "--bind-session", "ses_unserved", "--lock-dir", filepath.Join(root, "locks")}
	if code := runDeclared(args, &out, &stderr); code != 1 {
		t.Fatalf("feed exit = %d, stderr = %s", code, &stderr)
	}
	if launches.count() != 1 {
		t.Fatalf("launches = %d, want the feed to have started the supervisor once", launches.count())
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.shutdowns) != 1 {
		t.Fatalf("shutdowns = %v, want the refused feed to stop the supervisor it started", recorder.shutdowns)
	}
}

// bindRefusal is the supervisor-side refusal a bind of an unserved host answers.
type bindRefusal struct{}

func (*bindRefusal) Error() string { return "message_bind does not serve host \"claude\"" }
