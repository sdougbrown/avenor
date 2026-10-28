package stable

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/warmqueue"
)

func TestWarmResumeRotatesArtifactsAndRejectsConcurrentAdmission(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "old-events")
	oldSentinel := filepath.Join(root, "old-sentinel")
	if err := os.WriteFile(oldSentinel, []byte("DONE\n"), 0600); err != nil {
		t.Fatal(err)
	}
	sink, err := cli.NewEventWriter(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", eventWriter: sink, onEvent: oldPath, sentinelFile: oldSentinel, promptCh: make(chan struct{}, 1), session: runtime.Session{SessionID: "session"}, fullFinalOutput: "old", lastStopReason: "end_turn"}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{}
	writer := &runtimeFanoutWriter{base: sink, child: child, supervisor: sup}
	child.fanoutWriter = writer
	if err := writer.Write(events.Event{Event: "test.old"}); err != nil {
		t.Fatal(err)
	}
	nextPath, nextSentinel := filepath.Join(root, "new-events"), filepath.Join(root, "new-sentinel")
	got, warm, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: nextPath, SentinelFile: nextSentinel, resume: resumeSpawn{runID: "new-run"}})
	if err != nil || !warm {
		t.Fatalf("admit: warm=%v err=%v", warm, err)
	}
	defer child.eventWriter.Close()
	if got.OnEvent != nextPath || got.SentinelFile != nextSentinel || got.RunID != "new-run" || got.SessionID != "session" {
		t.Fatalf("result: %+v", got)
	}
	if child.fullFinalOutput != "" || child.lastStopReason != "" || len(child.promptQueue) != 1 {
		t.Fatalf("stale state: %+v", child)
	}
	if err := writer.Write(events.Event{Event: "test.new"}); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	next, err := os.ReadFile(nextPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(old) == string(next) || !containsEvent(old, "test.old") || containsEvent(old, "test.new") || !containsEvent(next, "test.new") {
		t.Fatalf("old=%s new=%s", old, next)
	}
	if _, _, err := sup.admitWarmResume(child, SpawnParams{Prompt: "again", OnEvent: filepath.Join(root, "third"), SentinelFile: filepath.Join(root, "third-sentinel")}); err == nil {
		t.Fatal("concurrent resume accepted")
	}
	if _, err := os.Stat(nextSentinel); !os.IsNotExist(err) {
		t.Fatalf("new sentinel exists before completion: %v", err)
	}
}

// TestWarmResumeRefusesUnverifiedArtifactRoot pins parity with the cold-spawn
// artifact hardening: a warm resume that needs the default artifact tree is
// refused on the same roots ensureArtifactRoot refuses, and it creates
// nothing under them.
func TestWarmResumeRefusesUnverifiedArtifactRoot(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, root string) string
	}{
		{
			name: "symlinked root",
			prepare: func(t *testing.T, root string) string {
				target := t.TempDir()
				if err := os.Symlink(target, root); err != nil {
					t.Fatal(err)
				}
				return target
			},
		},
		{
			name: "group writable root",
			prepare: func(t *testing.T, root string) string {
				if err := os.Mkdir(root, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(root, 0o770); err != nil {
					t.Fatal(err)
				}
				return root
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := artifactRoot()
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			target := tc.prepare(t, root)
			t.Cleanup(func() { _ = os.RemoveAll(root) })

			child := &childRuntime{id: "rt", promptCh: make(chan struct{}, 1)}
			initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
			sup := &Supervisor{runID: "warmtest"}
			_, warm, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next"})
			if !warm || err == nil || !strings.Contains(err.Error(), root) {
				t.Fatalf("admission: warm=%v err=%v, want a refusal naming %s", warm, err, root)
			}
			entries, err := os.ReadDir(target)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("refused admission created %v under the root", entries)
			}
			if len(child.promptQueue) != 0 || child.onEvent != "" || child.sentinelFile != "" {
				t.Fatal("refused admission changed the child's turn")
			}
		})
	}
}

// warmGuardProvider is a provider whose sessions carry the warm-message
// admission guard, the way channel-backed providers do.
type warmGuardProvider struct {
	*warmqueue.Queue
	*stableScriptedProvider
}

// TestWarmResumeRefusesPendingProviderMessage pins the provider-side warm
// guard: a prompt polled into a parked runtime's provider queue leaves the
// broker's ControlQueue empty, so the guard is what refuses admission. The
// refusal must release the broker admission lock, and a drained queue must
// admit with a callable unlock.
func TestWarmResumeRefusesPendingProviderMessage(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	b.CreateRun("rt", "")
	queue := warmqueue.NewQueue()
	queue.Enqueue("session", "queued prompt")
	provider := &warmGuardProvider{
		Queue: queue,
		stableScriptedProvider: &stableScriptedProvider{attempt: -1, scripts: []stableScriptedAttempt{
			itestAttempt("session", itestSessionEnd("session", "end_turn", "next")),
		}},
	}
	child := &childRuntime{id: "rt", provider: provider, session: runtime.Session{SessionID: "session"}, onEvent: filepath.Join(root, "old"), sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{broker: b}

	_, warm, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: filepath.Join(root, "new"), SentinelFile: filepath.Join(root, "new-sentinel")})
	if !warm || err == nil || !strings.Contains(err.Error(), "pending provider messages") {
		t.Fatalf("admission: warm=%v err=%v, want a provider-message refusal", warm, err)
	}
	// The refusal released the broker admission lock instead of leaking it.
	release, err := sup.broker.LockWarmAdmission("rt", "", true)
	if err != nil {
		t.Fatalf("broker admission lock leaked: %v", err)
	}
	release(false)
	// A drained queue lets the same admission through.
	queue.Reset()
	result, warm, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: filepath.Join(root, "new"), SentinelFile: filepath.Join(root, "new-sentinel")})
	if err != nil || !warm {
		t.Fatalf("admission after reset: warm=%v err=%v", warm, err)
	}
	defer child.eventWriter.Close()
	if result.RuntimeID != "rt" || len(child.promptQueue) != 1 {
		t.Fatalf("admission result = %+v, promptQueue=%d", result, len(child.promptQueue))
	}
	// An empty queue admits and hands back a callable unlock.
	brokerRelease, unlock, err := sup.lockWarmResumeAdmission(child, "")
	if err != nil || brokerRelease == nil || unlock == nil {
		t.Fatal("empty-queue admission failed or returned a nil release or unlock")
	}
	unlock()
	brokerRelease(false)
}

func TestWarmResumeRejectsPendingBrokerMessage(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	b.CreateRun("rt", "")
	if err := b.Send("rt", "agent_message", nil, "msg"); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", onEvent: filepath.Join(root, "old"), sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	child.provider = &warmGuardProvider{Queue: warmqueue.NewQueue()}
	sup := &Supervisor{broker: b}
	_, warm, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: filepath.Join(root, "new"), SentinelFile: filepath.Join(root, "new-sentinel")})
	if !warm || err == nil {
		t.Fatalf("pending message admission: warm=%v err=%v", warm, err)
	}
	if child.sentinelFile != filepath.Join(root, "old-sentinel") || len(child.promptQueue) != 0 {
		t.Fatal("rejected admission changed turn")
	}
}

// TestWarmResumePendingMessageCleansReservedArtifacts pins the reordered
// admission fence: the artifacts are reserved before the broker admission
// lock, the pending check runs under the lock, and a rejection removes
// everything the reservation created — the exclusively created event file
// even on a caller-supplied path, and the turn directory on default paths.
func TestWarmResumePendingMessageCleansReservedArtifacts(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	if _, err := b.CreateRun("rt", ""); err != nil {
		t.Fatal(err)
	}
	if err := b.Send("rt", "agent_message", nil, "msg"); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", onEvent: filepath.Join(root, "old"), sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	child.provider = &warmGuardProvider{Queue: warmqueue.NewQueue()}
	sup := &Supervisor{broker: b}
	newEvents := filepath.Join(root, "new-events")
	_, warm, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: newEvents, SentinelFile: filepath.Join(root, "new-sentinel")})
	if !warm || err == nil || !strings.Contains(err.Error(), "pending broker messages") {
		t.Fatalf("pending-message admission: warm=%v err=%v", warm, err)
	}
	if _, err := os.Stat(newEvents); !os.IsNotExist(err) {
		t.Fatalf("reserved event file survived the rejection: %v", err)
	}
	if child.onEvent != filepath.Join(root, "old") || len(child.promptQueue) != 0 {
		t.Fatal("rejected admission changed the child's turn")
	}

	// Default paths get the same cleanup: the rejected reservation's turn
	// directory is removed and the run directory is left empty.
	if _, err := b.CreateRun("rt2", ""); err != nil {
		t.Fatal(err)
	}
	if err := b.Send("rt2", "agent_message", nil, "msg"); err != nil {
		t.Fatal(err)
	}
	child2 := &childRuntime{id: "rt2", onEvent: filepath.Join(root, "old2"), sentinelFile: filepath.Join(root, "old2-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child2, testLifecycleOptions{exec: execParked})
	child2.provider = &warmGuardProvider{Queue: warmqueue.NewQueue()}
	sup2 := &Supervisor{broker: b, runID: "warm-resume-clean-test"}
	_, warm, err = sup2.admitWarmResume(child2, SpawnParams{Prompt: "next"})
	if !warm || err == nil {
		t.Fatalf("default-path pending-message admission: warm=%v err=%v", warm, err)
	}
	runDir, err := createArtifactRunDir(sup2.runID, "rt2")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(runDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected reservation left %v under %s", entries, runDir)
	}

	// A message that arrives while admission is between the reservation and
	// the lock is caught by the authoritative check under the lock, and the
	// reserved event file is still removed.
	if _, err := b.CreateRun("rt3", ""); err != nil {
		t.Fatal(err)
	}
	child3 := &childRuntime{id: "rt3", onEvent: filepath.Join(root, "old3"), sentinelFile: filepath.Join(root, "old3-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child3, testLifecycleOptions{exec: execParked})
	child3.provider = &warmGuardProvider{Queue: warmqueue.NewQueue()}
	sup3 := &Supervisor{broker: b}
	sup3.testHooks.warmAdmissionAfterReservation = func() {
		if err := b.Send("rt3", "agent_message", nil, "late"); err != nil {
			t.Fatal(err)
		}
	}
	lateEvents := filepath.Join(root, "late-events")
	_, warm, err = sup3.admitWarmResume(child3, SpawnParams{Prompt: "next", OnEvent: lateEvents, SentinelFile: filepath.Join(root, "late-sentinel")})
	if !warm || err == nil || !strings.Contains(err.Error(), "pending broker messages") {
		t.Fatalf("late-message admission: warm=%v err=%v", warm, err)
	}
	if _, err := os.Stat(lateEvents); !os.IsNotExist(err) {
		t.Fatalf("reserved event file survived the late-message rejection: %v", err)
	}
}

func TestWarmResumeRejectsMessageArrivingDuringAdmission(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	b.CreateRun("rt", "")
	if err := b.RenameRun("rt", "old-label"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "user-events")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", displayName: "old-label", onEvent: filepath.Join(root, "old"), sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1), fullFinalOutput: "previous"}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{broker: b}
	sup.testHooks.warmAdmissionAfterReservation = func() {
		if err := b.Send("rt", "agent_message", nil, "msg"); err != nil {
			t.Fatal(err)
		}
	}
	_, warm, err := sup.admitWarmResume(child, SpawnParams{Label: "new-label", Prompt: "next", OnEvent: path, SentinelFile: filepath.Join(root, "new-sentinel")})
	if !warm || err == nil {
		t.Fatalf("admission: warm=%v err=%v", warm, err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep" {
		t.Fatalf("user event file changed: %q, %v", data, err)
	}
	if b.DisplayName("rt") != "old-label" || child.displayName != "old-label" || child.fullFinalOutput != "previous" || child.onEvent != filepath.Join(root, "old") || len(child.promptQueue) != 0 {
		t.Fatal("rejected admission changed label or turn state")
	}
	if _, err := os.Stat(filepath.Join(root, "new-sentinel")); !os.IsNotExist(err) {
		t.Fatalf("sentinel created: %v", err)
	}
}

func TestWarmResumeWaitsForTurnBoundSend(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	b.CreateRun("rt", "")
	child := &childRuntime{id: "rt", onEvent: filepath.Join(root, "old"), sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	child.provider = &warmGuardProvider{Queue: warmqueue.NewQueue()}
	sup := &Supervisor{broker: b, runtimes: map[string]*childRuntime{"rt": child}}
	entered, release := make(chan struct{}), make(chan struct{})
	sent := make(chan error, 1)
	go func() {
		child.turnMu.Lock()
		close(entered)
		<-release
		err := b.Send("rt", "agent_message", nil, "msg")
		child.turnMu.Unlock()
		sent <- err
	}()
	<-entered
	admitted := make(chan error, 1)
	go func() {
		_, _, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: filepath.Join(root, "new"), SentinelFile: filepath.Join(root, "new-sentinel")})
		admitted <- err
	}()
	close(release)
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("send deadlocked")
	}
	select {
	case err := <-admitted:
		if err == nil {
			t.Fatal("admitted with pending message")
		}
	case <-time.After(time.Second):
		t.Fatal("admission deadlocked")
	}
}

func TestWarmResumeRejectsClaimedDirectPrompt(t *testing.T) {
	root := t.TempDir()
	child := &childRuntime{id: "rt", promptQueue: []string{"direct"}, promptCh: make(chan struct{}, 1), onEvent: filepath.Join(root, "old")}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	if prompt, ok := child.dequeuePrompt(); !ok || prompt != "direct" {
		t.Fatalf("dequeue = %q, %v", prompt, ok)
	}
	_, warm, err := (&Supervisor{}).admitWarmResume(child, SpawnParams{Prompt: "resume", OnEvent: filepath.Join(root, "new"), SentinelFile: filepath.Join(root, "sentinel")})
	if !warm || err == nil {
		t.Fatalf("claimed prompt admission: warm=%v err=%v", warm, err)
	}
	if child.onEvent != filepath.Join(root, "old") {
		t.Fatal("claimed prompt artifacts changed")
	}
}

func TestWarmResumeRejectsDerivedHandlerWithoutChildMutation(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "old-events")
	oldSentinel := filepath.Join(root, "old-sentinel")
	sink, err := cli.NewEventWriter(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	handler := permission.NewFileHandler("old-base")
	child := &childRuntime{
		id:                 "rt",
		eventWriter:        sink,
		onEvent:            oldPath,
		sentinelFile:       oldSentinel,
		derivedFileHandler: true,
		fileHandler:        handler,
		promptCh:           make(chan struct{}, 1),
		session:            runtime.Session{SessionID: "session"},
	}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{}
	sup.testHooks.warmResumePermissionHandler = func(string) (*permission.FileHandler, error) {
		return nil, errors.New("forced parse failure")
	}
	newPath, newSentinel := filepath.Join(root, "new-events"), filepath.Join(root, "new-sentinel")
	_, warm, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: newPath, SentinelFile: newSentinel})
	if !warm || err == nil || !strings.Contains(err.Error(), "permission handler") {
		t.Fatalf("admission: warm=%v err=%v", warm, err)
	}
	if child.eventWriter != sink || child.onEvent != oldPath || child.sentinelFile != oldSentinel || child.fileHandler != handler || len(child.promptQueue) != 0 {
		t.Fatal("rejected admission mutated the child")
	}
	if n := openFDsForPath(t, newPath); n != 0 {
		t.Fatalf("reserved event file left open: %d fds on %s", n, newPath)
	}
	if n := openFDsForPath(t, oldPath); n == 0 {
		t.Fatal("child's previous writer was closed by the failed admission")
	}
}

// openFDsForPath counts this process's open file descriptors pointing at path.
// It skips the count on platforms without a proc filesystem.
func openFDsForPath(t *testing.T, path string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot read /proc/self/fd: %v", err)
	}
	count := 0
	for _, entry := range entries {
		target, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err != nil {
			continue
		}
		if target == path {
			count++
		}
	}
	return count
}

func containsEvent(data []byte, name string) bool { return bytes.Contains(data, []byte(name)) }

// TestSameArtifactResolvesSymlinkedParent pins the artifact comparison: two
// spellings that reach one not-yet-existing file through a symlinked parent
// directory are the same artifact, while genuinely distinct paths and an
// identical spelling keep their expected verdicts.
func TestSameArtifactResolvesSymlinkedParent(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	ev := filepath.Join(real, "ev")
	if !sameArtifact(ev, ev) {
		t.Fatal("an identical path was not recognized as the same artifact")
	}
	if sameArtifact(ev, filepath.Join(real, "other")) {
		t.Fatal("two distinct paths in one directory were reported as the same artifact")
	}
	if !sameArtifact(filepath.Join(link, "ev"), ev) {
		t.Fatal("a path through a symlinked parent was not recognized as the same not-yet-existing artifact")
	}
}

// TestWarmResumeRejectsAliasedArtifactPath pins the admission-level effect:
// caller-supplied paths that alias the child's own not-yet-existing event path
// through a symlinked parent directory are refused instead of reserved, so
// admission cannot install a writer over the child's current artifact.
func TestWarmResumeRejectsAliasedArtifactPath(t *testing.T) {
	root := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", onEvent: filepath.Join(root, "ev"), sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	_, _, err := (&Supervisor{}).admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: filepath.Join(link, "ev"), SentinelFile: filepath.Join(root, "new-sentinel")})
	if err == nil || !strings.Contains(err.Error(), "must be distinct") {
		t.Fatalf("admission over an aliased event path = %v, want a distinct-path rejection", err)
	}
	if child.onEvent != filepath.Join(root, "ev") || len(child.promptQueue) != 0 {
		t.Fatal("rejected admission changed the child's turn")
	}
}

func TestUnstartedWarmResumeWritesFailureSentinel(t *testing.T) {
	sup := newTestSupervisor(t, Config{})
	path := filepath.Join(t.TempDir(), "sentinel")
	child := &childRuntime{id: "rt", sentinelFile: path, warmResumePending: true, session: runtime.Session{SessionID: "session"}}
	sup.finishUnstartedWarmResume(child)
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(data, []byte("FAILED\n")) || !bytes.Contains(data, []byte("SESSION=session\n")) {
		t.Fatalf("failure sentinel %q: %v", data, err)
	}
	if child.lastStopReason != "error" || child.exitCode != 1 {
		t.Fatalf("failure state: reason=%q exit=%d", child.lastStopReason, child.exitCode)
	}
}

// TestWarmResumeRejectsExistingSentinelPath pins the reserve-time sentinel
// check: when the caller-supplied sentinel path already exists, admission is
// refused before any artifact is created, so the existing sentinel's content
// survives and no reserved event file is left behind.
func TestWarmResumeRejectsExistingSentinelPath(t *testing.T) {
	root := t.TempDir()
	sentinel := filepath.Join(root, "sentinel")
	if err := os.WriteFile(sentinel, []byte("DONE\n"), 0600); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", onEvent: filepath.Join(root, "old"), sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	newEvents := filepath.Join(root, "new-events")
	_, _, err := (&Supervisor{}).admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: newEvents, SentinelFile: sentinel})
	if err == nil || !strings.Contains(err.Error(), "sentinel path already exists") {
		t.Fatalf("admission over an existing sentinel = %v, want a rejection", err)
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "DONE\n" {
		t.Fatalf("existing sentinel changed: %q %v", data, err)
	}
	if _, err := os.Stat(newEvents); !os.IsNotExist(err) {
		t.Fatalf("reserved event file survived the rejection: %v", err)
	}
	if child.onEvent != filepath.Join(root, "old") || len(child.promptQueue) != 0 {
		t.Fatal("rejected admission changed the child's turn")
	}
}

// TestUnstartedWarmResumeKeepsCompletionSentinel pins the settle guard: when a
// real completion sentinel already exists at the pending turn's path — for
// instance written by shutdown — finishUnstartedWarmResume leaves the file and
// the child's terminal state alone instead of clobbering it with a failure.
func TestUnstartedWarmResumeKeepsCompletionSentinel(t *testing.T) {
	sup := newTestSupervisor(t, Config{})
	path := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(path, []byte("SUCCEEDED\n"), 0600); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", sentinelFile: path, warmResumePending: true, session: runtime.Session{SessionID: "session"}}
	sup.finishUnstartedWarmResume(child)
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(data, []byte("SUCCEEDED\n")) {
		t.Fatalf("completion sentinel overwritten: %q %v", data, err)
	}
	if child.exitCode != 0 || child.lastStopReason != "" {
		t.Fatalf("failure state applied: exit=%d reason=%q", child.exitCode, child.lastStopReason)
	}
}

// TestShutdownWritesAdmittedWarmResumeSentinel drives the real termination
// call site (supervisor.go runChild's teardown defer) with an admitted warm
// resume whose turn is held open: shutting the supervisor down must leave the
// new turn's sentinel written at its reserved path — the terminal idle-end
// effect writes it, and finishUnstartedWarmResume, which runs after it in the
// same teardown, leaves an existing sentinel alone — so a host-side sentinel
// wait cannot hang until timeout.
func TestShutdownWritesAdmittedWarmResumeSentinel(t *testing.T) {
	sup := newTestSupervisor(t, Config{
		ControlSocket:        newStableSocketPath(t, "warm-shutdown-sentinel"),
		MaxRuntimes:          2,
		ParkedRuntimeTimeout: 0,
		ShutdownTimeout:      0,
	})
	t.Cleanup(func() {
		_ = sup.Shutdown("kill")
		_ = sup.broker.Stop()
	})
	release := make(chan struct{})
	var closeOnce sync.Once
	safeClose := func() { closeOnce.Do(func() { close(release) }) }
	t.Cleanup(safeClose)
	provider := &stableScriptedProvider{attempt: -1, emitted: make(chan int, 4), scripts: []stableScriptedAttempt{
		itestAttempt("ses_warm_shutdown", itestSessionEnd("ses_warm_shutdown", "end_turn", "done")),
		itestAttempt("ses_warm_shutdown", stableScriptedEvent{release: release}),
	}}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		return provider, nil
	}
	dir := t.TempDir()

	first, err := sup.spawn(SpawnParams{Prompt: "start", Dir: dir})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	waitForActiveRuntimeCount(t, sup, 0) // the first turn parks
	for len(provider.emitted) > 0 {
		<-provider.emitted
	}

	nextSentinel := filepath.Join(dir, "next-sentinel")
	resumed, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: first.RuntimeID, Message: "continue",
		SentinelFile: nextSentinel, EventLog: filepath.Join(dir, "next-events"),
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	// The resumed turn is in flight: the provider opened its second attempt.
	select {
	case <-provider.emitted:
	case <-time.After(5 * time.Second):
		t.Fatal("resumed turn never started")
	}

	// Shut down while the turn is held open; the termination path must write
	// the new turn's sentinel at its reserved path.
	if err := sup.Shutdown("kill"); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	child := sup.runtimeByID(resumed.RuntimeID)
	select {
	case <-child.runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not finish after shutdown")
	}
	data, err := os.ReadFile(nextSentinel)
	if err != nil || !bytes.Contains(data, []byte("SESSION=")) {
		t.Fatalf("new turn sentinel = %q: %v", data, err)
	}
}

func TestWarmResumeRejectsOldArtifactsWithoutTruncation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "events")
	sink, err := cli.NewEventWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if err := sink.Write(events.Event{Event: "keep"}); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", eventWriter: sink, onEvent: path, sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	_, _, err = (&Supervisor{}).admitWarmResume(child, SpawnParams{OnEvent: path, SentinelFile: filepath.Join(root, "new-sentinel")})
	if err == nil {
		t.Fatal("accepted old event path")
	}
	data, err := os.ReadFile(path)
	if err != nil || !containsEvent(data, "keep") {
		t.Fatalf("old events %s: %v", data, err)
	}
	if len(child.promptQueue) != 0 {
		t.Fatal("failed admission queued prompt")
	}
}
