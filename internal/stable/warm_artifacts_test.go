package stable

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
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

func TestWarmResumeRejectsPendingBrokerMessage(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	b.CreateRun("rt", "")
	if err := b.Send("rt", "agent_message", nil, "msg"); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", onEvent: filepath.Join(root, "old"), sentinelFile: filepath.Join(root, "old-sentinel"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{broker: b}
	_, warm, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: filepath.Join(root, "new"), SentinelFile: filepath.Join(root, "new-sentinel")})
	if !warm || err == nil {
		t.Fatalf("pending message admission: warm=%v err=%v", warm, err)
	}
	if child.sentinelFile != filepath.Join(root, "old-sentinel") || len(child.promptQueue) != 0 {
		t.Fatal("rejected admission changed turn")
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
	sup.testHooks.warmAdmissionAfterCheck = func() {
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
