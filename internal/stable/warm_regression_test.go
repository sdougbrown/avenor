package stable

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

func TestWarmAdmissionRejectsShutdownAndExistingArtifacts(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		root := t.TempDir()
		path := filepath.Join(root, "historical")
		if err := os.WriteFile(path, []byte("preserve"), 0600); err != nil {
			t.Fatal(err)
		}
		child := &childRuntime{id: "rt", onEvent: filepath.Join(root, "current"), promptCh: make(chan struct{}, 1)}
		initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
		sup := &Supervisor{shuttingDown: shutdown}
		_, _, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: path, SentinelFile: filepath.Join(root, "sentinel")})
		if err == nil {
			t.Fatal("admission succeeded")
		}
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "preserve" {
			t.Fatalf("artifact changed: %q %v", data, err)
		}
	}
}

func TestWarmAdmissionDrainsAnswersAndRotatesDerivedHandler(t *testing.T) {
	root := t.TempDir()
	done := make(chan struct{})
	child := &childRuntime{id: "rt", directAnswers: 1, directAnswersDone: done, derivedFileHandler: true, fileHandler: permission.NewFileHandler("old"), promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{}
	sentinel := filepath.Join(root, "sentinel")
	result := make(chan error, 1)
	go func() {
		_, _, err := sup.admitWarmResume(child, SpawnParams{Prompt: "new", OnEvent: filepath.Join(root, "events"), SentinelFile: sentinel})
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("admitted before answer drained: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	child.writeMu.Lock()
	child.directAnswers = 0
	close(done)
	child.writeMu.Unlock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("admission did not resume")
	}
	defer child.eventWriter.Close()
	if child.fileHandler.BasePath != cli.DerivePermBase(sentinel) {
		t.Fatalf("handler path = %q", child.fileHandler.BasePath)
	}
}

func TestWarmAdmissionClearsBrokerLastAndUpdatesTask(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	b.CreateRun("rt", "")
	if err := b.SetLastAgentMessage("rt", "old", "done", false); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", task: "old", promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{broker: b}
	_, _, err := sup.admitWarmResume(child, SpawnParams{Prompt: "new", OnEvent: filepath.Join(root, "events"), SentinelFile: filepath.Join(root, "sentinel")})
	if err != nil {
		t.Fatal(err)
	}
	defer child.eventWriter.Close()
	if _, ok := b.LastAgentMessage("rt"); ok {
		t.Fatal("old broker message remains")
	}
	if child.task != "new" || child.taskFile != "" {
		t.Fatalf("task remains %q %q", child.task, child.taskFile)
	}
}
