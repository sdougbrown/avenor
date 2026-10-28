package stable

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/pi"
)

func TestPiWarmResumePreservesUnreadInbox(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	if _, err := b.CreateRun("rt", "worker"); err != nil {
		t.Fatal(err)
	}
	if err := b.Send("rt", "agent_message", json.RawMessage(`{"message":"unread"}`), "message-1"); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{
		id: "rt", label: "worker", displayName: "worker", backend: "pi",
		provider: pi.NewWithOptions(runtime.StartOptions{}),
		session:  runtime.Session{SessionID: "session"}, promptCh: make(chan struct{}, 1),
	}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{broker: b, runtimes: map[string]*childRuntime{"rt": child}}
	resumed, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner}, messaging.ResumeRequest{
		Target: "rt", RunID: resumeUUID1, Message: "continue",
		EventLog: filepath.Join(root, "events"), SentinelFile: filepath.Join(root, "sentinel"),
	})
	if err != nil {
		t.Fatalf("resume with unread Pi inbox: %v", err)
	}
	defer child.eventWriter.Close()
	if resumed.RuntimeID != "rt" || resumed.SessionID != "session" || len(child.promptQueue) != 1 || child.promptQueue[0] != "continue" {
		t.Fatalf("resume did not continue the parked session: %+v, prompts=%v", resumed, child.promptQueue)
	}
	messages, err := b.DrainAgentMessages("rt", 0)
	if err != nil || len(messages) != 1 || messages[0].ID != "message-1" {
		t.Fatalf("unread inbox changed: %+v, %v", messages, err)
	}
}

func TestWarmResumeRejectsDerivedPermissionArtifactCollisions(t *testing.T) {
	for _, suffix := range []string{".req", ".req.response"} {
		t.Run(suffix, func(t *testing.T) {
			root := t.TempDir()
			sentinel := filepath.Join(root, "sentinel")
			eventPath := cli.DerivePermBase(sentinel) + suffix
			child := &childRuntime{id: "rt", derivedFileHandler: true, promptCh: make(chan struct{}, 1)}
			initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
			_, _, err := (&Supervisor{}).admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: eventPath, SentinelFile: sentinel})
			if err == nil {
				_ = child.eventWriter.Close()
				t.Fatal("admitted an event log at a permission protocol path")
			}
			if _, err := os.Stat(eventPath); !os.IsNotExist(err) {
				t.Fatalf("rejected reservation left an event file: %v", err)
			}
			if child.onEvent != "" || len(child.promptQueue) != 0 {
				t.Fatal("rejected reservation changed the turn")
			}
		})
	}
}

func TestWarmResumeRejectsEmptyMessage(t *testing.T) {
	root := t.TempDir()
	b := broker.New("")
	if _, err := b.CreateRun("rt", "worker"); err != nil {
		t.Fatal(err)
	}
	child := &childRuntime{id: "rt", promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{broker: b, runtimes: map[string]*childRuntime{"rt": child}}
	eventPath := filepath.Join(root, "events")
	_, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner}, messaging.ResumeRequest{
		Target: "rt", RunID: resumeUUID1, EventLog: eventPath, SentinelFile: filepath.Join(root, "sentinel"),
	})
	if err == nil {
		_ = child.eventWriter.Close()
		t.Fatal("admitted a resume with no message")
	}
	if len(child.promptQueue) != 0 {
		t.Fatal("empty message changed the turn")
	}
	if _, err := os.Stat(eventPath); !os.IsNotExist(err) {
		t.Fatalf("empty message reserved artifacts: %v", err)
	}
}

func TestWarmReservationAllowsControlAndRechecksAdmission(t *testing.T) {
	for _, change := range []string{"label", "shutdown"} {
		t.Run(change, func(t *testing.T) {
			root := t.TempDir()
			child := &childRuntime{id: "rt", onEvent: filepath.Join(root, "old-events"), derivedFileHandler: true, promptCh: make(chan struct{}, 1)}
			other := &childRuntime{id: "other", displayName: "other"}
			initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
			initializeTestLifecycle(t, other, testLifecycleOptions{exec: execParked})
			b := broker.New("")
			if _, err := b.CreateRun("other", "other"); err != nil {
				t.Fatal(err)
			}
			sup := &Supervisor{broker: b, runtimes: map[string]*childRuntime{"rt": child, "other": other}}
			entered, release := make(chan struct{}), make(chan struct{})
			sup.testHooks.warmResumePermissionHandler = func(string) (*permission.FileHandler, error) {
				close(entered)
				<-release
				return nil, nil
			}
			result := make(chan error, 1)
			path := filepath.Join(root, "new-events")
			go func() {
				_, _, err := sup.admitWarmResume(child, SpawnParams{Label: "next-label", Prompt: "next", OnEvent: path, SentinelFile: filepath.Join(root, "sentinel")})
				result <- err
			}()
			<-entered
			control := make(chan error, 1)
			go func() {
				status, err := sup.RuntimeStatus("rt")
				if err == nil && status.(map[string]any)["on_event"] != filepath.Join(root, "old-events") {
					err = fmt.Errorf("uncommitted artifacts became visible: %v", status)
				}
				if err == nil {
					sup.List()
					if change == "label" {
						_, err = sup.applySessionName("other", "next-label")
					} else {
						sup.controlMu.Lock()
						sup.shuttingDown = true
						sup.controlMu.Unlock()
					}
				}
				control <- err
			}()
			select {
			case err := <-control:
				close(release)
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				close(release)
				<-control
				<-result
				if child.eventWriter != nil {
					_ = child.eventWriter.Close()
				}
				t.Fatal("control operation blocked on artifact reservation")
			}
			select {
			case err := <-result:
				want := "label already in use"
				if change == "shutdown" {
					want = "shutting down"
				}
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("admission after %s = %v, want %s refusal", change, err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("admission did not finish")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("rejected reservation left event file: %v", err)
			}
			if child.onEvent != filepath.Join(root, "old-events") || len(child.promptQueue) != 0 {
				t.Fatal("rejected reservation changed the turn")
			}
		})
	}
}

type slowCloseSink struct {
	entered chan struct{}
	release chan struct{}
}

func (*slowCloseSink) Write(events.Event) error { return nil }
func (s *slowCloseSink) Close() error {
	close(s.entered)
	<-s.release
	return nil
}

func TestWarmWriterCloseAllowsControl(t *testing.T) {
	root := t.TempDir()
	sink := &slowCloseSink{entered: make(chan struct{}), release: make(chan struct{})}
	child := &childRuntime{id: "rt", eventWriter: sink, promptCh: make(chan struct{}, 1)}
	initializeTestLifecycle(t, child, testLifecycleOptions{exec: execParked})
	sup := &Supervisor{runtimes: map[string]*childRuntime{"rt": child}}
	result := make(chan error, 1)
	go func() {
		_, _, err := sup.admitWarmResume(child, SpawnParams{Prompt: "next", OnEvent: filepath.Join(root, "events"), SentinelFile: filepath.Join(root, "sentinel")})
		result <- err
	}()
	<-sink.entered
	control := make(chan error, 1)
	go func() {
		_, err := sup.RuntimeStatus("rt")
		control <- err
	}()
	select {
	case err := <-control:
		close(sink.release)
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(sink.release)
		<-control
		<-result
		_ = child.eventWriter.Close()
		t.Fatal("control operation blocked on previous writer close")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	_ = child.eventWriter.Close()
}
