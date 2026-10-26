package parentlink

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// awaitRecordGone polls until a parent no longer routes runID and holds no
// record for it.
func awaitRecordGone(t *testing.T, parent *branch, runID string) {
	t.Helper()
	deadline := time.Now().Add(awaitDeadline)
	for parent.link.Registry().Owns(runID) || parent.broker.GetRun(runID) != nil {
		if time.Now().After(deadline) {
			t.Fatalf("run %q was never retired at the parent", runID)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestDescendantOnlyChangesDoNotPublishTheRunSet pins the publication boundary:
// adopting, re-registering and retiring a child's runs leaves this broker's own
// run set untouched, so none of it signals the run observer that publishes
// upward. A change to a run the broker owns itself still publishes.
func TestDescendantOnlyChangesDoNotPublishTheRunSet(t *testing.T) {
	parent := startBranch(t, "")
	var publications atomic.Int64
	parent.broker.SetRunObserver(func() { publications.Add(1) })

	child := startBranch(t, parent.url(), "run-x")
	awaitRegistered(t, parent, "run-x")
	if parent.broker.GetRun("run-x") == nil {
		t.Fatal("the parent does not hold the adopted record")
	}
	if got := publications.Load(); got != 0 {
		t.Fatalf("the child's registration published %d times, want none", got)
	}

	// A re-registration that changes nothing is a descendant-only event too.
	if err := child.link.up.Sync(testContext(t)); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if got := publications.Load(); got != 0 {
		t.Fatalf("the re-registration published %d times, want none", got)
	}

	// The child retiring its own run retires the adopted record here, quietly.
	child.broker.DeleteRun("run-x")
	awaitRecordGone(t, parent, "run-x")
	if got := publications.Load(); got != 0 {
		t.Fatalf("the retirement published %d times, want none", got)
	}

	// A run this broker owns itself still publishes: its creation, its rename
	// and its deletion are each one publication.
	rootToken, err := parent.broker.CreateRun("root-run", "root-run")
	if err != nil {
		t.Fatalf("create the parent's own run: %v", err)
	}
	if got := publications.Load(); got != 1 {
		t.Fatalf("the run creation published %d times, want one", got)
	}
	status, body := postJSON(t, parent.broker.Addr(), "/rename", "root-run", rootToken, map[string]any{
		"name": "renamed-root",
	})
	if status != http.StatusOK {
		t.Fatalf("rename = (%d, %v), want 200", status, body)
	}
	if got := publications.Load(); got != 2 {
		t.Fatalf("the rename published %d times, want one more", got)
	}
	parent.broker.DeleteRun("root-run")
	if got := publications.Load(); got != 3 {
		t.Fatalf("the deletion published %d times, want one more", got)
	}
}

// TestRelayRefusalRetiresTheAdoptedRecordQuietly pins the retirement window a
// relay refusal opens: after the registry stopped routing the refused run but
// before the adopted record is retired, the record must not publish as one of
// this broker's own runs, and the retirement itself must not signal the run
// observer.
func TestRelayRefusalRetiresTheAdoptedRecordQuietly(t *testing.T) {
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("start parent broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	var (
		publications atomic.Int64
		paused       = make(chan struct{})
		release      = make(chan struct{})
		armed        atomic.Bool
	)
	link, err := Install(Options{
		Generation: b.Store().Generation,
		RetireRuns: func(runIDs []string) {
			if armed.Load() {
				close(paused)
				<-release
			}
			for _, runID := range runIDs {
				b.DeleteRun(runID)
			}
		},
	}, b)
	if err != nil {
		t.Fatalf("build parent messenger: %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })
	parent := &branch{broker: b, link: link, tokens: map[string]string{}}

	rootToken, err := b.CreateRun("root-run", "root-run")
	if err != nil {
		t.Fatalf("create the parent's own run: %v", err)
	}
	parent.tokens["root-run"] = rootToken
	// The spy installs after the broker's own run exists, so the counts below
	// hold exactly the publications the events under test cause.
	b.SetRunObserver(func() { publications.Add(1) })

	child := startBranch(t, parent.url(), "run-x")
	awaitRegistered(t, parent, "run-x")

	// The child stops publishing, so its run disappears without the parent
	// learning a new claim: the parent keeps routing a run the child no longer
	// holds, which is what the next ask surfaces.
	child.broker.SetRunObserver(func() {})
	child.broker.DeleteRun("run-x")

	// The relay reaches the child, which refuses the credential: the registry
	// stops routing the run, and the pausing RetireRuns holds the caller before
	// the adopted record is retired.
	armed.Store(true)
	askDone := make(chan struct{})
	var askStatus int
	var askBody map[string]any
	go func() {
		askStatus, askBody = ask(t, parent, "root-run", "run-x", newMsgID(t))
		close(askDone)
	}()
	<-paused

	if parent.link.Registry().Owns("run-x") {
		t.Fatal("the relay left the refused run registered")
	}
	claims := parent.broker.ParentRunClaims()
	if len(claims) != 1 || claims[0].RunID != "root-run" || claims[0].Token != rootToken {
		t.Fatalf("claims in the refusal window = %+v, want only root-run with its own credential", claims)
	}

	close(release)
	<-askDone
	if askStatus != http.StatusNotFound {
		t.Fatalf("ask = (%d, %v), want 404 target-unavailable", askStatus, askBody)
	}
	if got := publications.Load(); got != 0 {
		t.Fatalf("the relay's retirement published %d times, want none", got)
	}
	if b.GetRun("run-x") != nil {
		t.Fatal("the adopted record was not retired")
	}
}
