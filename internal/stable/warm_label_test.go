package stable

import (
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

func TestWarmResumeImplicitLabelWithHarnessName(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(
		itestAttempt("session-one", itestSessionEnd("session-one", "end_turn", "first")),
		itestAttempt("session-one", gate.step(), itestSessionEnd("session-one", "end_turn", "next")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))
	id := h.spawn(map[string]any{"label": "initial"})
	h.awaitStatus(id, "done")
	h.awaitCondition("runtime parked", func() bool {
		child := h.sup.runtimeByID(id)
		child.mu.Lock()
		defer child.mu.Unlock()
		return child.lc.isParked()
	})
	if err := h.sup.broker.SetHarnessName(id, "harness-name"); err != nil {
		t.Fatal(err)
	}
	result, err := h.sup.Resume(messaging.Caller{Kind: messaging.CallerOwner}, messaging.ResumeRequest{Target: id, Message: "next"})
	if err != nil {
		t.Fatal(err)
	}
	h.sup.controlMu.Lock()
	holder := h.sup.labelHolderLocked(result.Label)
	h.sup.controlMu.Unlock()
	if result.Label != "harness-name" || result.RuntimeID != id || holder != id {
		t.Fatalf("result = %+v", result)
	}
}

func TestWarmResumeConcurrentLabelReservation(t *testing.T) {
	t.Parallel()
	gate := newItestGate()
	defer gate.open()
	provider := newItestProvider(
		itestAttempt("session-one", itestSessionEnd("session-one", "end_turn", "first")),
		itestAttempt("session-two", itestSessionEnd("session-two", "end_turn", "first")),
		itestAttempt("session-next", gate.step(), itestSessionEnd("session-next", "end_turn", "next")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))
	first := h.spawn(map[string]any{"label": "first-label"})
	h.awaitStatus(first, "done")
	second := h.spawn(map[string]any{"label": "second-label"})
	h.awaitStatus(second, "done")
	for _, id := range []string{first, second} {
		h.awaitCondition("runtime parked", func() bool {
			child := h.sup.runtimeByID(id)
			child.mu.Lock()
			defer child.mu.Unlock()
			return child.lc.isParked()
		})
	}
	var results [2]messaging.ResumeResult
	var errs [2]error
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, id := range []string{first, second} {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			<-start
			results[i], errs[i] = h.sup.Resume(messaging.Caller{Kind: messaging.CallerOwner}, messaging.ResumeRequest{Target: id, Message: "next", Label: "shared-label"})
		}(i, id)
	}
	close(start)
	wg.Wait()
	winner := -1
	for i, err := range errs {
		if err == nil {
			if winner != -1 {
				t.Fatal("both resumes accepted the same label")
			}
			winner = i
		} else if !strings.Contains(err.Error(), "label already in use") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if winner == -1 {
		t.Fatalf("neither resume succeeded: %v", errs)
	}
	result := results[winner]
	if result.Label != "shared-label" || h.sup.broker.DisplayName(result.RuntimeID) != result.Label {
		t.Fatalf("resume label %q, broker label %q", result.Label, h.sup.broker.DisplayName(result.RuntimeID))
	}
	h.sup.controlMu.Lock()
	holder := h.sup.labelHolderLocked(result.Label)
	h.sup.controlMu.Unlock()
	if holder != result.RuntimeID {
		t.Fatalf("label holder = %q, want %q", holder, result.RuntimeID)
	}
}
