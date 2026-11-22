package stable

// dispatch_cancel_window_test.go pins the interleavings of workflow.cancel and
// one controller dispatch's attempt start. The cancel surface commits the
// instance cancel durably and then snapshots the runtimes it owns
// (cancelWorkflowRuntimes); a dispatch commits the attempt intent
// (BeginDispatch) and later registers the attempt's child runtime. Two
// orderings matter, and both are driven here deterministically through the
// supervisor's test hooks:
//
//  1. before registration — the reproduced race. The attempt intent is durable
//     and no child runtime is registered yet, so the cancel finds nothing to
//     stop and the dispatch used to start the provider for an already-canceled
//     workflow. TestDispatchStartsNoProviderWhenCancelCommitsAfterBeginDispatch
//     lands the cancel in that window; TestDispatchStartsNoProviderWhenCancel-
//     CommitsBeforeAttemptRegistration lands it after every dispatch-side step,
//     which is the window a status recheck placed on the dispatch path alone
//     (a TOCTOU) cannot close: only a check inside the registration critical
//     section, which the cancel's snapshot serializes against, sees it.
//  2. after registration — cancel finds the attempt's runtime and stops it.
//     TestCancelAfterRegisteredRuntimeStopsTheAttempt pins that ordering.
//  3. unknown ownership — an attempt whose instance status cannot be read must
//     hold, not start. TestAttemptStartHoldsWhenInstanceStatusIsUnreadable pins
//     that the registration check is fail-closed rather than fail-open.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/workflow"
)

// cancelWindowProvider is the local deterministic inference provider for these
// tests. It counts the two observable steps of provider execution (session
// start and first prompt) and defers the turn to the packaged
// blockingAdmissionProvider, which blocks until the fixture releases it, so a
// started attempt stays live instead of running to completion.
type cancelWindowProvider struct {
	*blockingAdmissionProvider
	starts  atomic.Int32
	prompts atomic.Int32
}

func (p *cancelWindowProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.starts.Add(1)
	return p.blockingAdmissionProvider.Start(ctx, opts)
}

func (p *cancelWindowProvider) Prompt(ctx context.Context, sessionID, prompt string) error {
	p.prompts.Add(1)
	return p.blockingAdmissionProvider.Prompt(ctx, sessionID, prompt)
}

// cancelWorkflowInstance cancels through the supervisor's production control
// surface and returns the result map.
func cancelWorkflowInstance(t *testing.T, sup *Supervisor, workflowID, reason string) (map[string]any, error) {
	t.Helper()
	res, err := (lazyWorkflowHandler{sup}).WorkflowCancel(workflowID, mustJSON(t, map[string]string{
		"actor":  "ops",
		"reason": reason,
	}))
	if err != nil {
		return nil, err
	}
	result, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("cancel result = %T, want map[string]any", res)
	}
	return result, nil
}

// workflowInstance reads the durable instance snapshot.
func workflowInstance(t *testing.T, mgr *workflow.Manager, workflowID string) workflow.WorkflowInstance {
	t.Helper()
	insp, err := mgr.WorkflowInspect(workflowID)
	if err != nil {
		t.Fatalf("WorkflowInspect %s: %v", workflowID, err)
	}
	inst, ok := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	if !ok {
		t.Fatalf("inspect %s missing instance: %#v", workflowID, insp)
	}
	return inst
}

// installCancelWindowProvider routes provider creation through a counting
// cancelWindowProvider and returns the provider and the factory-call counter.
func installCancelWindowProvider(f *dispatchFixture) (*cancelWindowProvider, *atomic.Int32) {
	provider := &cancelWindowProvider{blockingAdmissionProvider: f.provider}
	var calls atomic.Int32
	f.sup.newProviderFunc = func(_ runtime.StartOptions, _ string) (runtime.Provider, error) {
		calls.Add(1)
		return provider, nil
	}
	return provider, &calls
}

// TestDispatchStartsNoProviderWhenCancelCommitsAfterBeginDispatch drives one
// dispatch and lands workflow.cancel in the window between the committed
// attempt intent and the attempt's runtime registration. The cancel is issued
// synchronously from the afterBeginDispatch seam, so the interleaving is exact:
// the cancel commits and returns before the executor starts. The canceled
// workflow's provider must never start.
func TestDispatchStartsNoProviderWhenCancelCommitsAfterBeginDispatch(t *testing.T) {
	t.Parallel()
	f := newDispatchFixture(t, "dispatch-cancel-window", 4, 4, "")
	provider, providerCalls := installCancelWindowProvider(f)

	var (
		seamAttemptID string
		cancelErr     error
		cancelResult  map[string]any
	)
	f.sup.testHooks.afterBeginDispatch = func(attemptID string) {
		seamAttemptID = attemptID
		cancelResult, cancelErr = cancelWorkflowInstance(t, f.sup, f.wf, "cancel after the attempt intent, before the runtime")
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if seamAttemptID == "" || seamAttemptID != out.AttemptID {
		t.Fatalf("afterBeginDispatch attempt = %q, dispatch attempt = %q, want the committed attempt", seamAttemptID, out.AttemptID)
	}
	if cancelErr != nil {
		t.Fatalf("cancel in the begin-dispatch window: %v", cancelErr)
	}
	if got := cancelResult["status"]; got != string(workflow.WorkflowCanceled) {
		t.Fatalf("cancel result status = %v, want %q (result %#v)", got, workflow.WorkflowCanceled, cancelResult)
	}
	// The cancel succeeded against an instance with no registered runtime: the
	// attempt's child runtime is registered by the executor, after this window.
	if got := cancelResult["canceled_runtimes"]; got != 0 {
		t.Fatalf("cancel stopped %v runtimes, want 0 in the post-intent window (result %#v)", got, cancelResult)
	}
	inst := workflowInstance(t, f.mgr, f.wf)
	if inst.Status != workflow.WorkflowCanceled {
		t.Fatalf("instance status after the window cancel = %s, want %s", inst.Status, workflow.WorkflowCanceled)
	}

	// The desired behavior: a successful cancel means the attempt's provider
	// never executes. Provider creation is synchronous inside the executor, so
	// after dispatchWorkflowNode returns a started provider is already counted.
	if got := providerCalls.Load(); got != 0 {
		t.Fatalf("provider executed for a canceled workflow: provider factory calls = %d, session starts = %d, prompts = %d, "+
			"dispatch outcome = %s (runtime %q), instance = %s",
			got, provider.starts.Load(), provider.prompts.Load(), out.Kind, out.RuntimeID, describeInstance(&inst, got))
	}
	if provider.starts.Load() != 0 || provider.prompts.Load() != 0 {
		t.Fatalf("provider started %d session(s) and %d prompt(s) for a canceled workflow",
			provider.starts.Load(), provider.prompts.Load())
	}
	if out.Kind != DispatchCanceled {
		t.Fatalf("dispatch outcome = %s (%s), want %s", out.Kind, out.Detail, DispatchCanceled)
	}
	if got := activeRuntimeCount(f.sup); got != 0 {
		t.Fatalf("live runtimes = %d after the canceled dispatch, want 0", got)
	}
	// The attempt carries the pre-start cancellation fact, not a provider
	// failure and not a run that was merely never recorded.
	attempts := attemptsForNode(&inst, "start")
	if len(attempts) != 1 || attempts[0].Status != workflow.AttemptFailed {
		t.Fatalf("attempts = %+v, want one failed pre-start attempt", attempts)
	}
	if attempts[0].MarkerKind != "dispatch" || attempts[0].MarkerLabel != "dispatch_canceled" {
		t.Fatalf("attempt marker = %q/%q, want dispatch/dispatch_canceled; instance = %s",
			attempts[0].MarkerKind, attempts[0].MarkerLabel, describeInstance(&inst, providerCalls.Load()))
	}
}

// TestDispatchStartsNoProviderWhenCancelCommitsBeforeAttemptRegistration lands
// the cancel after every step the dispatch path performs on its own and
// immediately before the registration critical section. A status recheck
// placed anywhere on the dispatch path alone is a TOCTOU: it runs before this
// seam, sees an uncanceled instance, and the provider starts anyway. Only a
// check inside the registration critical section — which the cancel's runtime
// snapshot serializes against — closes this ordering.
func TestDispatchStartsNoProviderWhenCancelCommitsBeforeAttemptRegistration(t *testing.T) {
	t.Parallel()
	f := newDispatchFixture(t, "dispatch-cancel-pre-registration", 4, 4, "")
	provider, providerCalls := installCancelWindowProvider(f)

	var (
		seamAttemptID string
		cancelErr     error
		cancelResult  map[string]any
	)
	f.sup.testHooks.beforeAttemptRegistration = func(attemptID string) {
		seamAttemptID = attemptID
		cancelResult, cancelErr = cancelWorkflowInstance(t, f.sup, f.wf, "cancel after every dispatch-side step, before registration")
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if seamAttemptID == "" || seamAttemptID != out.AttemptID {
		t.Fatalf("beforeAttemptRegistration attempt = %q, dispatch attempt = %q, want the committed attempt", seamAttemptID, out.AttemptID)
	}
	if cancelErr != nil {
		t.Fatalf("cancel before the registration critical section: %v", cancelErr)
	}
	if got := cancelResult["status"]; got != string(workflow.WorkflowCanceled) {
		t.Fatalf("cancel result status = %v, want %q (result %#v)", got, workflow.WorkflowCanceled, cancelResult)
	}
	if got := cancelResult["canceled_runtimes"]; got != 0 {
		t.Fatalf("cancel stopped %v runtimes, want 0 before the runtime is registered (result %#v)", got, cancelResult)
	}
	inst := workflowInstance(t, f.mgr, f.wf)
	if inst.Status != workflow.WorkflowCanceled {
		t.Fatalf("instance status after the window cancel = %s, want %s", inst.Status, workflow.WorkflowCanceled)
	}
	if got := providerCalls.Load(); got != 0 {
		t.Fatalf("provider executed for a workflow canceled before its attempt registered: provider factory calls = %d, "+
			"session starts = %d, prompts = %d, dispatch outcome = %s (runtime %q), instance = %s",
			got, provider.starts.Load(), provider.prompts.Load(), out.Kind, out.RuntimeID, describeInstance(&inst, got))
	}
	if out.Kind != DispatchCanceled {
		t.Fatalf("dispatch outcome = %s (%s), want %s", out.Kind, out.Detail, DispatchCanceled)
	}
	if got := activeRuntimeCount(f.sup); got != 0 {
		t.Fatalf("live runtimes = %d after the canceled dispatch, want 0", got)
	}
}

// TestAttemptStartHoldsWhenInstanceStatusIsUnreadable proves the registration
// check is fail-closed: when the attempt's durable instance status cannot be
// read at registration time, the attempt's provider must not start. Unknown
// ownership is not dispatchable.
func TestAttemptStartHoldsWhenInstanceStatusIsUnreadable(t *testing.T) {
	t.Parallel()
	f := newDispatchFixture(t, "dispatch-attempt-unreadable", 4, 4, "")
	provider, providerCalls := installCancelWindowProvider(f)

	instanceDir := filepath.Join(f.mgr.Store().Root(), "instances", f.wf)
	var seamAttemptID string
	f.sup.testHooks.beforeAttemptRegistration = func(attemptID string) {
		seamAttemptID = attemptID
		// The attempt's durable instance disappears between the committed intent
		// and its registration: the registration read cannot learn the owner
		// state, so the start must hold.
		if err := os.RemoveAll(instanceDir); err != nil {
			t.Fatalf("remove instance state in the registration window: %v", err)
		}
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if seamAttemptID == "" || seamAttemptID != out.AttemptID {
		t.Fatalf("beforeAttemptRegistration attempt = %q, dispatch attempt = %q, want the committed attempt", seamAttemptID, out.AttemptID)
	}
	if got := providerCalls.Load(); got != 0 {
		t.Fatalf("provider executed for an attempt with unreadable ownership: provider factory calls = %d, session starts = %d, "+
			"dispatch outcome = %s (%s)", got, provider.starts.Load(), out.Kind, out.Detail)
	}
	if out.Kind != DispatchStartFailed {
		t.Fatalf("dispatch outcome = %s (%s), want %s", out.Kind, out.Detail, DispatchStartFailed)
	}
	if got := activeRuntimeCount(f.sup); got != 0 {
		t.Fatalf("live runtimes = %d after the held attempt start, want 0", got)
	}
}

// TestCancelAfterRegisteredRuntimeStopsTheAttempt pins the complementary
// ordering: the dispatch registered the attempt's runtime and its provider is
// mid-turn, then the cancel finds that runtime and stops it. The attempt's
// provider never runs a second time.
func TestCancelAfterRegisteredRuntimeStopsTheAttempt(t *testing.T) {
	t.Parallel()
	f := newDispatchFixture(t, "dispatch-cancel-post-registration", 4, 4, "")
	provider, providerCalls := installCancelWindowProvider(f)

	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if out.Kind != Dispatched {
		t.Fatalf("dispatch outcome = %s (%s), want dispatched", out.Kind, out.Detail)
	}
	waitFor(t, "the attempt's provider turn to start", func() bool { return provider.prompts.Load() == 1 })
	waitFor(t, "the attempt's runtime to be live", func() bool { return activeRuntimeCount(f.sup) == 1 })

	cancelResult, err := cancelWorkflowInstance(t, f.sup, f.wf, "cancel a dispatched attempt")
	if err != nil {
		t.Fatalf("cancel after registration: %v", err)
	}
	if got := cancelResult["status"]; got != string(workflow.WorkflowCanceled) {
		t.Fatalf("cancel result status = %v, want %q (result %#v)", got, workflow.WorkflowCanceled, cancelResult)
	}
	if got := cancelResult["canceled_runtimes"]; got != 1 {
		t.Fatalf("cancel stopped %v runtimes, want the attempt's registered runtime (result %#v)", got, cancelResult)
	}
	waitFor(t, "the canceled attempt's runtime to complete", func() bool { return activeRuntimeCount(f.sup) == 0 })

	if got := providerCalls.Load(); got != 1 {
		t.Fatalf("provider factory calls = %d after the cancel, want the single pre-cancel attempt", got)
	}
	if got := provider.prompts.Load(); got != 1 {
		t.Fatalf("provider prompts = %d, want the single pre-cancel turn", got)
	}
	inst := workflowInstance(t, f.mgr, f.wf)
	if inst.Status != workflow.WorkflowCanceled {
		t.Fatalf("instance status = %s, want %s; observed %s", inst.Status, workflow.WorkflowCanceled, describeInstance(&inst, providerCalls.Load()))
	}
	act := activationFor(&inst, "start")
	if act == nil || act.Status != workflow.ActivationCanceled {
		t.Fatalf("activation = %+v, want canceled; observed %s", act, describeInstance(&inst, providerCalls.Load()))
	}
	attempts := attemptsForNode(&inst, "start")
	if len(attempts) != 1 || attempts[0].Status != workflow.AttemptCanceled {
		t.Fatalf("attempts = %+v, want one canceled attempt", attempts)
	}
}

// TestDispatchStartsNoProviderWhenCancelCommitsInsideRegistrationLock commits
// the cancel while the registration critical section is held, through the
// manager directly (the supervisor's own cancel handler deadlocks on the
// held lock). This pins where the refusal read lives: the hook runs after the
// lock is acquired and before the status read, so a read placed anywhere
// earlier — outside the lock, or before it is acquired — sees an uncanceled
// instance, starts the provider, and fails this test.
func TestDispatchStartsNoProviderWhenCancelCommitsInsideRegistrationLock(t *testing.T) {
	t.Parallel()
	f := newDispatchFixture(t, "dispatch-cancel-in-lock", 4, 4, "")
	provider, providerCalls := installCancelWindowProvider(f)

	var (
		seamAttemptID string
		cancelErr     error
		cancelResult  any
	)
	f.sup.testHooks.insideAttemptRegistration = func(attemptID string) {
		seamAttemptID = attemptID
		cancelResult, cancelErr = f.mgr.WorkflowCancel(f.wf, mustJSON(t, map[string]string{
			"actor":  "ops",
			"reason": "cancel committed inside the registration critical section",
		}))
	}
	out, err := f.sup.dispatchWorkflowNode(t.Context(), f.dispatchRequest(nil))
	if err != nil {
		t.Fatalf("dispatchWorkflowNode: %v", err)
	}
	if seamAttemptID == "" || seamAttemptID != out.AttemptID {
		t.Fatalf("insideAttemptRegistration attempt = %q, dispatch attempt = %q, want the committed attempt", seamAttemptID, out.AttemptID)
	}
	if cancelErr != nil {
		t.Fatalf("cancel inside the registration lock: %v", cancelErr)
	}
	if got := cancelResult.(map[string]any)["status"]; got != string(workflow.WorkflowCanceled) {
		t.Fatalf("cancel result status = %v, want %q (result %#v)", got, workflow.WorkflowCanceled, cancelResult)
	}
	if got := providerCalls.Load(); got != 0 {
		t.Fatalf("provider executed for a cancel committed inside the registration lock: provider factory calls = %d, "+
			"session starts = %d, prompts = %d, dispatch outcome = %s (runtime %q)",
			got, provider.starts.Load(), provider.prompts.Load(), out.Kind, out.RuntimeID)
	}
	if out.Kind != DispatchCanceled {
		t.Fatalf("dispatch outcome = %s (%s), want %s", out.Kind, out.Detail, DispatchCanceled)
	}
	if got := activeRuntimeCount(f.sup); got != 0 {
		t.Fatalf("live runtimes = %d after the canceled dispatch, want 0", got)
	}
}

// TestAttemptStartFailureMarkerClassifiesTheRefusal pins the marker
// classification shared by every workflow executor's start-failure path: a
// refusal error must still match through wrapping (the executors receive it
// from spawn), and a plain start failure keeps the executor's own marker.
func TestAttemptStartFailureMarkerClassifiesTheRefusal(t *testing.T) {
	refusal := fmt.Errorf("start attempt: %w", errAttemptWorkflowCanceled)
	for _, startErr := range []error{errAttemptWorkflowCanceled, refusal} {
		got := attemptStartFailureMarker(startErr, "loop", "")
		if got[0] != dispatchCanceledMarkerKind || got[1] != dispatchCanceledMarkerLabel {
			t.Fatalf("refusal %v: marker = %v, want %s/%s", startErr, got, dispatchCanceledMarkerKind, dispatchCanceledMarkerLabel)
		}
	}
	if got := attemptStartFailureMarker(errors.New("provider unavailable"), "loop", ""); got[0] != "loop" || got[1] != "" {
		t.Fatalf("plain start failure: marker = %v, want the executor's loop marker", got)
	}
	if got := attemptStartFailureMarker(errors.New("provider unavailable")); got != nil {
		t.Fatalf("plain start failure without a marker: marker = %v, want none", got)
	}
}
