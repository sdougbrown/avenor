package stable

// Integration tests for external-gate adapter polling over real fixture
// adapters: an auto external node parks automatically after publication
// completes, registered adapters drive the pinned success_outcome only when
// every required gate passes, advisory results route through their declared
// branches, a new publication head supersedes the old review, staged
// evidence backs the gate result, unavailable adapters record deduplicated
// diagnostics, and disable kills an in-flight adapter's process group.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
	"github.com/sdougbrown/avenor/internal/workflowcontroller"
)

// pollSubject is the exact subject every fixture adapter reports; the test
// publication drives the same repository/pull request/head so pins match.
var pollSubject = map[string]any{"type": "pull_request", "repository": "sdougbrown/avenor", "pull_request": 143, "revision": "cc793f7"}

// pollReviewTemplate builds the integration template: a manual publication
// recording the subject outputs, an auto external review node whose bound
// gate consumes them, and a manual merge node behind a human gate.
func pollReviewTemplate(t *testing.T, templateID string, gates []map[string]any) []byte {
	t.Helper()
	reviewGateInputs := map[string]any{
		"repository":  map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "repository"}},
		"pull_number": map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "pr_number"}},
		"head_sha":    map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "pr_head"}},
	}
	subjectBinding := map[string]any{
		"type":         "pull_request",
		"repository":   map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "repository"}},
		"pull_request": map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "pr_number"}},
		"revision":     map[string]any{"from_node_output": map[string]any{"node_id": "publication", "output_id": "pr_head"}},
	}
	reviewGates := make([]any, 0, len(gates))
	for _, gate := range gates {
		gate["inputs"] = reviewGateInputs
		gate["subject_binding"] = subjectBinding
		reviewGates = append(reviewGates, gate)
	}
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1.0.0",
		"entry_nodes":      []string{"publication"},
		"nodes": []any{
			map[string]any{
				"id":     "publication",
				"action": map[string]any{"type": "manual"},
				"outputs": []any{
					map[string]any{"id": "repository", "name": "Repository", "type": "string", "required": true},
					map[string]any{"id": "pr_number", "name": "PR number", "type": "number", "required": true},
					map[string]any{"id": "pr_head", "name": "PR head SHA", "type": "string", "required": true},
				},
				"outcomes": []any{map[string]any{"name": "published", "target_node_id": "review"}},
			},
			map[string]any{
				"id":           "review",
				"dependencies": []string{"publication"},
				"action":       map[string]any{"type": "external", "source": "github"},
				"dispatch":     map[string]any{"mode": "auto", "controller_id": "c1", "success_outcome": "clean"},
				"branches":     map[string]any{"clean": "merge", "failed": "publication"},
				"gates":        reviewGates,
			},
			map[string]any{
				"id":           "merge",
				"dependencies": []string{"review"},
				"action":       map[string]any{"type": "manual"},
				"gates":        []any{map[string]any{"id": "merge-auth", "type": "human", "required": true}},
			},
		},
		"terminal_outcomes": []string{"done"},
	}
	return mustJSON(t, template)
}

// pollFixture wires a supervisor over a temp workflow root with fixture
// adapters staged in a private manifest directory.
type pollFixture struct {
	sup        *Supervisor
	mgr        *workflow.Manager
	cstore     *workflowcontroller.ControllerStore
	root       string
	adapterDir string
	wf         string
}

// newPollFixture stages the named fixture adapter scripts with owner-only
// permissions, writes one manifest per (id, script) pair, builds the
// supervisor, registers and instantiates the review template, and enables
// the controller (which loads the registry).
func newPollFixture(t *testing.T, name string, templateID string, manifests map[string]string, gates []map[string]any) *pollFixture {
	t.Helper()
	root := filepath.Join(t.TempDir(), "wfroot")
	adapterDir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(adapterDir); err == nil {
		adapterDir = resolved
	}
	if err := os.Chmod(adapterDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for id, script := range manifests {
		exe := stagePollFixture(t, adapterDir, script)
		writePollManifest(t, adapterDir, id+".json", id, exe)
	}
	// Fail loudly here rather than as a silent timeout later: a trust or
	// manifest load failure leaves the supervisor's registry empty and every
	// poll reporting adapter_unavailable forever.
	if _, err := workflowcontroller.LoadAdapterRegistry(adapterDir); err != nil {
		t.Fatalf("adapter registry load from %s: %v", adapterDir, err)
	}
	sup := NewSupervisor(Config{
		ControlSocket:      newStableSocketPath(t, name),
		WorkflowRoot:       root,
		WorkflowAdapterDir: adapterDir,
		ShutdownTimeout:    0,
	})
	sup.controllerRenewInterval = 25 * time.Millisecond
	sup.controllerPollBaseDelay = 200 * time.Millisecond
	mgr, cstore, err := sup.workflowBarrierResult()
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}
	f := &pollFixture{sup: sup, mgr: mgr, cstore: cstore, root: root, adapterDir: adapterDir, wf: ""}
	if _, err := mgr.WorkflowCreate(pollReviewTemplate(t, templateID, gates)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	out, err := mgr.WorkflowInstantiate(mustJSON(t, map[string]string{"template_id": templateID, "template_version": "1.0.0"}))
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	f.wf = out.(map[string]any)["workflow_id"].(string)
	if _, err := cstore.Create("c1", 4); err != nil {
		t.Fatalf("controller create: %v", err)
	}
	if _, err := sup.WorkflowControllerEnable("c1"); err != nil {
		t.Fatalf("controller enable: %v", err)
	}
	// The enable-time registry load must have produced exactly the staged
	// adapters; an empty registry means every poll fails unavailable.
	wantIDs := make([]string, 0, len(manifests))
	for id := range manifests {
		wantIDs = append(wantIDs, id)
	}
	sort.Strings(wantIDs)
	reg := sup.workflowAdapters.Load()
	if reg == nil {
		t.Fatal("adapter registry not loaded after controller enable")
	}
	if got := reg.IDs(); !slices.Equal(got, wantIDs) {
		t.Fatalf("loaded adapter IDs = %v, want %v", got, wantIDs)
	}
	t.Cleanup(func() { f.stop(t) })
	return f
}

// stop tears the fixture down: disable first so no fresh poll starts, then
// stop the leader loops and the broker.
func (f *pollFixture) stop(t *testing.T) {
	t.Helper()
	if _, err := f.cstore.SetDesiredState("c1", workflowcontroller.DesiredDisabled, "test cleanup"); err != nil {
		t.Logf("cleanup disable: %v", err)
	}
	f.sup.stopControllerLoops()
	_ = f.sup.broker.Stop()
	f.sup.stopReaper()
}

// stagePollFixture copies a testdata adapter script into dir with
// owner-only permissions and returns its path.
func stagePollFixture(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "workflowcontroller", "testdata", "adapters", name))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// writePollManifest writes a valid manifest for id pointing at exe with the
// shared poll input schema.
func writePollManifest(t *testing.T, dir, filename, id, exe string) {
	t.Helper()
	content := fmt.Sprintf(`{"version":1,"id":%q,"executable":%q,"args":[],"timeout_ms":5000,"max_stdout_bytes":65536,"max_stderr_bytes":65536,"inherit_env":[],"inputs":{"repository":"string","pull_number":"integer","head_sha":"string"}}`, id, exe)
	if err := os.WriteFile(filepath.Join(dir, filename), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// workflowInstance returns the workflow's current instance.
func (f *pollFixture) workflowInstance(t *testing.T) workflow.WorkflowInstance {
	t.Helper()
	insp, err := f.mgr.WorkflowInspect(f.wf)
	if err != nil {
		t.Fatalf("WorkflowInspect: %v", err)
	}
	inst, ok := insp.(map[string]any)["instance"].(workflow.WorkflowInstance)
	if !ok {
		t.Fatalf("inspect %s missing instance: %#v", f.wf, insp)
	}
	return inst
}

// activationByNode returns the newest activation of a node.
func (f *pollFixture) activationByNode(t *testing.T, nodeID string) *workflow.Activation {
	t.Helper()
	var found *workflow.Activation
	for i := range f.workflowInstance(t).Activations {
		a := f.workflowInstance(t).Activations[i]
		if a.NodeID == workflow.NodeID(nodeID) {
			found = &a
		}
	}
	return found
}

// drivePublication claims, starts, and completes the newest pending
// publication activation with the fixture subject outputs.
func (f *pollFixture) drivePublication(t *testing.T, repository string, pullNumber int, head string) {
	t.Helper()
	act := f.activationByNode(t, "publication")
	if act == nil || act.Status != workflow.ActivationPending {
		t.Fatalf("no pending publication activation: %+v", act)
	}
	res, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "claim", "node_id": "publication", "activation_id": string(act.ID), "actor": "alice",
	}))
	if err != nil {
		t.Fatalf("publication claim: %v", err)
	}
	claim := res.(map[string]any)
	start, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "start", "node_id": "publication", "activation_id": string(act.ID),
		"lease_id": claim["lease_id"], "owner_token": claim["owner_token"],
	}))
	if err != nil {
		t.Fatalf("publication start: %v", err)
	}
	attemptID := start.(map[string]any)["attempt_id"].(string)
	if err := f.mgr.RecordAttemptTerminated(workflow.WorkflowID(f.wf), "publication", act.ID,
		workflow.AttemptID(attemptID), workflow.LeaseID(claim["lease_id"].(string)), workflow.AttemptSucceeded); err != nil {
		t.Fatalf("publication terminate: %v", err)
	}
	if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "complete", "node_id": "publication", "activation_id": string(act.ID),
		"attempt_id": attemptID, "lease_id": claim["lease_id"], "owner_token": claim["owner_token"],
		"outcome": "published",
		"outputs": []map[string]any{
			{"definition_id": "repository", "value": repository},
			{"definition_id": "pr_number", "value": pullNumber},
			{"definition_id": "pr_head", "value": head},
		},
	})); err != nil {
		t.Fatalf("publication complete: %v", err)
	}
}

// waitReviewStatus polls until the newest review activation has the status.
func (f *pollFixture) waitReviewStatus(t *testing.T, want workflow.ActivationStatus) {
	t.Helper()
	waitFor(t, "review status "+string(want), func() bool {
		act := f.activationByNode(t, "review")
		return act != nil && act.Status == want
	})
}

// waitCursor waits until a cursor exists for the review gate and returns it.
func (f *pollFixture) waitCursor(t *testing.T, gateID string) workflowcontroller.PollCursor {
	t.Helper()
	waitFor(t, "poll cursor for "+gateID, func() bool {
		_, ok := f.findCursor(gateID)
		return ok
	})
	cursor, ok := f.findCursor(gateID)
	if !ok {
		t.Fatalf("cursor vanished")
	}
	return cursor
}

// findCursor returns the poll cursor for a review gate.
func (f *pollFixture) findCursor(gateID string) (workflowcontroller.PollCursor, bool) {
	rec, _, err := f.cstore.Get("c1")
	if err != nil {
		return workflowcontroller.PollCursor{}, false
	}
	for _, c := range rec.PollCursors {
		if c.WorkflowID == f.wf && c.NodeID == "review" && c.GateID == gateID {
			return *c, true
		}
	}
	return workflowcontroller.PollCursor{}, false
}

// findCursorForActivation returns the poll cursor for one activation's gate.
func (f *pollFixture) findCursorForActivation(gateID string, actID workflow.ActivationID) (workflowcontroller.PollCursor, bool) {
	rec, _, err := f.cstore.Get("c1")
	if err != nil {
		return workflowcontroller.PollCursor{}, false
	}
	for _, c := range rec.PollCursors {
		if c.WorkflowID == f.wf && c.ActivationID == string(actID) && c.GateID == gateID {
			return *c, true
		}
	}
	return workflowcontroller.PollCursor{}, false
}

// gateInstances returns the workflow's recorded gate instances.
func (f *pollFixture) gateInstances(t *testing.T) []workflow.GateInstance {
	t.Helper()
	return f.workflowInstance(t).Gates
}

// TestControllerRunnerReseedsParkedCursor proves the anti-entropy pass
// repairs a lost poll cursor: an activation parked through the manager
// directly with no cursor (the state after a crash between the park commit
// and cursor creation) gets its cursor re-seeded by a freshly started runner
// and the gate is polled.
func TestControllerRunnerReseedsParkedCursor(t *testing.T) {
	f := newPollFixture(t, "poll-reseed", "poll-reseed-tmpl",
		map[string]string{"gh-review": "passed.sh"},
		[]map[string]any{{"id": "pr-review", "type": "external", "required": true, "adapter_id": "gh-review"}})
	// Stop the leader loop so the park below lands through the manager
	// directly, without the runner's post-park cursor creation.
	f.sup.stopControllerLoop("c1")
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")

	// Park the ready review activation under a manually acquired leader
	// lease, then release the lease: awaiting_gate, no cursor anywhere.
	inst := f.workflowInstance(t)
	act := f.activationByNode(t, "review")
	rec, granted, err := f.cstore.AcquireLease("c1", "park-owner")
	if err != nil || !granted {
		t.Fatalf("acquire lease: granted=%v err=%v", granted, err)
	}
	parkErr := f.cstore.WithLeader("c1", rec.Leader.LeaseID, rec.Leader.OwnerEpoch, func() error {
		_, err := f.mgr.ParkExternal(workflow.ParkExternalRequest{
			WorkflowID:       workflow.WorkflowID(f.wf),
			NodeID:           "review",
			ActivationID:     act.ID,
			ExpectedRevision: inst.Revision,
			ControllerID:     "c1",
			LeaderLeaseID:    rec.Leader.LeaseID,
		})
		return err
	})
	if parkErr != nil {
		t.Fatalf("park external: %v", parkErr)
	}
	if _, err := f.cstore.ReleaseLease("c1", rec.Leader.LeaseID, rec.Leader.OwnerEpoch); err != nil {
		t.Fatalf("release lease: %v", err)
	}
	f.waitReviewStatus(t, workflow.ActivationAwaitingGate)
	if _, ok := f.findCursor("pr-review"); ok {
		t.Fatalf("cursor exists before the runner re-seeds it")
	}

	// A fresh runner re-seeds the missing cursor on its first anti-entropy
	// pass and polls the gate; the adapter drives the success outcome.
	f.sup.startControllerLoop(f.cstore, "c1")
	f.waitCursor(t, "pr-review")
	waitFor(t, "re-seeded cursor polled", func() bool {
		c, ok := f.findCursor("pr-review")
		return ok && c.PollCount >= 1
	})
	f.waitReviewStatus(t, workflow.ActivationSatisfied)
}

// TestControllerPollAutoParkPassesWithEvidence proves the full happy path:
// the review activation parks automatically after publication completes
// (no attempt, no admission), the registered passed adapter drives the
// pinned success_outcome, the gate result references staged evidence whose
// bytes are the adapter's raw stdout, and the merge node stays pending
// behind its human gate — the adapter can never satisfy a human gate or
// trigger a merge.
func TestControllerPollAutoParkPassesWithEvidence(t *testing.T) {
	f := newPollFixture(t, "poll-passed", "poll-passed-tmpl",
		map[string]string{"gh-review": "passed.sh"},
		[]map[string]any{{"id": "pr-review", "type": "external", "required": true, "adapter_id": "gh-review"}})
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")

	f.waitReviewStatus(t, workflow.ActivationAwaitingGate)
	f.waitCursor(t, "pr-review")
	// The first poll commits before the adapter runs. The committed event is
	// durable in the controller's event log, unlike the cursor itself, which
	// the applied result clears — a commit-then-clear can complete entirely
	// between two polls of a cursor-based wait on a fast host.
	eventsPath := filepath.Join(f.cstore.ControllersRoot(), "c1", "events.ndjson")
	waitFor(t, "committed first poll", func() bool {
		data, err := os.ReadFile(eventsPath)
		return err == nil && bytes.Count(data, []byte(`"kind":"poll_committed"`)) >= 1
	})
	// The parked activation carries no runtime state.
	review := f.activationByNode(t, "review")
	if len(review.AttemptIDs) != 0 || review.ActiveLease != nil {
		t.Fatalf("parked review recorded runtime state: %+v", review)
	}
	// The adapter drives the pinned success_outcome once the gate passes.
	f.waitReviewStatus(t, workflow.ActivationSatisfied)
	if review := f.activationByNode(t, "review"); review.SelectedOutcome != workflow.OutcomeName("clean") {
		t.Fatalf("review outcome = %q, want clean", review.SelectedOutcome)
	}
	// The adapter response is staged evidence referenced by the gate result.
	gates := f.gateInstances(t)
	if len(gates) != 1 || gates[0].Status != workflow.GatePassed || len(gates[0].EvidenceIDs) != 1 {
		t.Fatalf("gate instances = %+v, want one passed gate with evidence", gates)
	}
	evidenceID := gates[0].EvidenceIDs[0]
	if gates[0].PollID == "" || gates[0].ResponseHash == "" {
		t.Fatalf("gate instance = %+v, want the poll id and response hash", gates[0])
	}
	evPath := filepath.Join(f.root, "instances", f.wf, "evidence", string(evidenceID), "adapter-response.json")
	raw, err := os.ReadFile(evPath)
	if err != nil {
		t.Fatalf("staged evidence: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil || parsed["result"] != "passed" {
		t.Fatalf("staged evidence = %s (%v), want the adapter stdout", raw, err)
	}
	// The merge node stays pending behind its human gate: no attempt, no
	// gate decision, no merge.
	merge := f.activationByNode(t, "merge")
	if merge == nil || merge.Status != workflow.ActivationPending {
		t.Fatalf("merge activation = %+v, want pending (adapter cannot trigger merge)", merge)
	}
	for _, gi := range f.gateInstances(t) {
		if gi.GateID == "merge-auth" {
			t.Fatalf("human gate decided: %+v", gi)
		}
	}
}

// TestControllerPollChangesRequestedRoutesCorrection proves an unmapped-free
// changes_requested result routes through result_outcomes onto the declared
// correction branch.
func TestControllerPollChangesRequestedRoutesCorrection(t *testing.T) {
	f := newPollFixture(t, "poll-changes", "poll-changes-tmpl",
		map[string]string{"gh-review": "changes-requested.sh"},
		[]map[string]any{{
			"id": "pr-review", "type": "external", "required": true, "adapter_id": "gh-review",
			"result_outcomes": map[string]any{"changes_requested": "failed"},
		}})
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")

	f.waitReviewStatus(t, workflow.ActivationRejected)
	// The declared branch creates a fresh publication activation for the
	// correction loop.
	waitFor(t, "correction publication activation", func() bool {
		count := 0
		for _, a := range f.workflowInstance(t).Activations {
			if a.NodeID == workflow.NodeID("publication") {
				count++
			}
		}
		return count >= 2
	})
	pubCount := 0
	for _, a := range f.workflowInstance(t).Activations {
		if a.NodeID == "publication" {
			pubCount++
		}
	}
	if pubCount < 2 {
		t.Fatalf("publication activations = %d, want a new one after changes_requested", pubCount)
	}
}

// TestControllerPollPendingAndSilentStayParked proves empty verdict-less
// output and silence both leave the gate pending: the review stays parked
// with scheduled cursors and no terminal result.
func TestControllerPollPendingAndSilentStayParked(t *testing.T) {
	f := newPollFixture(t, "poll-pending", "poll-pending-tmpl",
		map[string]string{
			"gh-review": "empty-commented.sh",
			"ci":        "silent.sh",
		},
		[]map[string]any{
			{"id": "pr-review", "type": "external", "required": true, "adapter_id": "gh-review"},
			{"id": "ci", "type": "external", "required": true, "adapter_id": "ci"},
		})
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")

	f.waitReviewStatus(t, workflow.ActivationAwaitingGate)
	f.waitCursor(t, "pr-review")
	f.waitCursor(t, "ci")
	// Both gates stay open across several poll cycles: pending results
	// record no gate decision and neither pending nor silence resolves the
	// activation.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		review := f.activationByNode(t, "review")
		if review.Status != workflow.ActivationAwaitingGate {
			t.Fatalf("review status = %q, want parked", review.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The polls happened and backed off; no terminal gate decision exists.
	cursor, _ := f.findCursor("pr-review")
	if cursor.RetryCount < 1 {
		t.Fatalf("pr-review retry count = %d, want backed-off polls", cursor.RetryCount)
	}
	if gates := f.gateInstances(t); len(gates) != 0 {
		t.Fatalf("gate instances = %+v, want none (pending results record no decision)", gates)
	}
}

// TestControllerPollNewHeadSupersedesOldReview proves a new publication head
// creates a new review activation that polls with the new subject while the
// old activation can never land another result.
func TestControllerPollNewHeadSupersedesOldReview(t *testing.T) {
	f := newPollFixture(t, "poll-head", "poll-head-tmpl",
		map[string]string{"gh-review": "echo-input.sh"},
		[]map[string]any{{
			"id": "pr-review", "type": "external", "required": true, "adapter_id": "gh-review",
			"result_outcomes": map[string]any{"changes_requested": "failed"},
		}})
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")
	f.waitReviewStatus(t, workflow.ActivationAwaitingGate)
	oldCursor := f.waitCursor(t, "pr-review")

	// The adapter received the pinned inputs (the durable activation
	// outputs), not a controller-chosen subject.
	inputPath := filepath.Join(f.adapterDir, "input.json")
	waitFor(t, "adapter received pinned inputs", func() bool {
		raw, err := os.ReadFile(inputPath)
		return err == nil && strings.Contains(string(raw), `"head_sha":"cc793f7"`)
	})

	// A correction loop publishes a new head: the changes_requested routing
	// is submitted with the pinned subject exactly as a trusted reporter
	// would.
	review1 := f.activationByNode(t, "review")
	oldReviewID := review1.ID
	if _, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "gate", "node_id": "review", "activation_id": string(review1.ID), "gate_id": "pr-review",
		"operation": "external_result", "result": "changes_requested", "poll_id": "poll-manual-1",
		"source": "github", "subject": pollSubject, "response_hash": "hash-1",
		"observed_at": time.Now().UTC(), "evidence_ids": []string{"ev-1"},
	})); err != nil {
		t.Fatalf("changes_requested gate command: %v", err)
	}
	f.drivePublication(t, "sdougbrown/avenor", 143, "def456")
	f.waitReviewStatus(t, workflow.ActivationAwaitingGate)
	review2 := f.activationByNode(t, "review")
	waitFor(t, "new head cursor", func() bool {
		_, ok := f.findCursorForActivation("pr-review", review2.ID)
		return ok
	})
	newCursor, _ := f.findCursorForActivation("pr-review", review2.ID)
	if newCursor.SubjectHash == oldCursor.SubjectHash {
		t.Fatalf("new head reused subject hash %q", newCursor.SubjectHash)
	}
	waitFor(t, "adapter received the new head", func() bool {
		raw, err := os.ReadFile(inputPath)
		return err == nil && strings.Contains(string(raw), `"head_sha":"def456"`)
	})

	// The old cursor self-heals: the runner's next poll of the superseded
	// cursor lands as obsolete and drops it.
	waitFor(t, "old cursor dropped", func() bool {
		_, ok := f.findCursorForActivation("pr-review", oldReviewID)
		return !ok
	})

	// The old activation's result can never land again: the pinned subject
	// moved on and the activation is resolved.
	if oldReviewID == review2.ID {
		t.Fatalf("correction did not create a new review activation")
	}
	_, err := f.mgr.WorkflowCommand(f.wf, mustJSON(t, map[string]any{
		"op": "gate", "node_id": "review", "activation_id": string(oldReviewID), "gate_id": "pr-review",
		"operation": "external_result", "result": "passed", "poll_id": oldCursor.PollID,
		"source": "github", "subject": pollSubject, "response_hash": "hash-2",
		"observed_at": time.Now().UTC(), "evidence_ids": []string{"ev-2"},
	}))
	if err == nil {
		t.Fatalf("stale review result landed")
	}
}

// TestControllerPollAdapterUnavailableDiagnostic proves an unknown adapter
// leaves the gate pending and records a deduplicated diagnostic.
func TestControllerPollAdapterUnavailableDiagnostic(t *testing.T) {
	f := newPollFixture(t, "poll-missing", "poll-missing-tmpl",
		map[string]string{},
		[]map[string]any{{"id": "pr-review", "type": "external", "required": true, "adapter_id": "not-registered"}})
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")
	f.waitReviewStatus(t, workflow.ActivationAwaitingGate)

	waitFor(t, "adapter_unavailable diagnostic", func() bool {
		rec, _, err := f.cstore.Get("c1")
		if err != nil {
			return false
		}
		_, ok := rec.Diagnostics["adapter_unavailable/not-registered"]
		return ok
	})
	f.waitCursor(t, "pr-review")
	review := f.activationByNode(t, "review")
	if review.Status != workflow.ActivationAwaitingGate {
		t.Fatalf("review status = %q, want parked while the adapter is unavailable", review.Status)
	}
}

// TestControllerDisableKillsInFlightAdapter proves disable cancels an
// in-flight adapter invocation: the poll context's cancellation kills the
// adapter's whole process group.
func TestControllerDisableKillsInFlightAdapter(t *testing.T) {
	f := newPollFixture(t, "poll-hang", "poll-hang-tmpl",
		map[string]string{"gh-review": "hang.sh"},
		[]map[string]any{{"id": "pr-review", "type": "external", "required": true, "adapter_id": "gh-review"}})
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")
	f.waitReviewStatus(t, workflow.ActivationAwaitingGate)
	f.waitCursor(t, "pr-review")

	// hang.sh records its child sleeper's pid in the manifest directory.
	pidFile := filepath.Join(f.adapterDir, "child.pid")
	waitFor(t, "in-flight adapter child", func() bool {
		raw, err := os.ReadFile(pidFile)
		return err == nil && strings.TrimSpace(string(raw)) != ""
	})
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("child.pid: %v", err)
	}
	var childPID int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(raw)), "%d", &childPID); err != nil {
		t.Fatalf("parse child.pid %q: %v", raw, err)
	}
	if err := syscall.Kill(childPID, 0); err != nil {
		t.Fatalf("child %d not alive before disable", childPID)
	}

	// Disable persists the desired state first, then cancels the poll
	// context, killing the adapter's process group.
	if _, err := f.sup.WorkflowControllerDisable("c1", mustJSON(t, map[string]string{"reason": "test disable"})); err != nil {
		t.Fatalf("disable: %v", err)
	}
	waitFor(t, "adapter process group gone", func() bool {
		return syscall.Kill(childPID, 0) != nil
	})
	// The cursor stays frozen in flight: the next leader reuses its poll ID.
	rec, _, _ := f.cstore.Get("c1")
	var found *workflowcontroller.PollCursor
	for _, c := range rec.PollCursors {
		if c.GateID == "pr-review" {
			found = c
		}
	}
	if found == nil || found.PollID == "" || !found.NextPollAt.IsZero() {
		t.Fatalf("cursor after disable = %+v, want frozen in flight with a committed poll ID", found)
	}
}

// TestControllerParkLostLeaseReportsNotLeader proves a park under a lost or
// foreign leader lease maps the store's ErrNotLeader onto ResultNotLeader
// with a nil error, so the runner records not_leader instead of a raw
// dispatch_error and keeps leadership.
func TestControllerParkLostLeaseReportsNotLeader(t *testing.T) {
	f := newPollFixture(t, "poll-notleader", "poll-notleader-tmpl",
		map[string]string{"gh-review": "passed.sh"},
		[]map[string]any{{"id": "pr-review", "type": "external", "required": true, "adapter_id": "gh-review"}})
	// Stop the leader loop so the park below lands through the test
	// directly, without the runner's post-park cursor creation.
	f.sup.stopControllerLoop("c1")
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")
	inst := f.workflowInstance(t)
	act := f.activationByNode(t, "review")
	if act == nil {
		t.Fatal("no review activation")
	}
	dec := workflowcontroller.Decision{
		Candidate: workflowcontroller.Candidate{
			Identity: workflow.ExecutionIdentity{
				WorkflowID:   workflow.WorkflowID(f.wf),
				NodeID:       "review",
				ActivationID: act.ID,
			},
			Kind:         workflowcontroller.CandidateExternalPark,
			ControllerID: "c1",
			Revision:     inst.Revision,
		},
	}
	// A foreign lease (not the live leader) makes WithLeader return
	// ErrNotLeader before ParkExternal runs.
	res, err := f.sup.parkExternalNode(dec, workflowcontroller.LeaderLease{LeaseID: "foreign-lease", OwnerEpoch: 0})
	if err != nil {
		t.Fatalf("park under foreign lease: %v", err)
	}
	if res.Kind != workflowcontroller.ResultNotLeader {
		t.Fatalf("park kind = %q, want not_leader", res.Kind)
	}
}

// TestStartupSweepRemovesOrphanedStagingFilesOnly proves the startup barrier
// sweeps orphaned adapter staging files without touching anything else: a
// pre-staged <root>/adapter-poll loses its temp staging file, keeps its
// subdirectory (the sweep only removes non-directory entries), and a real
// evidence copy staged by a live workflow is immutable across the sweep.
func TestStartupSweepRemovesOrphanedStagingFilesOnly(t *testing.T) {
	root := filepath.Join(t.TempDir(), "wfroot")
	staging := filepath.Join(root, "adapter-poll")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(staging, "poll-orphan.json")
	if err := os.WriteFile(orphan, []byte(`{"result":"passed"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(staging, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	nestedFile := filepath.Join(nested, "keep.json")
	if err := os.WriteFile(nestedFile, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	evidenceFile := filepath.Join(root, "instances", "wf-live", "evidence", "ev-1", "adapter-response.json")
	if err := os.MkdirAll(filepath.Dir(evidenceFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(evidenceFile, []byte(`{"result":"passed"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	sup := NewSupervisor(Config{
		ControlSocket: newStableSocketPath(t, "poll-sweep"),
		WorkflowRoot:  root,
	})
	_, _, err := sup.workflowBarrierResult() // the barrier runs the sweep
	if err != nil {
		t.Fatalf("workflow barrier: %v", err)
	}

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphaned staging file = %v, want removed", err)
	}
	if _, err := os.Stat(nestedFile); err != nil {
		t.Fatalf("nested staging entry removed: %v (the sweep only removes files)", err)
	}
	data, err := os.ReadFile(evidenceFile)
	if err != nil || string(data) != `{"result":"passed"}` {
		t.Fatalf("staged evidence copy changed: %q (%v), want immutable", data, err)
	}
}

// TestControllerPollFailedGateCommandDiscardsEvidence proves the discard path
// in applyPollResult: when the external_result gate command fails after the
// evidence staged successfully, the staged evidence is removed and the gate
// stays open for the next poll. The workflow's event log is made unwritable
// after parking, so every store read succeeds but every command commit fails.
func TestControllerPollFailedGateCommandDiscardsEvidence(t *testing.T) {
	f := newPollFixture(t, "poll-discard", "poll-discard-tmpl",
		map[string]string{"gh-review": "passed.sh"},
		[]map[string]any{{"id": "pr-review", "type": "external", "required": true, "adapter_id": "gh-review"}})
	f.drivePublication(t, "sdougbrown/avenor", 143, "cc793f7")

	f.waitReviewStatus(t, workflow.ActivationAwaitingGate)
	f.waitCursor(t, "pr-review")

	// Break only the workflow commit: reads of the parked state succeed, but
	// the gate command's event append fails after the evidence staged.
	eventsPath := filepath.Join(f.root, "instances", f.wf, "events.ndjson")
	if err := os.Chmod(eventsPath, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(eventsPath, 0o644) })

	// The runner keeps polling: each cycle stages the adapter stdout, fails
	// the gate command, and discards the evidence.
	eventsCtrl := filepath.Join(f.cstore.ControllersRoot(), "c1", "events.ndjson")
	waitFor(t, "repeated poll commits under the broken commit", func() bool {
		data, err := os.ReadFile(eventsCtrl)
		return err == nil && bytes.Count(data, []byte(`"kind":"poll_committed"`)) >= 2
	})
	f.sup.stopControllerLoop("c1")

	// The gate never landed and nothing it staged survived.
	if gates := f.gateInstances(t); len(gates) != 0 {
		t.Fatalf("gate instances = %+v, want none (the gate command failed)", gates)
	}
	if review := f.activationByNode(t, "review"); review == nil || review.Status != workflow.ActivationAwaitingGate {
		t.Fatalf("review status = %+v, want still parked awaiting_gate", review)
	}
	evidenceRoot := filepath.Join(f.root, "instances", f.wf, "evidence")
	entries, err := os.ReadDir(evidenceRoot)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read evidence root: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("evidence entries = %v, want all discarded", entries)
	}
	// The failed applies leave the cursor parked for retry.
	cursor, ok := f.findCursor("pr-review")
	if !ok {
		t.Fatal("poll cursor vanished after failed applies, want it left for retry")
	}
	if cursor.RetryCount < 1 {
		t.Fatalf("cursor retry count = %d, want backed-off retries", cursor.RetryCount)
	}
}
