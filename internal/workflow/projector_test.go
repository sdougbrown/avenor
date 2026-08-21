package workflow

// Internal tests for the deterministic Markdown projector. These pin the exact
// projected bytes with committed golden files and prove that a projection
// failure never fails a committed state transition. They use only the
// standard library.

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// update regenerates the golden files under testdata/projections when set:
//
//	go test ./internal/workflow/ -run Projection -update
var update = flag.Bool("update", false, "regenerate projection golden files")

// goldenSnapshot builds a fixed, deterministic snapshot by hand. It carries no
// wall-clock values (zero times throughout) and exercises multiple nodes,
// repeated iterations on one node, multiple attempts/evidence/gates, and two
// gate instances on different nodes (so review-1.md and review-2.md differ and
// live under different node dirs).
func goldenSnapshot() Snapshot {
	return Snapshot{
		SchemaVersion: 1,
		Instance: WorkflowInstance{
			WorkflowID:      "wf_golden",
			InstanceID:      "wfi_golden",
			TemplateID:      "tmpl",
			TemplateVersion: "2",
			Revision:        4,
			CreatedAt:       time.Time{},
			UpdatedAt:       time.Time{},
			Status:          WorkflowAwaitingGate,
			Activations: []Activation{
				{
					ID:              "act_build_1",
					NodeID:          "build",
					Iteration:       0,
					Status:          ActivationSatisfied,
					AttemptIDs:      []AttemptID{"att_build_1"},
					SelectedOutcome: "ok",
				},
				{
					ID:         "act_build_2",
					NodeID:     "build",
					Iteration:  1,
					Status:     ActivationRunning,
					AttemptIDs: []AttemptID{"att_build_2"},
				},
				{
					ID:         "act_review_1",
					NodeID:     "review",
					Iteration:  0,
					Status:     ActivationAwaitingGate,
					AttemptIDs: []AttemptID{"att_review_1"},
				},
				{
					// Attached but its child has not reached a terminal outcome.
					ID:          "act_spawn_1",
					NodeID:      "spawn",
					Status:      ActivationAwaitingChild,
					AttemptIDs:  []AttemptID{"att_spawn_1"},
					ActiveLease: &Lease{ID: "lease_spawn_1", ActivationID: "act_spawn_1", Owner: "kernel"},
				},
			},
			Attempts: []Attempt{
				{
					ID: "att_build_1",
					Identity: ExecutionIdentity{
						WorkflowID:   "wf_golden",
						NodeID:       "build",
						ActivationID: "act_build_1",
						AttemptID:    "att_build_1",
					},
					Status:  AttemptSucceeded,
					Backend: "pi",
					Agent:   "builder",
					Model:   "qwen",
				},
				{
					ID: "att_build_2",
					Identity: ExecutionIdentity{
						WorkflowID:   "wf_golden",
						NodeID:       "build",
						ActivationID: "act_build_2",
						AttemptID:    "att_build_2",
					},
					Status:  AttemptRunning,
					Backend: "pi",
					Agent:   "builder",
					Model:   "qwen",
				},
				{
					ID: "att_review_1",
					Identity: ExecutionIdentity{
						WorkflowID:   "wf_golden",
						NodeID:       "review",
						ActivationID: "act_review_1",
						AttemptID:    "att_review_1",
					},
					Status:  AttemptSucceeded,
					Backend: "pi",
					Agent:   "reviewer",
					Model:   "claude",
				},
			},
			Evidence: []Evidence{
				{
					ID:           "ev_build_1",
					Kind:         "git",
					Source:       EvidenceMachine,
					Authority:    "ci",
					StoredPath:   "evidence/ev_build_1.bin",
					SHA256:       "aaaa",
					ActivationID: "act_build_1",
				},
				{
					ID:           "ev_review_1",
					Kind:         "diff",
					Source:       EvidenceAgent,
					Authority:    "reviewer",
					StoredPath:   "evidence/ev_review_1.bin",
					SHA256:       "bbbb",
					ActivationID: "act_review_1",
				},
			},
			Gates: []GateInstance{
				{
					ID:           "gate_build_1",
					GateID:       "build-gate",
					ActivationID: "act_build_1",
					Status:       GatePassed,
					Outcome:      "approve",
					Actor:        "ci",
					Reason:       "checks green",
					Source:       "ci",
					EvidenceIDs:  []EvidenceID{"ev_build_1"},
				},
				{
					ID:           "gate_review_1",
					GateID:       "review-gate",
					ActivationID: "act_review_1",
					Status:       GateActionRequired,
					Outcome:      "",
					Actor:        "human",
					Reason:       "waiting on human",
					Source:       "web",
					EvidenceIDs:  []EvidenceID{"ev_review_1"},
				},
			},
			Children: []ChildReference{
				{
					// Attached but the child has no terminal outcome yet: no
					// outcome or outputs recorded on the manifest entry.
					ID:               "cref_spawn_1",
					NodeID:           "spawn",
					ParentActivation: "act_spawn_1",
					WorkflowID:       "wf_child_golden",
					TemplateID:       "child-tmpl",
					TemplateVersion:  "1",
				},
				{
					// Resolved: the mapped outcome and the bound child output
					// reference are recorded on the manifest entry.
					ID:               "cref_spawn2_1",
					NodeID:           "spawn2",
					ParentActivation: "act_spawn2_1",
					WorkflowID:       "wf_child_golden_2",
					TemplateID:       "child-tmpl",
					TemplateVersion:  "2",
					Outcome:          "done",
					Outputs: []OutputReference{{
						WorkflowID:   "wf_child_golden_2",
						NodeID:       "build",
						ActivationID: "act_build_1",
						OutputID:     "co",
						Revision:     2,
					}},
				},
			},
		},
	}
}

// goldenDir returns the golden testdata root.
func goldenDir() string { return filepath.Join("testdata", "projections") }

// checkGolden compares got against a golden file, or writes it when -update.
func checkGolden(t *testing.T, rel, got string) {
	t.Helper()
	path := filepath.Join(goldenDir(), rel)
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with -update to generate)", path, err)
	}
	if !bytes.Equal(want, []byte(got)) {
		t.Errorf("%s mismatch:\n--- want ---\n%s\n--- got ---\n%s", rel, want, got)
	}
}

func TestProjectionWorkflowMD(t *testing.T) {
	snap := goldenSnapshot()
	checkGolden(t, "workflow.md.golden", ProjectWorkflowMD(snap))
}

func TestProjectionExecutionMD(t *testing.T) {
	snap := goldenSnapshot()
	checkGolden(t, "nodes/build/execution.md.golden", ProjectExecutionMD(snap, "build"))
	checkGolden(t, "nodes/review/execution.md.golden", ProjectExecutionMD(snap, "review"))
}

func TestProjectionReviewMD(t *testing.T) {
	snap := goldenSnapshot()
	// build's single gate -> review-1.md under build; review's gate -> review-1.md under review.
	checkGolden(t, "nodes/build/review-1.md.golden", ProjectReviewMD(snap, snap.Instance.Gates[0]))
	checkGolden(t, "nodes/review/review-1.md.golden", ProjectReviewMD(snap, snap.Instance.Gates[1]))
}

func TestProjectionWriteProjections(t *testing.T) {
	snap := goldenSnapshot()
	dir := t.TempDir()
	if err := WriteProjections(dir, snap); err != nil {
		t.Fatalf("WriteProjections: %v", err)
	}
	// Every written file must be byte-identical to its golden.
	cases := []struct {
		rel  string
		want string
	}{
		{"workflow.md", ProjectWorkflowMD(snap)},
		{"nodes/build/execution.md", ProjectExecutionMD(snap, "build")},
		{"nodes/review/execution.md", ProjectExecutionMD(snap, "review")},
		{"nodes/build/review-1.md", ProjectReviewMD(snap, snap.Instance.Gates[0])},
		{"nodes/review/review-1.md", ProjectReviewMD(snap, snap.Instance.Gates[1])},
	}
	for _, c := range cases {
		got, err := os.ReadFile(filepath.Join(dir, c.rel))
		if err != nil {
			t.Fatalf("read %s: %v", c.rel, err)
		}
		if !bytes.Equal(got, []byte(c.want)) {
			t.Errorf("%s not identical to projection", c.rel)
		}
	}
	// And to the committed goldens too.
	for _, rel := range []string{"workflow.md.golden", "nodes/build/execution.md.golden", "nodes/review/execution.md.golden"} {
		got, err := os.ReadFile(filepath.Join(dir, stripGolden(rel)))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		want, err := os.ReadFile(filepath.Join(goldenDir(), rel))
		if err != nil {
			t.Fatalf("read golden %s: %v", rel, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s differs from golden", rel)
		}
	}
}

// stripGolden turns "a/b.md.golden" into "a/b.md".
func stripGolden(rel string) string {
	if i := len(".golden"); bytes.HasSuffix([]byte(rel), []byte(".golden")) {
		return rel[:len(rel)-i]
	}
	return rel
}

func TestProjectionDeterministic(t *testing.T) {
	snap := goldenSnapshot()
	if a, b := ProjectWorkflowMD(snap), ProjectWorkflowMD(snap); a != b {
		t.Error("ProjectWorkflowMD not byte-stable across calls")
	}
	if a, b := ProjectExecutionMD(snap, "build"), ProjectExecutionMD(snap, "build"); a != b {
		t.Error("ProjectExecutionMD not byte-stable across calls")
	}
	// No wall-clock leakage: zero times never appear in any projection.
	for _, s := range []string{
		ProjectWorkflowMD(snap),
		ProjectExecutionMD(snap, "build"),
		ProjectExecutionMD(snap, "review"),
	} {
		if bytes.Contains([]byte(s), []byte("0001-01-01")) {
			t.Error("projection leaked a wall-clock value")
		}
	}
}

// TestProjectionErrorIsNonFatal proves regenerateProjections never fails a
// committed transition: it does not panic and returns nothing (void) even when
// writing fails.
func TestProjectionErrorIsNonFatal(t *testing.T) {
	snap := goldenSnapshot()
	// A store rooted at a path whose instance dir cannot be created/written:
	// point the instance dir at an existing regular file so MkdirAll fails.
	tmp := t.TempDir()
	s := New(tmp)
	// instanceDir is <root>/instances/<wf>; make <root>/instances a file so the
	// node dir cannot be created.
	instancesDir := filepath.Join(tmp, "instances")
	if err := os.WriteFile(instancesDir, []byte("x"), 0o644); err != nil {
		t.Fatalf("make instances file: %v", err)
	}
	wf := WorkflowID("wf_broken")

	// Must not panic; the void return is itself the "did not error" contract.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("regenerateProjections panicked: %v", r)
			}
		}()
		s.regenerateProjections(wf, snap)
	}()
}

// TestMdEsc locks Markdown escaping: pipe and every control/newline character
// are neutralized so rendered projections cannot inject table rows or arbitrary
// Markdown lines.
func TestMdEsc(t *testing.T) {
	got := mdEsc("a|b\nc\rd\te\x00f")
	if strings.ContainsAny(got, "\n\r\t\x00") {
		t.Fatalf("mdEsc leaked a control character: %q", got)
	}
	if !strings.Contains(got, `\|`) {
		t.Fatalf("mdEsc did not escape the pipe: %q", got)
	}
	if strings.Contains(got, "|") && !strings.Contains(got, `\|`) {
		t.Fatalf("mdEsc left an unescaped pipe: %q", got)
	}
}
