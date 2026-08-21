package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// in-memory template helpers for the pure composition tests.

func compositionLeaf(t *testing.T, id TemplateID) Template {
	t.Helper()
	return Template{
		SchemaVersion:   1,
		TemplateID:      id,
		TemplateVersion: "1",
		EntryNodes:      []NodeID{"start"},
		Nodes: []NodeDefinition{
			{ID: "start", Action: Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "do"}}},
		},
		TerminalOutcomes: []OutcomeName{"done"},
	}
}

// compositionWorkflowNode builds a workflow-action node composing childID@1
// under childKey with a single done->done outcome mapping.
func compositionWorkflowNode(id NodeID, childID TemplateID, childKey string) NodeDefinition {
	return NodeDefinition{
		ID: id,
		Action: Action{Kind: ActionWorkflow, Workflow: &WorkflowAction{
			TemplateID:      childID,
			TemplateVersion: "1",
			ChildKey:        childKey,
			OutcomeMap:      map[OutcomeName]OutcomeName{"done": "done"},
		}},
	}
}

// memoryResolver builds a TemplateResolver over a map keyed "id@version".
func memoryResolver(templates map[string]Template) TemplateResolver {
	return func(templateID TemplateID, templateVersion TemplateVersion) (Template, error) {
		key := string(templateID) + "@" + string(templateVersion)
		template, ok := templates[key]
		if !ok {
			return Template{}, fmt.Errorf("pinned child template %s not found", key)
		}
		return template, nil
	}
}

func TestCompositionRejectsUnresolvablePinnedVersion(t *testing.T) {
	// The parent references child@1, which is not stored at all.
	parent := Template{
		TemplateID:       "parent",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{compositionWorkflowNode("spawn", "child", "c1")},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	_, _, err := BuildComposition("wf_parent", parent, memoryResolver(map[string]Template{}))
	if err == nil {
		t.Fatal("BuildComposition with unresolvable pinned child succeeded")
	}
	if !strings.Contains(err.Error(), "pinned child template child@1 not found") {
		t.Fatalf("error = %v, want pinned-version not-found", err)
	}
}

func TestCompositionRejectsMismatchedPinnedVersion(t *testing.T) {
	// child is stored at version 1; the parent pins the non-existent version 2.
	templates := map[string]Template{"child@1": compositionLeaf(t, "child")}
	parent := Template{
		TemplateID:      "parent",
		TemplateVersion: "1",
		EntryNodes:      []NodeID{"spawn"},
		Nodes: []NodeDefinition{{
			ID: "spawn",
			Action: Action{Kind: ActionWorkflow, Workflow: &WorkflowAction{
				TemplateID:      "child",
				TemplateVersion: "2",
				ChildKey:        "c1",
				OutcomeMap:      map[OutcomeName]OutcomeName{"done": "done"},
			}},
		}},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	_, _, err := BuildComposition("wf_parent", parent, memoryResolver(templates))
	if err == nil {
		t.Fatal("BuildComposition with mismatched pinned version succeeded")
	}
	if !strings.Contains(err.Error(), "pinned child template child@2 not found") {
		t.Fatalf("error = %v, want pinned-version not-found for child@2", err)
	}
}

func TestCompositionRejectsSelfCycle(t *testing.T) {
	self := Template{
		TemplateID:       "a",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{compositionWorkflowNode("spawn", "a", "c1")},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	templates := map[string]Template{"a@1": self}
	_, _, err := BuildComposition("wf_a", self, memoryResolver(templates))
	if err == nil {
		t.Fatal("BuildComposition of self-composing template succeeded")
	}
	if !strings.Contains(err.Error(), "composition cycle") || !strings.Contains(err.Error(), "a -> a") {
		t.Fatalf("error = %v, want self-composition cycle", err)
	}
}

func TestCompositionRejectsTransitiveCycle(t *testing.T) {
	a := Template{
		TemplateID:       "a",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{compositionWorkflowNode("spawn", "b", "c1")},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	b := Template{
		TemplateID:       "b",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{compositionWorkflowNode("spawn", "a", "c1")},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	templates := map[string]Template{"a@1": a, "b@1": b}
	_, _, err := BuildComposition("wf_a", a, memoryResolver(templates))
	if err == nil {
		t.Fatal("BuildComposition of cyclic composition a->b->a succeeded")
	}
	if !strings.Contains(err.Error(), "composition cycle") || !strings.Contains(err.Error(), "a -> b -> a") {
		t.Fatalf("error = %v, want transitive cycle chain a -> b -> a", err)
	}
}

// depthChain builds a chain of composers: chain[0] composes chain[1], ... ,
// chain[n-1] composes the leaf. Each composer pins its child at version 1.
func depthChain(t *testing.T, ids []TemplateID, leaf Template, limits *CompositionLimits) map[string]Template {
	t.Helper()
	templates := map[string]Template{string(leaf.TemplateID) + "@1": leaf}
	for i, id := range ids {
		child := leaf.TemplateID
		if i+1 < len(ids) {
			child = ids[i+1]
		}
		templates[string(id)+"@1"] = Template{
			TemplateID:        id,
			TemplateVersion:   "1",
			EntryNodes:        []NodeID{"spawn"},
			Nodes:             []NodeDefinition{compositionWorkflowNode("spawn", child, "c1")},
			TerminalOutcomes:  []OutcomeName{"done"},
			CompositionLimits: limits,
		}
	}
	return templates
}

func TestCompositionDepthBoundEnforced(t *testing.T) {
	// max_depth 1: the composer may only nest leaf children, so a
	// composer->composer->leaf chain (depth 2 below the top) is rejected.
	templates := depthChain(t, []TemplateID{"a", "b"}, compositionLeaf(t, "leaf"),
		&CompositionLimits{MaximumDepth: 1, MaximumChildren: 4})
	_, _, err := BuildComposition("wf_a", templates["a@1"], memoryResolver(templates))
	if err == nil {
		t.Fatal("BuildComposition exceeded max_depth 1")
	}
	if !strings.Contains(err.Error(), "exceeding max_depth 1") {
		t.Fatalf("error = %v, want max_depth rejection", err)
	}
}

func TestCompositionDefaultDepthBoundEnforced(t *testing.T) {
	// No limits declared: a 9-level chain (depth 9) exceeds the default
	// bound (8) and must be rejected.
	ids := make([]TemplateID, 9)
	for i := range ids {
		ids[i] = TemplateID(fmt.Sprintf("t%d", i))
	}
	templates := depthChain(t, ids, compositionLeaf(t, "leaf"), nil)
	_, _, err := BuildComposition("wf_t0", templates["t0@1"], memoryResolver(templates))
	if err == nil {
		t.Fatal("BuildComposition of 9-level chain passed the default depth bound")
	}
	if !strings.Contains(err.Error(), "exceeding max_depth 8") {
		t.Fatalf("error = %v, want default max_depth rejection", err)
	}

	// An 8-level chain sits exactly at the default bound and is accepted.
	templates8 := depthChain(t, ids[:8], compositionLeaf(t, "leaf"), nil)
	if _, _, err := BuildComposition("wf_t0", templates8["t0@1"], memoryResolver(templates8)); err != nil {
		t.Fatalf("BuildComposition of 8-level chain rejected at default bound: %v", err)
	}
}

func TestCompositionChildCountBoundEnforced(t *testing.T) {
	nodes := make([]NodeDefinition, 0, 3)
	for i := 0; i < 3; i++ {
		nodes = append(nodes, compositionWorkflowNode(NodeID(fmt.Sprintf("spawn%d", i)), "leaf", fmt.Sprintf("c%d", i)))
	}
	tooMany := Template{
		TemplateID:        "parent",
		TemplateVersion:   "1",
		EntryNodes:        []NodeID{"spawn0"},
		Nodes:             nodes,
		TerminalOutcomes:  []OutcomeName{"done"},
		CompositionLimits: &CompositionLimits{MaximumDepth: 8, MaximumChildren: 2},
	}
	templates := map[string]Template{"leaf@1": compositionLeaf(t, "leaf")}
	_, _, err := BuildComposition("wf_parent", tooMany, memoryResolver(templates))
	if err == nil {
		t.Fatal("BuildComposition with 3 children passed max_children 2")
	}
	if !strings.Contains(err.Error(), "exceeding max_children 2") {
		t.Fatalf("error = %v, want max_children rejection", err)
	}
}

func TestCompositionDefaultChildCountBoundEnforced(t *testing.T) {
	nodes := make([]NodeDefinition, 0, DefaultMaximumCompositionChildren+1)
	for i := 0; i < DefaultMaximumCompositionChildren+1; i++ {
		nodes = append(nodes, compositionWorkflowNode(NodeID(fmt.Sprintf("spawn%d", i)), "leaf", fmt.Sprintf("c%d", i)))
	}
	parent := Template{
		TemplateID:       "parent",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{NodeID(fmt.Sprintf("spawn%d", DefaultMaximumCompositionChildren))},
		Nodes:            nodes,
		TerminalOutcomes: []OutcomeName{"done"},
	}
	templates := map[string]Template{"leaf@1": compositionLeaf(t, "leaf")}
	_, _, err := BuildComposition("wf_parent", parent, memoryResolver(templates))
	if err == nil {
		t.Fatal("BuildComposition above the default child bound passed")
	}
	if !strings.Contains(err.Error(), "exceeding max_children 16") {
		t.Fatalf("error = %v, want default max_children rejection", err)
	}
}

// fanoutTemplate builds a template with n workflow-action nodes, each
// composing the single child pin leafID@1, with no composition limits
// declared (so the per-template defaults apply).
func fanoutTemplate(id TemplateID, leafID TemplateID, n int) Template {
	nodes := make([]NodeDefinition, 0, n)
	for i := 0; i < n; i++ {
		nodes = append(nodes, compositionWorkflowNode(NodeID(fmt.Sprintf("spawn%d", i)), leafID, fmt.Sprintf("c%d", i)))
	}
	return Template{
		TemplateID:       id,
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{NodeID(fmt.Sprintf("spawn%d", n-1))},
		Nodes:            nodes,
		TerminalOutcomes: []OutcomeName{"done"},
	}
}

func TestCompositionGlobalInstanceBoundEnforced(t *testing.T) {
	leaf := compositionLeaf(t, "leaf")
	// A 16-wide, 3-deep fan-out respects every per-template bound (16
	// children at the default cap, depth 3 under the default 8), but
	// materializes 1 + 16 + 256 + 4096 = 4369 instances in total — past the
	// global cap — and must be rejected.
	templates := map[string]Template{
		"leaf@1": leaf,
		"c2@1":   fanoutTemplate("c2", "leaf", DefaultMaximumCompositionChildren),
		"c1@1":   fanoutTemplate("c1", "c2", DefaultMaximumCompositionChildren),
		"c0@1":   fanoutTemplate("c0", "c1", DefaultMaximumCompositionChildren),
	}
	_, _, err := BuildComposition("wf_c0", templates["c0@1"], memoryResolver(templates))
	if err == nil {
		t.Fatal("BuildComposition past the global instance bound passed")
	}
	if !strings.Contains(err.Error(), "exceeding the maximum of 4096") {
		t.Fatalf("error = %v, want global instance bound rejection", err)
	}

	// The two-level tree materializes 1 + 16 + 256 = 273 instances, under the
	// cap, and is accepted with the reported total.
	twoLevel := map[string]Template{"leaf@1": leaf, "c2@1": templates["c2@1"], "c1@1": templates["c1@1"]}
	_, instances, err := BuildComposition("wf_c1", templates["c1@1"], memoryResolver(twoLevel))
	if err != nil {
		t.Fatalf("BuildComposition under the global bound: %v", err)
	}
	if instances != 273 {
		t.Fatalf("subtree instances = %d, want 273", instances)
	}
}

func TestCompositionRejectsUndeclaredOutputBindings(t *testing.T) {
	leaf := compositionLeaf(t, "child")
	leaf.Nodes[0].Outputs = []OutputDefinition{{ID: "co", Name: "co", Type: OutputString}}

	for _, tc := range []struct {
		name      string
		binding   OutputBinding
		wantError string
	}{
		{"undeclared child output", OutputBinding{ChildOutput: "nope", ParentOutput: "po"}, "child_output \"nope\" is not a declared output"},
		{"undeclared parent output", OutputBinding{ChildOutput: "co", ParentOutput: "nope"}, "parent_output \"nope\" is not a declared output"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := compositionWorkflowNode("spawn", "child", "c1")
			node.Action.Workflow.OutputBindings = []OutputBinding{tc.binding}
			node.Outputs = []OutputDefinition{{ID: "po", Name: "po", Type: OutputString}}
			parent := Template{
				TemplateID:       "parent",
				TemplateVersion:  "1",
				EntryNodes:       []NodeID{"spawn"},
				Nodes:            []NodeDefinition{node},
				TerminalOutcomes: []OutcomeName{"done"},
			}
			templates := map[string]Template{"child@1": leaf}
			_, _, err := BuildComposition("wf_parent", parent, memoryResolver(templates))
			if err == nil {
				t.Fatalf("BuildComposition accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error = %v, want %q", err, tc.wantError)
			}
		})
	}

	// The declared pair is accepted.
	node := compositionWorkflowNode("spawn", "child", "c1")
	node.Action.Workflow.OutputBindings = []OutputBinding{{ChildOutput: "co", ParentOutput: "po"}}
	node.Outputs = []OutputDefinition{{ID: "po", Name: "po", Type: OutputString}}
	parent := Template{
		TemplateID:       "parent",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{node},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	templates := map[string]Template{"child@1": leaf}
	if _, _, err := BuildComposition("wf_parent", parent, memoryResolver(templates)); err != nil {
		t.Fatalf("BuildComposition rejected declared output binding: %v", err)
	}
}

func TestCompositionRejectsUndeclaredOutcomeMap(t *testing.T) {
	leaf := compositionLeaf(t, "child")
	// The child can produce "done" (terminal) and "skipped" (node outcome).
	leaf.Nodes[0].Outcomes = []OutcomeDefinition{{Name: "skipped", Terminal: true}}

	for _, tc := range []struct {
		name       string
		outcomeMap map[OutcomeName]OutcomeName
		wantError  string
	}{
		{"undeclared child outcome", map[OutcomeName]OutcomeName{"mystery": "done"}, "child outcome \"mystery\" is not a declared outcome"},
		{"undeclared parent outcome", map[OutcomeName]OutcomeName{"done": "mystery"}, "parent outcome \"mystery\" is not an outcome of node"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := compositionWorkflowNode("spawn", "child", "c1")
			node.Action.Workflow.OutcomeMap = tc.outcomeMap
			parent := Template{
				TemplateID:       "parent",
				TemplateVersion:  "1",
				EntryNodes:       []NodeID{"spawn"},
				Nodes:            []NodeDefinition{node},
				TerminalOutcomes: []OutcomeName{"done"},
			}
			templates := map[string]Template{"child@1": leaf}
			_, _, err := BuildComposition("wf_parent", parent, memoryResolver(templates))
			if err == nil {
				t.Fatalf("BuildComposition accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error = %v, want %q", err, tc.wantError)
			}
		})
	}

	// Declared child outcomes (terminal and node outcomes) resolve.
	node := compositionWorkflowNode("spawn", "child", "c1")
	node.Action.Workflow.OutcomeMap = map[OutcomeName]OutcomeName{"skipped": "done"}
	parent := Template{
		TemplateID:       "parent",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{node},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	templates := map[string]Template{"child@1": leaf}
	if _, _, err := BuildComposition("wf_parent", parent, memoryResolver(templates)); err != nil {
		t.Fatalf("BuildComposition rejected declared outcome map: %v", err)
	}
}

func TestCompositionBuildsValidManifest(t *testing.T) {
	templates := map[string]Template{
		"a@1": compositionLeaf(t, "a"),
		"b@1": compositionLeaf(t, "b"),
	}
	parent := Template{
		TemplateID:      "parent",
		TemplateVersion: "1",
		EntryNodes:      []NodeID{"spawnA"},
		Nodes: []NodeDefinition{
			compositionWorkflowNode("spawnA", "a", "ka"),
			compositionWorkflowNode("spawnB", "b", "kb"),
		},
		TerminalOutcomes: []OutcomeName{"done"},
	}

	comp, _, err := BuildComposition("wf_parent", parent, memoryResolver(templates))
	if err != nil {
		t.Fatalf("BuildComposition: %v", err)
	}
	if len(comp.Children) != 2 {
		t.Fatalf("manifest children = %d, want 2", len(comp.Children))
	}
	want := []struct {
		node    NodeID
		child   TemplateID
		childID WorkflowID
	}{
		{"spawnA", "a", DeriveChildWorkflowID("wf_parent", "spawnA", "ka")},
		{"spawnB", "b", DeriveChildWorkflowID("wf_parent", "spawnB", "kb")},
	}
	for i, w := range want {
		got := comp.Children[i]
		if got.NodeID != w.node {
			t.Fatalf("child[%d].node_id = %q, want %q", i, got.NodeID, w.node)
		}
		if got.ChildWorkflowID != w.childID {
			t.Fatalf("child[%d].workflow_id = %q, want %q", i, got.ChildWorkflowID, w.childID)
		}
		if got.Template.TemplateID != w.child {
			t.Fatalf("child[%d].template = %q, want %q", i, got.Template.TemplateID, w.child)
		}
	}

	// The manifest is deterministic: a second build yields identical child
	// IDs for the same parent.
	again, _, err := BuildComposition("wf_parent", parent, memoryResolver(templates))
	if err != nil {
		t.Fatalf("second BuildComposition: %v", err)
	}
	for i := range comp.Children {
		if again.Children[i].ChildWorkflowID != comp.Children[i].ChildWorkflowID {
			t.Fatalf("child[%d] id not deterministic: %q vs %q", i, again.Children[i].ChildWorkflowID, comp.Children[i].ChildWorkflowID)
		}
	}

	// A non-composing template composes nothing and never consults the
	// resolver.
	leaf := compositionLeaf(t, "leaf")
	never := func(TemplateID, TemplateVersion) (Template, error) {
		t.Fatal("resolver consulted for a leaf template")
		return Template{}, nil
	}
	comp, _, err = BuildComposition("wf_leaf", leaf, never)
	if err != nil {
		t.Fatalf("BuildComposition of leaf: %v", err)
	}
	if len(comp.Children) != 0 {
		t.Fatalf("leaf manifest children = %d, want 0", len(comp.Children))
	}
}

// compositionManagerFixture stores a leaf child and a parent composing one
// child under a deterministic derived ID, and returns them.
func compositionManagerFixture(t *testing.T) (*Manager, *Store, Template) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	leaf := compositionLeaf(t, "comp-child")
	parent := Template{
		SchemaVersion:    1,
		TemplateID:       "comp-parent",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{compositionWorkflowNode("spawn", "comp-child", "c1")},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	for _, template := range []Template{leaf, parent} {
		if err := s.StoreTemplate(template.TemplateID, template.TemplateVersion, template); err != nil {
			t.Fatalf("StoreTemplate %s: %v", template.TemplateID, err)
		}
	}
	return m, s, parent
}

func TestCompositionInstantiateMaterializesManifestDurably(t *testing.T) {
	m, _, _ := compositionManagerFixture(t)
	payload, _ := json.Marshal(map[string]string{
		"template_id": "comp-parent", "template_version": "1",
	})
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := WorkflowID(out.(map[string]any)["workflow_id"].(string))

	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent: exists=%v err=%v", exists, err)
	}
	if len(snap.Instance.Children) != 1 {
		t.Fatalf("instance children = %d, want 1", len(snap.Instance.Children))
	}
	ref := snap.Instance.Children[0]
	wantChild := DeriveChildWorkflowID(wf, "spawn", "c1")
	if ref.NodeID != "spawn" || ref.WorkflowID != wantChild {
		t.Fatalf("child reference = %+v, want node spawn / workflow %q", ref, wantChild)
	}
	if ref.TemplateID != "comp-child" || ref.TemplateVersion != "1" {
		t.Fatalf("child template pin = %s@%s, want comp-child@1", ref.TemplateID, ref.TemplateVersion)
	}
	// The child instance itself was created.
	if _, exists, err := m.store.loadCurrent(wantChild); err != nil || !exists {
		t.Fatalf("child instance: exists=%v err=%v", exists, err)
	}
}

func TestCompositionChildCreationIsIdempotent(t *testing.T) {
	m, s, parent := compositionManagerFixture(t)
	payload, _ := json.Marshal(map[string]string{
		"template_id": "comp-parent", "template_version": "1",
	})
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := WorkflowID(out.(map[string]any)["workflow_id"].(string))

	countInstances := func() int {
		entries, err := os.ReadDir(filepath.Join(s.Root(), "instances"))
		if err != nil {
			t.Fatalf("read instances dir: %v", err)
		}
		return len(entries)
	}
	before := countInstances()
	if before != 2 {
		t.Fatalf("instance count after instantiate = %d, want 2 (parent + child)", before)
	}

	// Replaying the composition for the same parent must reuse the existing
	// child under its derived ID, never create a duplicate.
	comp, _, err := BuildComposition(wf, parent, m.resolveTemplate)
	if err != nil {
		t.Fatalf("BuildComposition on replay: %v", err)
	}
	if len(comp.Children) != 1 {
		t.Fatalf("replay manifest children = %d, want 1", len(comp.Children))
	}
	childID, err := m.ensureChildInstance(comp.Children[0], nil)
	if err != nil {
		t.Fatalf("replayed ensureChildInstance: %v", err)
	}
	if childID != DeriveChildWorkflowID(wf, "spawn", "c1") {
		t.Fatalf("replayed child id = %q, want derived id", childID)
	}
	if after := countInstances(); after != before {
		t.Fatalf("instance count after replay = %d, want %d (no duplicate children)", after, before)
	}
}

func TestCompositionPartialFailureLeavesOrphans(t *testing.T) {
	// Pins the current orphan behavior: when a later child fails to
	// materialize, the already-created children remain on disk as active,
	// unparented workflows and only the error is surfaced. Orphan cleanup is
	// out of scope for this stage (see WorkflowInstantiate).
	//
	// The failure is driven through the same ensureChildInstance path the
	// manager's materialization loop uses: "broken" composes a grandchild that
	// is never stored, so materializing it fails inside its own
	// BuildComposition. (At the public API the parent's own BuildComposition
	// rejects this tree up front, before any child is materialized, since it
	// validates the whole descendant tree; materialization-stage failures such
	// as a failing parent instantiate command or a concurrent creator reach
	// this same per-child path.)
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)

	ok := compositionLeaf(t, "orphan-ok")
	if err := s.StoreTemplate(ok.TemplateID, ok.TemplateVersion, ok); err != nil {
		t.Fatalf("StoreTemplate: %v", err)
	}
	broken := Template{
		TemplateID:       "orphan-broken",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{compositionWorkflowNode("spawn", "never-stored", "g1")},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	parent := WorkflowID("wf_orphan_parent")
	children := []CompositionChild{
		{NodeID: "spawn0", ChildWorkflowID: DeriveChildWorkflowID(parent, "spawn0", "c0"), Template: ok},
		{NodeID: "spawn1", ChildWorkflowID: DeriveChildWorkflowID(parent, "spawn1", "c1"), Template: broken},
	}

	if _, err := m.ensureChildInstance(children[0], nil); err != nil {
		t.Fatalf("first child: %v", err)
	}
	if _, err := m.ensureChildInstance(children[1], nil); err == nil {
		t.Fatal("second child materialized, want failure on unresolvable pinned grandchild")
	} else if !strings.Contains(err.Error(), "pinned child template never-stored@1 not found") {
		t.Fatalf("error = %v, want pinned-version rejection", err)
	}

	// The first child is left on disk as an active, unparented workflow, and
	// the parent was never committed.
	snap, exists, err := m.store.loadCurrent(children[0].ChildWorkflowID)
	if err != nil || !exists {
		t.Fatalf("orphan child: exists=%v err=%v", exists, err)
	}
	if snap.Instance.Status != WorkflowActive {
		t.Fatalf("orphan child status = %v, want active", snap.Instance.Status)
	}
	if _, exists, err := m.store.loadCurrent(parent); err != nil || exists {
		t.Fatalf("parent instance: exists=%v err=%v, want uncommitted", exists, err)
	}
}

// ---------------------------------------------------------------------------
// Phase 2: the local workflow executor (attach -> awaiting_child -> outcome)
// ---------------------------------------------------------------------------

// compositionPhase2Templates builds the child leaf template (a single manual
// node; terminal outcomes done and failed) and a parent composing it under
// child_key "c1" with the given outcome map.
func compositionPhase2Templates(t *testing.T, outcomeMap map[OutcomeName]OutcomeName) (parent, child Template) {
	t.Helper()
	child = Template{
		SchemaVersion:    1,
		TemplateID:       "p2-child",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"start"},
		Nodes:            []NodeDefinition{{ID: "start", Action: Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "do"}}}},
		TerminalOutcomes: []OutcomeName{"done", "failed"},
	}
	node := compositionWorkflowNode("spawn", "p2-child", "c1")
	node.Action.Workflow.OutcomeMap = outcomeMap
	parent = Template{
		SchemaVersion:    1,
		TemplateID:       "p2-parent",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{node},
		TerminalOutcomes: []OutcomeName{"done", "failed"},
	}
	return parent, child
}

// compositionPhase2Fixture stores the templates, instantiates the parent, and
// returns (manager, store, parent wf, child wf). The manager's default
// workflow executor is in place; tests that need a bounded wait re-register
// one.
func compositionPhase2Fixture(t *testing.T, outcomeMap map[OutcomeName]OutcomeName) (*Manager, *Store, WorkflowID, WorkflowID) {
	t.Helper()
	parent, child := compositionPhase2Templates(t, outcomeMap)
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	for _, template := range []Template{child, parent} {
		if err := s.StoreTemplate(template.TemplateID, template.TemplateVersion, template); err != nil {
			t.Fatalf("StoreTemplate %s: %v", template.TemplateID, err)
		}
	}
	payload, _ := json.Marshal(map[string]string{"template_id": "p2-parent", "template_version": "1"})
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := WorkflowID(out.(map[string]any)["workflow_id"].(string))
	return m, s, wf, DeriveChildWorkflowID(wf, "spawn", "c1")
}

// registerBoundedWorkflowExecutor replaces the manager's workflow executor
// with one whose wait is bounded so a failure cannot hang the suite.
func registerBoundedWorkflowExecutor(t *testing.T, m *Manager, maxWait time.Duration) {
	t.Helper()
	m.RegisterExecutor(ActionWorkflow, &workflowExecutor{manager: m, maxWait: maxWait})
}

// claimStartSpawn claims the parent's spawn activation and returns the start
// command payload plus the claim result.
func claimStartSpawn(t *testing.T, m *Manager, s *Store, wf WorkflowID) (json.RawMessage, map[string]any) {
	t.Helper()
	snap, exists, err := s.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent: exists=%v err=%v", exists, err)
	}
	act := activationByNode(&snap.Instance, "spawn")
	if act == nil {
		t.Fatalf("spawn activation not found")
	}
	res := claimActivation(t, m, wf, "spawn", string(act.ID), "alice")
	return startCommandPayload(t, "spawn", string(act.ID), res, nil), res
}

// waitParentAwaitingChild polls the parent until its spawn activation reaches
// the durable awaiting_child status (the composition executor's attach point)
// or the deadline elapses.
func waitParentAwaitingChild(s *Store, wf WorkflowID, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		snap, exists, err := s.loadCurrent(wf)
		if err != nil {
			return err
		}
		if exists {
			for i := range snap.Instance.Activations {
				a := snap.Instance.Activations[i]
				if a.NodeID == "spawn" && a.Status == ActivationAwaitingChild {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("parent spawn activation never reached awaiting_child within %s", timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// driveChildTerminalErr drives a single-node manual child instance to a
// terminal completion with outcome (and optional outputs) directly through
// store commands — no provider, no campaign. It errors instead of failing so
// it is safe from a helper goroutine.
func driveChildTerminalErr(s *Store, wf WorkflowID, outcome string, outputs []OutputValue) error {
	snap, exists, err := s.loadCurrent(wf)
	if err != nil {
		return err
	}
	if !exists || len(snap.Instance.Activations) == 0 {
		return fmt.Errorf("child workflow %s has no activation (exists=%v)", wf, exists)
	}
	childAct := snap.Instance.Activations[0].ID
	identity := ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: childAct}
	snap, err = s.ApplyCommand(wf, Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "c-claim",
		Identity:         identity,
		LeaseID:          "c-lease-1",
		Actor:            "alice",
	})
	if err != nil {
		return fmt.Errorf("child claim: %w", err)
	}
	snap, err = s.ApplyCommand(wf, Command{
		Kind:             CommandStart,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "c-start",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: childAct, AttemptID: "c-att-1"},
		LeaseID:          "c-lease-1",
	})
	if err != nil {
		return fmt.Errorf("child start: %w", err)
	}
	snap, err = s.ApplyCommand(wf, Command{
		Kind:             CommandComplete,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "c-complete",
		Identity:         identity,
		LeaseID:          "c-lease-1",
		Outcome:          OutcomeName(outcome),
		Outputs:          outputs,
	})
	if err != nil {
		return fmt.Errorf("child complete: %w", err)
	}
	if !isTerminalStatus(snap.Instance.Status) {
		return fmt.Errorf("child status = %s, want terminal", snap.Instance.Status)
	}
	return nil
}

// driveChildTerminal is driveChildTerminalErr that fails the test on error
// (main goroutine only).
func driveChildTerminal(t *testing.T, s *Store, wf WorkflowID, outcome string, outputs []OutputValue) {
	t.Helper()
	if err := driveChildTerminalErr(s, wf, outcome, outputs); err != nil {
		t.Fatalf("drive child terminal: %v", err)
	}
}

// countInstances counts the materialized workflow instances on disk.
func countInstances(t *testing.T, s *Store) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.Root(), "instances"))
	if err != nil {
		t.Fatalf("read instances dir: %v", err)
	}
	return len(entries)
}

// workflowEventKinds returns the set of event kinds in the instance's log.
func workflowEventKinds(t *testing.T, m *Manager, wf WorkflowID) map[string]bool {
	t.Helper()
	out, err := m.WorkflowEvents(string(wf), 0, 1000)
	if err != nil {
		t.Fatalf("WorkflowEvents: %v", err)
	}
	kinds := map[string]bool{}
	events := out.(map[string]any)["events"].([]map[string]any)
	for _, e := range events {
		kinds[e["kind"].(string)] = true
	}
	return kinds
}

func TestWorkflowStartAttachesAndResolvesChildOutcome(t *testing.T) {
	m, s, wf, childID := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done"})
	registerBoundedWorkflowExecutor(t, m, 10*time.Second)
	payload, res := claimStartSpawn(t, m, s, wf)
	leaseID, _ := res["lease_id"].(string)

	// The child is driven to terminal only after the durable attach is
	// visible in the parent's snapshot: this asserts the await is real and
	// durable, and exercises the poll loop.
	driveErr := make(chan error, 1)
	go func() {
		if err := waitParentAwaitingChild(s, wf, 10*time.Second); err != nil {
			driveErr <- err
			return
		}
		driveErr <- driveChildTerminalErr(s, childID, "done", nil)
	}()

	if _, err := m.WorkflowCommand(string(wf), payload); err != nil {
		t.Fatalf("start: %v", err)
	}
	if e := <-driveErr; e != nil {
		t.Fatalf("child driver: %v", e)
	}

	snap, exists, err := s.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent: exists=%v err=%v", exists, err)
	}
	act := activationByNode(&snap.Instance, "spawn")
	if act.Status != ActivationSatisfied {
		t.Fatalf("spawn status = %s, want satisfied", act.Status)
	}
	if act.SelectedOutcome != "done" {
		t.Fatalf("spawn selected outcome = %q, want done", act.SelectedOutcome)
	}
	if act.ActiveLease != nil {
		t.Fatalf("spawn lease still held after resolution")
	}
	if snap.Instance.Status != WorkflowCompleted || snap.Instance.TerminalOutcome != "done" {
		t.Fatalf("workflow status = %s/%s, want completed/done", snap.Instance.Status, snap.Instance.TerminalOutcome)
	}
	// The child reference carries identity + mapped outcome only.
	if len(snap.Instance.Children) != 1 {
		t.Fatalf("instance children = %d, want 1", len(snap.Instance.Children))
	}
	ref := snap.Instance.Children[0]
	if ref.ParentActivation != act.ID || ref.WorkflowID != childID || ref.Outcome != "done" {
		t.Fatalf("child reference = %+v, want attached to %s / child %s / outcome done", ref, act.ID, childID)
	}
	// No child state is copied into the parent.
	if len(snap.Instance.Outputs) != 0 {
		t.Fatalf("parent instance outputs = %v, want none (no child state copied)", snap.Instance.Outputs)
	}
	// The kernel-owned composition attempt terminates with the resolution.
	if len(snap.Instance.Attempts) != 1 || snap.Instance.Attempts[0].Status != AttemptSucceeded {
		t.Fatalf("attempts = %+v, want one succeeded kernel attempt", snap.Instance.Attempts)
	}
	// No duplicate child instance was created by the executor path.
	if n := countInstances(t, s); n != 2 {
		t.Fatalf("instance count = %d, want 2 (parent + child)", n)
	}
	// The attach and outcome are durable events in the parent's log.
	kinds := workflowEventKinds(t, m, wf)
	if !kinds[string(EventChildAttached)] || !kinds[string(EventChildOutcome)] {
		t.Fatalf("event kinds = %v, want child_attached and child_outcome", kinds)
	}
	_ = leaseID
}

func TestWorkflowStartResolvesAlreadyTerminalChild(t *testing.T) {
	m, s, wf, childID := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done", "failed": "failed"})
	registerBoundedWorkflowExecutor(t, m, 5*time.Second)
	// The child reaches terminal BEFORE the parent's workflow node starts:
	// the attach must resolve immediately, not double-await.
	driveChildTerminal(t, s, childID, "done", nil)

	payload, _ := claimStartSpawn(t, m, s, wf)
	if _, err := m.WorkflowCommand(string(wf), payload); err != nil {
		t.Fatalf("start: %v", err)
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	act := activationByNode(&snap.Instance, "spawn")
	if act.Status != ActivationSatisfied || act.SelectedOutcome != "done" {
		t.Fatalf("spawn = %s/%s, want satisfied/done", act.Status, act.SelectedOutcome)
	}
	if snap.Instance.Status != WorkflowCompleted {
		t.Fatalf("workflow status = %s, want completed", snap.Instance.Status)
	}
	if ref := snap.Instance.Children[0]; ref.Outcome != "done" || ref.ParentActivation != act.ID {
		t.Fatalf("child reference = %+v, want outcome done attached to %s", ref, act.ID)
	}
}

func TestWorkflowChildUnmappedOutcomeIsSafe(t *testing.T) {
	// "failed" is a declared terminal outcome of the child template but the
	// parent's outcome_map does not map it: the executor must fail safely,
	// never fabricate a parent outcome, and leave the parent durably in
	// awaiting_child (resumable).
	m, s, wf, childID := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done"})
	registerBoundedWorkflowExecutor(t, m, 5*time.Second)
	driveChildTerminal(t, s, childID, "failed", nil)

	payload, _ := claimStartSpawn(t, m, s, wf)
	_, err := m.WorkflowCommand(string(wf), payload)
	if err == nil || !strings.Contains(err.Error(), "not mapped") {
		t.Fatalf("start err = %v, want unmapped-outcome rejection", err)
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	act := activationByNode(&snap.Instance, "spawn")
	if act.Status != ActivationAwaitingChild {
		t.Fatalf("spawn status = %s, want awaiting_child (durable, resumable)", act.Status)
	}
	if ref := snap.Instance.Children[0]; ref.Outcome != "" || len(ref.Outputs) != 0 {
		t.Fatalf("child reference = %+v, want no outcome/outputs recorded", ref)
	}
	if snap.Instance.Status != WorkflowActive {
		t.Fatalf("workflow status = %s, want active", snap.Instance.Status)
	}
	// Nothing resolved, so the instance is still fully readable (no log
	// corruption).
	if _, err := m.WorkflowStatus(string(wf)); err != nil {
		t.Fatalf("WorkflowStatus after safe failure: %v", err)
	}
}

func TestWorkflowChildTypedOutputsMapped(t *testing.T) {
	// The child produces a typed output; the parent binds it by identity
	// only (no value copied).
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	child := Template{
		SchemaVersion:   1,
		TemplateID:      "p2-child",
		TemplateVersion: "1",
		EntryNodes:      []NodeID{"start"},
		Nodes: []NodeDefinition{{
			ID:      "start",
			Action:  Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "do"}},
			Outputs: []OutputDefinition{{ID: "co", Name: "co", Type: OutputString}},
		}},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	parentNode := compositionWorkflowNode("spawn", "p2-child", "c1")
	parentNode.Outputs = []OutputDefinition{{ID: "po", Name: "po", Type: OutputString}}
	parentNode.Action.Workflow.OutputBindings = []OutputBinding{{ChildOutput: "co", ParentOutput: "po"}}
	parent := Template{
		SchemaVersion:    1,
		TemplateID:       "p2-parent",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{parentNode},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	for _, template := range []Template{child, parent} {
		if err := s.StoreTemplate(template.TemplateID, template.TemplateVersion, template); err != nil {
			t.Fatalf("StoreTemplate %s: %v", template.TemplateID, err)
		}
	}
	payload, _ := json.Marshal(map[string]string{"template_id": "p2-parent", "template_version": "1"})
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := WorkflowID(out.(map[string]any)["workflow_id"].(string))
	childID := DeriveChildWorkflowID(wf, "spawn", "c1")
	registerBoundedWorkflowExecutor(t, m, 10*time.Second)

	childSnap, exists, err := s.loadCurrent(childID)
	if err != nil || !exists || len(childSnap.Instance.Activations) == 0 {
		t.Fatalf("child loadCurrent: exists=%v err=%v", exists, err)
	}
	childActID := childSnap.Instance.Activations[0].ID

	driveErr := make(chan error, 1)
	go func() {
		if err := waitParentAwaitingChild(s, wf, 10*time.Second); err != nil {
			driveErr <- err
			return
		}
		driveErr <- driveChildTerminalErr(s, childID, "done", []OutputValue{{
			ID:           "ov1",
			DefinitionID: "co",
			ActivationID: childActID,
			Revision:     1,
			Value:        []byte(`"result"`),
		}})
	}()

	parentPayload, res := claimStartSpawn(t, m, s, wf)
	if _, err := m.WorkflowCommand(string(wf), parentPayload); err != nil {
		t.Fatalf("start: %v", err)
	}
	if e := <-driveErr; e != nil {
		t.Fatalf("child driver: %v", e)
	}
	_ = res

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	ref := snap.Instance.Children[0]
	wantRefs := []OutputReference{{
		WorkflowID:   childID,
		NodeID:       "start",
		ActivationID: childActID,
		OutputID:     "co",
		Revision:     1,
	}}
	if len(ref.Outputs) != 1 || ref.Outputs[0] != wantRefs[0] {
		t.Fatalf("child reference outputs = %+v, want %+v", ref.Outputs, wantRefs)
	}
	// The output VALUE stays in the child; the parent holds identity only.
	if len(snap.Instance.Outputs) != 0 {
		t.Fatalf("parent instance outputs = %v, want none", snap.Instance.Outputs)
	}
}

func TestWorkflowChildOutcomeBranchesToTarget(t *testing.T) {
	// A non-terminal mapped parent outcome (branch key) creates the target
	// activation instead of completing the workflow.
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	child := Template{
		SchemaVersion:    1,
		TemplateID:       "p2-child",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"start"},
		Nodes:            []NodeDefinition{{ID: "start", Action: Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "do"}}}},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	spawn := compositionWorkflowNode("spawn", "p2-child", "c1")
	spawn.Action.Workflow.OutcomeMap = map[OutcomeName]OutcomeName{"done": "next"}
	spawn.Branches = map[OutcomeName]NodeID{"next": "after"}
	parent := Template{
		SchemaVersion:   1,
		TemplateID:      "p2-branch-parent",
		TemplateVersion: "1",
		EntryNodes:      []NodeID{"spawn"},
		Nodes: []NodeDefinition{
			spawn,
			{ID: "after", Action: Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "next"}}},
		},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	for _, template := range []Template{child, parent} {
		if err := s.StoreTemplate(template.TemplateID, template.TemplateVersion, template); err != nil {
			t.Fatalf("StoreTemplate %s: %v", template.TemplateID, err)
		}
	}
	payload, _ := json.Marshal(map[string]string{"template_id": "p2-branch-parent", "template_version": "1"})
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := WorkflowID(out.(map[string]any)["workflow_id"].(string))
	childID := DeriveChildWorkflowID(wf, "spawn", "c1")
	registerBoundedWorkflowExecutor(t, m, 10*time.Second)

	driveErr := make(chan error, 1)
	go func() {
		if err := waitParentAwaitingChild(s, wf, 10*time.Second); err != nil {
			driveErr <- err
			return
		}
		driveErr <- driveChildTerminalErr(s, childID, "done", nil)
	}()

	parentPayload, res := claimStartSpawn(t, m, s, wf)
	if _, err := m.WorkflowCommand(string(wf), parentPayload); err != nil {
		t.Fatalf("start: %v", err)
	}
	if e := <-driveErr; e != nil {
		t.Fatalf("child driver: %v", e)
	}
	_ = res

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	act := activationByNode(&snap.Instance, "spawn")
	if act.Status != ActivationSatisfied || act.SelectedOutcome != "next" {
		t.Fatalf("spawn = %s/%s, want satisfied/next", act.Status, act.SelectedOutcome)
	}
	if snap.Instance.Status != WorkflowActive {
		t.Fatalf("workflow status = %s, want active (branched, not terminal)", snap.Instance.Status)
	}
	after := activationByNode(&snap.Instance, "after")
	if after == nil || after.Status != ActivationPending || after.IncomingOutcome != "next" {
		t.Fatalf("after activation = %+v, want pending with incoming next", after)
	}
	if ref := snap.Instance.Children[0]; ref.Outcome != "next" {
		t.Fatalf("child reference outcome = %q, want next", ref.Outcome)
	}
}

func TestWorkflowExecutorRedispatchIsIdempotent(t *testing.T) {
	// Re-dispatching the same attempt (resume semantics) is a no-op: the
	// attach idempotency key skips the attach, and the satisfied activation
	// makes the outcome resolution a no-op.
	m, s, wf, childID := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done"})
	registerBoundedWorkflowExecutor(t, m, 5*time.Second)
	driveChildTerminal(t, s, childID, "done", nil)

	payload, res := claimStartSpawn(t, m, s, wf)
	out, err := m.WorkflowCommand(string(wf), payload)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("start result = %#v, want map", out)
	}
	attemptID := mm["attempt_id"].(string)

	before, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	exec, ok := m.executor(ActionWorkflow).(*workflowExecutor)
	if !ok {
		t.Fatalf("workflow executor = %T, want *workflowExecutor", m.executor(ActionWorkflow))
	}
	err = exec.Dispatch(context.Background(), ExecutorContext{
		WorkflowID:   wf,
		NodeID:       "spawn",
		ActivationID: activationByNode(&before.Instance, "spawn").ID,
		AttemptID:    AttemptID(attemptID),
		LeaseID:      LeaseID(res["lease_id"].(string)),
		Action:       Action{Kind: ActionWorkflow, Workflow: &WorkflowAction{TemplateID: "p2-child", TemplateVersion: "1", ChildKey: "c1", OutcomeMap: map[OutcomeName]OutcomeName{"done": "done"}}},
	})
	if err != nil {
		t.Fatalf("re-dispatch: %v", err)
	}
	after, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after re-dispatch: %v", err)
	}
	if before.Instance.Revision != after.Instance.Revision {
		t.Fatalf("re-dispatch advanced revision %d -> %d, want no-op", before.Instance.Revision, after.Instance.Revision)
	}
	if after.Instance.Status != WorkflowCompleted {
		t.Fatalf("workflow status = %s, want completed (unchanged)", after.Instance.Status)
	}
}

// startSpawnDirect claims, starts, and attaches the parent's spawn activation
// directly through store commands (no executor), using the executor's own
// idempotency keys for the attach so a later re-dispatch of the same attempt
// dedupes against it. It returns the activation ID.
func startSpawnDirect(t *testing.T, s *Store, wf WorkflowID, lease LeaseID, attempt AttemptID) ActivationID {
	t.Helper()
	identity := ExecutionIdentity{WorkflowID: wf, NodeID: "spawn"}
	snap, exists, err := s.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent: exists=%v err=%v", exists, err)
	}
	actID := activationByNode(&snap.Instance, "spawn").ID
	identity.ActivationID = actID
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim-" + string(attempt),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "spawn", ActivationID: actID},
		LeaseID:          lease,
		Actor:            "alice",
	}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after claim: %v", err)
	}
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandStart,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "start-" + string(attempt),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "spawn", ActivationID: actID, AttemptID: attempt},
		LeaseID:          lease,
	}); err != nil {
		t.Fatalf("start: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after start: %v", err)
	}
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandChildAttach,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "child-attach-" + string(attempt),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "spawn", ActivationID: actID, AttemptID: attempt},
		LeaseID:          lease,
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	return actID
}

// applyChildOutcomeDirect resolves the spawn activation through a direct
// child-outcome command (no executor).
func applyChildOutcomeDirect(t *testing.T, s *Store, wf WorkflowID, actID ActivationID, lease LeaseID, attempt AttemptID, outcome string) {
	t.Helper()
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandChildOutcome,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "child-outcome-" + string(attempt),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "spawn", ActivationID: actID, AttemptID: attempt},
		LeaseID:          lease,
		Outcome:          OutcomeName(outcome),
	}); err != nil {
		t.Fatalf("child outcome: %v", err)
	}
}

func phase2ReDispatchContext(wf WorkflowID, actID ActivationID, attempt AttemptID, lease LeaseID) ExecutorContext {
	return ExecutorContext{
		WorkflowID:   wf,
		NodeID:       "spawn",
		ActivationID: actID,
		AttemptID:    attempt,
		LeaseID:      lease,
		Action: Action{Kind: ActionWorkflow, Workflow: &WorkflowAction{
			TemplateID: "p2-child", TemplateVersion: "1", ChildKey: "c1",
			OutcomeMap: map[OutcomeName]OutcomeName{"done": "done"},
		}},
	}
}

func phase2AttemptStatus(t *testing.T, s *Store, wf WorkflowID, attempt AttemptID) AttemptStatus {
	t.Helper()
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	for i := range snap.Instance.Attempts {
		if snap.Instance.Attempts[i].ID == attempt {
			return snap.Instance.Attempts[i].Status
		}
	}
	t.Fatalf("attempt %s not found in instance attempts", attempt)
	return ""
}

func TestWorkflowExecutorRedispatchTerminatesUnrecordedAttempt(t *testing.T) {
	// Crash-window recovery: the child_outcome event landed but the process
	// died before the kernel attempt termination was recorded, leaving the
	// attempt stuck "starting" forever. A re-dispatch of the same attempt
	// must record the termination (succeeded) and change nothing else.
	m, s, wf, childID := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done"})
	registerBoundedWorkflowExecutor(t, m, 5*time.Second)
	driveChildTerminal(t, s, childID, "done", nil)

	const attempt = AttemptID("att-crash")
	actID := startSpawnDirect(t, s, wf, "crash-lease", attempt)
	applyChildOutcomeDirect(t, s, wf, actID, "crash-lease", attempt, "done")

	if got := phase2AttemptStatus(t, s, wf, attempt); got != AttemptStarting {
		t.Fatalf("attempt status before re-dispatch = %q, want starting (termination never recorded)", got)
	}

	exec, ok := m.executor(ActionWorkflow).(*workflowExecutor)
	if !ok {
		t.Fatalf("workflow executor = %T, want *workflowExecutor", m.executor(ActionWorkflow))
	}
	if err := exec.Dispatch(context.Background(), phase2ReDispatchContext(wf, actID, attempt, "crash-lease")); err != nil {
		t.Fatalf("re-dispatch: %v", err)
	}
	if got := phase2AttemptStatus(t, s, wf, attempt); got != AttemptSucceeded {
		t.Fatalf("attempt status after re-dispatch = %q, want succeeded", got)
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	if snap.Instance.Status != WorkflowCompleted || snap.Instance.TerminalOutcome != "done" {
		t.Fatalf("workflow = %s/%s, want completed/done", snap.Instance.Status, snap.Instance.TerminalOutcome)
	}
}

func TestWorkflowExecutorStopsWaitingOnceParentLeavesAwaitingChild(t *testing.T) {
	// Parent-side check in the poll loop: once the activation has left
	// awaiting_child, a re-dispatch must stop waiting for the (still
	// running) child instead of polling to the bound. The bound is set long
	// so a missing check would fail rather than pass.
	m, s, wf, _ := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done"})
	registerBoundedWorkflowExecutor(t, m, 30*time.Second)

	const attempt = AttemptID("att-parent-check")
	actID := startSpawnDirect(t, s, wf, "pc-lease", attempt)
	// Resolve the activation while the child is still non-terminal.
	applyChildOutcomeDirect(t, s, wf, actID, "pc-lease", attempt, "done")

	exec, ok := m.executor(ActionWorkflow).(*workflowExecutor)
	if !ok {
		t.Fatalf("workflow executor = %T, want *workflowExecutor", m.executor(ActionWorkflow))
	}
	start := time.Now()
	if err := exec.Dispatch(context.Background(), phase2ReDispatchContext(wf, actID, attempt, "pc-lease")); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("dispatch took %s, want fast return after the parent left awaiting_child", elapsed)
	}
}
