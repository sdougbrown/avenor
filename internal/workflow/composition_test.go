package workflow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
