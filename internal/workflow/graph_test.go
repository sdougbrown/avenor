package workflow

import (
	"slices"
	"strings"
	"testing"
)

// validGraphTemplate returns a fully valid, typed graph template exercising
// the corrected conventions: a node's Branches map declares its branch
// outcomes (no need to also list them in Outcomes), TerminalOutcomes is the
// global vocabulary (no node needs Terminal==true), a bounded loop wraps body
// nodes with entry/checkpoint in body and exit outcomes among the terminal
// outcomes, and a single workflow-action node has configured composition
// limits with valid input/output bindings and an outcome map whose values are
// legal.
func validGraphTemplate() Template {
	return Template{
		SchemaVersion:   1,
		TemplateID:      "factory",
		TemplateVersion: "1.0.0",
		EntryNodes:      []NodeID{"start"},
		Nodes: []NodeDefinition{
			{
				ID:       "start",
				Name:     "Start",
				Action:   Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "begin"}},
				Branches: map[OutcomeName]NodeID{"ok": "work"},
				Outputs:  []OutputDefinition{{ID: "head", Name: "Head", Type: OutputString}},
			},
			{
				ID:           "work",
				Name:         "Work",
				Dependencies: []NodeID{"start"},
				Action:       Action{Kind: ActionLoop, Loop: &LoopAction{LoopFile: "work.json"}},
			},
			{
				ID:           "finish",
				Name:         "Finish",
				Dependencies: []NodeID{"work"},
				Action:       Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "wrap up"}},
			},
			{
				ID:           "review",
				Name:         "Review",
				Dependencies: []NodeID{"finish"},
				Action: Action{Kind: ActionWorkflow, Workflow: &WorkflowAction{
					TemplateID:      "child",
					TemplateVersion: "2",
					ChildKey:        "review-1",
					InputBindings:   []InputBinding{{Input: "head", From: &TemplateOutputReference{NodeID: "start", OutputID: "head"}}},
					OutputBindings:  []OutputBinding{{ChildOutput: "out", ParentOutput: "verdict"}},
					OutcomeMap:      map[OutcomeName]OutcomeName{"clean": "done"},
				}},
				Outputs: []OutputDefinition{{ID: "verdict", Name: "Verdict", Type: OutputString}},
			},
		},
		TerminalOutcomes: []OutcomeName{"done"},
		BoundedLoops: []BoundedLoopDefinition{{
			ID:           "retry",
			BodyNodes:    []NodeID{"work", "finish"},
			EntryNodeID:  "work",
			CheckpointID: "finish",
			MaximumRuns:  3,
			ExitOutcomes: []OutcomeName{"done"},
		}},
		CompositionLimits: &CompositionLimits{MaximumDepth: 4, MaximumChildren: 16},
	}
}

// mutateNode returns a copy of template with the single node identified by id
// passed to mutate. Mutations that replace slices/maps must assign the new
// slice/map to the pointer.
func mutateNode(t Template, id NodeID, mutate func(*NodeDefinition)) Template {
	t.Nodes = append([]NodeDefinition(nil), t.Nodes...)
	for i := range t.Nodes {
		if t.Nodes[i].ID != id {
			continue
		}
		n := t.Nodes[i]
		mutate(&n)
		t.Nodes[i] = n
	}
	return t
}

// mutateWorkflow passes the named workflow-action node's action to mutate,
// copying the underlying WorkflowAction so the base node is untouched.
func mutateWorkflow(t Template, id NodeID, mutate func(*WorkflowAction)) Template {
	return mutateNode(t, id, func(n *NodeDefinition) {
		if n.Action.Kind != ActionWorkflow || n.Action.Workflow == nil {
			return
		}
		w := *n.Action.Workflow
		mutate(&w)
		n.Action.Workflow = &w
	})
}

func mutateLoop(t Template, index int, mutate func(*BoundedLoopDefinition)) Template {
	t.BoundedLoops = append([]BoundedLoopDefinition(nil), t.BoundedLoops...)
	loop := t.BoundedLoops[index]
	mutate(&loop)
	t.BoundedLoops[index] = loop
	return t
}

func appendNodes(t Template, nodes ...NodeDefinition) Template {
	t.Nodes = append(append([]NodeDefinition(nil), t.Nodes...), nodes...)
	return t
}

func appendLoop(t Template, loop BoundedLoopDefinition) Template {
	t.BoundedLoops = append(t.BoundedLoops, loop)
	return t
}

func TestValidateGraph(t *testing.T) {
	tests := []struct {
		name     string
		template Template
		wantErr  string // substring of the expected error, "" for success
	}{
		{
			name:     "valid DAG with branches, bounded loop, and workflow action",
			template: validGraphTemplate(),
			wantErr:  "",
		},
		{
			name: "duplicate node id",
			template: appendNodes(validGraphTemplate(),
				NodeDefinition{ID: "start", Action: Action{Kind: ActionManual, Manual: &ManualAction{}}}),
			wantErr: "duplicate node id",
		},
		{
			name: "duplicate entry node id",
			template: func() Template {
				t := validGraphTemplate()
				t.EntryNodes = []NodeID{"start", "start"}
				return t
			}(),
			wantErr: "duplicate entry node",
		},
		{
			name: "duplicate dependency within a node",
			template: mutateNode(validGraphTemplate(), "work", func(n *NodeDefinition) {
				n.Dependencies = []NodeID{"start", "start"}
			}),
			wantErr: "duplicate dependency",
		},
		{
			name: "duplicate outcome name within a node",
			template: mutateNode(validGraphTemplate(), "start", func(n *NodeDefinition) {
				n.Outcomes = []OutcomeDefinition{
					{Name: "ok", TargetNodeID: "work"},
					{Name: "ok", TargetNodeID: "work"},
				}
			}),
			wantErr: "duplicate outcome",
		},
		{
			name: "duplicate bounded loop id",
			template: appendLoop(validGraphTemplate(),
				BoundedLoopDefinition{ID: "retry", BodyNodes: []NodeID{"work"}, EntryNodeID: "work", CheckpointID: "work", MaximumRuns: 1, ExitOutcomes: []OutcomeName{"done"}}),
			wantErr: "duplicate bounded loop id",
		},
		{
			name: "missing dependency reference",
			template: mutateNode(validGraphTemplate(), "work", func(n *NodeDefinition) {
				n.Dependencies = []NodeID{"ghost"}
			}),
			wantErr: "depends on unknown node",
		},
		{
			name: "missing entry node reference",
			template: func() Template {
				t := validGraphTemplate()
				t.EntryNodes = []NodeID{"ghost"}
				return t
			}(),
			wantErr: "entry node",
		},
		{
			name: "non-terminal outcome missing target node",
			template: mutateNode(validGraphTemplate(), "start", func(n *NodeDefinition) {
				n.Outcomes = []OutcomeDefinition{{Name: "go"}}
			}),
			wantErr: "must declare a target node",
		},
		{
			name: "terminal outcome with a target node",
			template: mutateNode(validGraphTemplate(), "start", func(n *NodeDefinition) {
				n.Outcomes = []OutcomeDefinition{{Name: "terminate", Terminal: true, TargetNodeID: "work"}}
			}),
			wantErr: "must not declare target node",
		},
		{
			name: "missing branch target",
			template: mutateNode(validGraphTemplate(), "start", func(n *NodeDefinition) {
				n.Branches = map[OutcomeName]NodeID{"ok": "ghost"}
			}),
			wantErr: "targets unknown node",
		},
		{
			name: "self dependency cycle",
			template: mutateNode(validGraphTemplate(), "work", func(n *NodeDefinition) {
				n.Dependencies = []NodeID{"work"}
			}),
			wantErr: "dependency cycle",
		},
		{
			name: "ordinary edge cycle a to b",
			template: appendNodes(validGraphTemplate(),
				NodeDefinition{ID: "a", Dependencies: []NodeID{"b"}, Action: Action{Kind: ActionManual, Manual: &ManualAction{}}},
				NodeDefinition{ID: "b", Dependencies: []NodeID{"a"}, Action: Action{Kind: ActionManual, Manual: &ManualAction{}}},
			),
			wantErr: "dependency cycle",
		},
		{
			name: "bounded loop entry not in body",
			template: mutateLoop(validGraphTemplate(), 0, func(l *BoundedLoopDefinition) {
				l.EntryNodeID = "ghost"
			}),
			wantErr: "is not a body node",
		},
		{
			name: "bounded loop checkpoint not in body",
			template: mutateLoop(validGraphTemplate(), 0, func(l *BoundedLoopDefinition) {
				l.CheckpointID = "ghost"
			}),
			wantErr: "checkpoint node",
		},
		{
			name: "bounded loop maximum iterations not positive",
			template: mutateLoop(validGraphTemplate(), 0, func(l *BoundedLoopDefinition) {
				l.MaximumRuns = 0
			}),
			wantErr: "maximum_runs > 0",
		},
		{
			name: "bounded loop exit outcome not terminal",
			template: mutateLoop(validGraphTemplate(), 0, func(l *BoundedLoopDefinition) {
				l.ExitOutcomes = []OutcomeName{"nope"}
			}),
			wantErr: "not a terminal outcome",
		},
		{
			name: "bounded loop body node not in node set",
			template: mutateLoop(validGraphTemplate(), 0, func(l *BoundedLoopDefinition) {
				l.BodyNodes = []NodeID{"work", "ghost"}
			}),
			wantErr: "does not exist",
		},
		{
			name: "workflow input binding references nonexistent node",
			template: mutateWorkflow(validGraphTemplate(), "review", func(w *WorkflowAction) {
				w.InputBindings = []InputBinding{{Input: "head", From: &TemplateOutputReference{NodeID: "ghost", OutputID: "head"}}}
			}),
			wantErr: "references unknown node",
		},
		{
			name: "workflow input binding references undeclared output",
			template: mutateWorkflow(validGraphTemplate(), "review", func(w *WorkflowAction) {
				w.InputBindings = []InputBinding{{Input: "head", From: &TemplateOutputReference{NodeID: "start", OutputID: "nope"}}}
			}),
			wantErr: "references undeclared output",
		},
		{
			name: "workflow action with nil composition limits and valid from binding",
			template: func() Template {
				t := validGraphTemplate()
				t.CompositionLimits = nil
				return t
			}(),
			wantErr: "",
		},
		{
			name: "composition limits max_depth zero",
			template: func() Template {
				t := validGraphTemplate()
				t.CompositionLimits = &CompositionLimits{MaximumDepth: 0, MaximumChildren: 16}
				return t
			}(),
			wantErr: "max_depth must be >= 1",
		},
		{
			name: "composition limits max_children zero",
			template: func() Template {
				t := validGraphTemplate()
				t.CompositionLimits = &CompositionLimits{MaximumDepth: 4, MaximumChildren: 0}
				return t
			}(),
			wantErr: "max_children must be >= 1",
		},
		{
			name: "composition limits negative max_depth",
			template: func() Template {
				t := validGraphTemplate()
				t.CompositionLimits = &CompositionLimits{MaximumDepth: -1, MaximumChildren: 16}
				return t
			}(),
			wantErr: "max_depth must be >= 1",
		},
		{
			name: "blank entry in terminal outcomes",
			template: func() Template {
				t := validGraphTemplate()
				t.TerminalOutcomes = []OutcomeName{"", "done"}
				return t
			}(),
			wantErr: "terminal outcome entry is empty",
		},
		{
			name: "duplicate terminal outcome entry",
			template: func() Template {
				t := validGraphTemplate()
				t.TerminalOutcomes = []OutcomeName{"done", "done"}
				return t
			}(),
			wantErr: "duplicate terminal outcome",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateGraph(test.template)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateGraph() error = %v, want success", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateGraph() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestDeclaredTerminalOutcomes(t *testing.T) {
	got := declaredTerminalOutcomes(Template{TerminalOutcomes: []OutcomeName{"a", "a", "b", "c"}})
	want := []OutcomeName{"a", "b", "c"}
	if !slices.Equal(got, want) {
		t.Fatalf("declaredTerminalOutcomes() = %v, want %v", got, want)
	}

	// Duplicates are dropped after the first appearance; order is preserved.
	got = declaredTerminalOutcomes(Template{TerminalOutcomes: []OutcomeName{"a", "b", "a", "c", "b"}})
	want = []OutcomeName{"a", "b", "c"}
	if !slices.Equal(got, want) {
		t.Fatalf("declaredTerminalOutcomes() = %v, want %v", got, want)
	}
}
