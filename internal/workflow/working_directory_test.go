package workflow

// working_directory_test.go covers the per-attempt working-directory
// declaration: template-side validation (the only declared value form is
// {"from_instance_param": "<param id>"} naming a declared string param),
// instantiate-time path rules for a param value that feeds a
// working-directory declaration (absolute, clean), and dispatch-time
// resolution (node override wins over the template default, unset falls back
// to the supervisor's working directory).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// workingDirTemplateJSON builds a template declaring the given
// working_directory forms (nil omits the field) plus one required param.
func workingDirTemplateJSON(t *testing.T, templateID string, templateLevel, nodeLevel any) []byte {
	t.Helper()
	tmpl := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"params":           []any{map[string]any{"id": "worktree_path", "type": "string", "required": true}},
		"nodes": []any{
			map[string]any{
				"id":     "start",
				"action": map[string]any{"type": "run", "prompt": "do the thing"},
			},
		},
		"terminal_outcomes": []string{"done"},
	}
	if templateLevel != nil {
		tmpl["working_directory"] = templateLevel
	}
	if nodeLevel != nil {
		tmpl["nodes"].([]any)[0].(map[string]any)["working_directory"] = nodeLevel
	}
	data, err := json.Marshal(tmpl)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func TestValidateWorkingDirectoryDeclarations(t *testing.T) {
	tests := []struct {
		name          string
		templateLevel any
		nodeLevel     any
		wantErr       string
	}{
		{
			name:          "template default naming a declared param",
			templateLevel: map[string]any{"from_instance_param": "worktree_path"},
		},
		{
			name:      "node override naming a declared param",
			nodeLevel: map[string]any{"from_instance_param": "worktree_path"},
		},
		{
			name:          "template default and node override together",
			templateLevel: map[string]any{"from_instance_param": "worktree_path"},
			nodeLevel:     map[string]any{"from_instance_param": "worktree_path"},
		},
		{
			name:          "literal template-level path",
			templateLevel: "/absolute/worktree",
			wantErr:       "literal path is not a declared form",
		},
		{
			name:      "literal node-level path",
			nodeLevel: "/absolute/worktree",
			wantErr:   "literal path is not a declared form",
		},
		{
			name:          "undeclared param",
			templateLevel: map[string]any{"from_instance_param": "nope"},
			wantErr:       "undeclared instance param",
		},
		{
			name:      "undeclared param on a node",
			nodeLevel: map[string]any{"from_instance_param": "nope"},
			wantErr:   "undeclared instance param",
		},
		{
			name:          "blank from_instance_param",
			templateLevel: map[string]any{"from_instance_param": ""},
			wantErr:       "minLength",
		},
		{
			name:          "empty object",
			templateLevel: map[string]any{},
			wantErr:       "required at /working_directory",
		},
		{
			name:          "unknown field",
			templateLevel: map[string]any{"from_instance_param": "worktree_path", "path": "/x"},
			wantErr:       "unknown field",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTemplateJSON(workingDirTemplateJSON(t, "wd-decl", tc.templateLevel, tc.nodeLevel))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTemplateJSON() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateTemplateJSON() = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestResolveNodeWorkingDirectory proves the resolution rule once: the node
// override wins over the template default, the template default applies when
// the node declares none, and an unset declaration resolves to empty — the
// supervisor's working directory.
func TestResolveNodeWorkingDirectory(t *testing.T) {
	template := Template{
		Params:           []TemplateParam{{ID: "default_dir", Type: "string"}, {ID: "node_dir", Type: "string"}},
		WorkingDirectory: &WorkingDirectoryRef{FromInstanceParam: "default_dir"},
	}
	withNodeOverride := template
	withNodeOverride.Nodes = []NodeDefinition{{
		ID:               "start",
		WorkingDirectory: &WorkingDirectoryRef{FromInstanceParam: "node_dir"},
	}}
	params := map[string]string{"default_dir": "/tmp/default", "node_dir": "/tmp/node"}

	if got := ResolveNodeWorkingDirectory(withNodeOverride, withNodeOverride.Nodes[0], params); got != "/tmp/node" {
		t.Fatalf("node override = %q, want /tmp/node", got)
	}
	plain := template
	plain.Nodes = []NodeDefinition{{ID: "start"}}
	if got := ResolveNodeWorkingDirectory(plain, plain.Nodes[0], params); got != "/tmp/default" {
		t.Fatalf("template default = %q, want /tmp/default", got)
	}
	noDeclaration := Template{Nodes: []NodeDefinition{{ID: "start"}}}
	if got := ResolveNodeWorkingDirectory(noDeclaration, noDeclaration.Nodes[0], params); got != "" {
		t.Fatalf("unset declaration = %q, want empty", got)
	}
	if got := ResolveNodeWorkingDirectory(plain, plain.Nodes[0], nil); got != "" {
		t.Fatalf("unsupplied param = %q, want empty", got)
	}
}

// TestInstantiateWorkingDirectoryParamPathRules proves an instantiation
// value feeding a working-directory declaration must be an absolute, clean
// path; existence is deliberately NOT checked here (a worktree may be
// created after instantiation).
func TestInstantiateWorkingDirectoryParamPathRules(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate(workingDirTemplateJSON(t, "wd-instantiate", map[string]any{"from_instance_param": "worktree_path"}, nil)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	instantiate := func(value string) error {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"template_id": "wd-instantiate", "template_version": "1",
			"params": map[string]string{"worktree_path": value},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		_, err = m.WorkflowInstantiate(payload)
		return err
	}
	rejected := []struct {
		name    string
		value   string
		wantErr string
	}{
		{"relative path", "relative/worktree", "absolute path"},
		{"dot-relative path", "./worktree", "absolute path"},
		{"parent segments", "/tmp/worktree/../other", "clean absolute path"},
		{"trailing slash", "/tmp/worktree/", "clean absolute path"},
		{"double slash", "/tmp//worktree", "clean absolute path"},
		{"bare dot", ".", "absolute path"},
	}
	for _, tc := range rejected {
		if err := instantiate(tc.value); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("%s: instantiate(%q) = %v, want containing %q", tc.name, tc.value, err, tc.wantErr)
		}
	}
	// A clean absolute path is accepted even though the directory does not
	// exist yet.
	if err := instantiate("/tmp/wd-instantiate-not-created-yet"); err != nil {
		t.Fatalf("instantiate(clean absolute path) = %v, want nil", err)
	}
}

// TestValidateWorkingDirectoryRequiresRequiredParam proves a working
// directory can only name a required param: an unsupplied optional param
// would otherwise resolve to unset and fall back to the supervisor's
// working directory.
func TestValidateWorkingDirectoryRequiresRequiredParam(t *testing.T) {
	var tmpl map[string]any
	if err := json.Unmarshal(workingDirTemplateJSON(t, "wd-optional", map[string]any{"from_instance_param": "worktree_path"}, nil), &tmpl); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	tmpl["params"] = []any{map[string]any{"id": "worktree_path", "type": "string"}}
	data, err := json.Marshal(tmpl)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ValidateTemplateJSON(data); err == nil || !strings.Contains(err.Error(), "must be declared required") {
		t.Fatalf("ValidateTemplateJSON() = %v, want the required-param rejection", err)
	}
}

// TestCheckWorkingDirectory pins the dispatch-time usability check: an
// existing directory passes, and a relative path, a missing path, a path to a
// regular file, and a path whose parent is a regular file (a stat error other
// than not-exist) are each rejected with their own reason.
func TestCheckWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckWorkingDirectory(dir); err != nil {
		t.Fatalf("CheckWorkingDirectory(existing dir) = %v, want nil", err)
	}
	for _, tc := range []struct {
		path    string
		wantErr string
	}{
		{"relative/dir", "must be an absolute path"},
		{filepath.Join(dir, "missing"), "does not exist"},
		{file, "is not a directory"},
		{filepath.Join(file, "child"), "is unusable"},
	} {
		if err := CheckWorkingDirectory(tc.path); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("CheckWorkingDirectory(%q) = %v, want containing %q", tc.path, err, tc.wantErr)
		}
	}
}
