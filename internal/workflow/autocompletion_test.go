package workflow

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// autoEvalNode builds a single auto run node with the given outputs,
// completion contract, and branches.
func autoEvalNode(outputs []OutputDefinition, completion *CompletionContract, branches map[OutcomeName]NodeID) *NodeDefinition {
	return &NodeDefinition{
		ID:         "produce",
		Action:     Action{Kind: ActionRun, Run: &RunAction{Prompt: "work"}},
		Outputs:    outputs,
		Completion: completion,
		Branches:   branches,
	}
}

func evalTemplate(node *NodeDefinition, terminalOutcomes ...string) *Template {
	return &Template{
		SchemaVersion:    1,
		TemplateID:       "eval",
		TemplateVersion:  "1",
		TerminalOutcomes: convertOutcomes(terminalOutcomes),
		Nodes:            []NodeDefinition{*node},
	}
}

func convertOutcomes(names []string) []OutcomeName {
	out := make([]OutcomeName, len(names))
	for i, n := range names {
		out[i] = OutcomeName(n)
	}
	return out
}

func stringOutput(id string, required bool, source *OutputSource) OutputDefinition {
	return OutputDefinition{ID: OutputID(id), Name: id, Type: OutputString, Required: required, Source: source}
}

func TestEvaluateAutoCompletionSingleCandidateOutcome(t *testing.T) {
	tmpl := evalTemplate(autoEvalNode(nil, nil, map[OutcomeName]NodeID{"done": "next"}), "done")
	plan, err := EvaluateAutoCompletion(tmpl, &tmpl.Nodes[0], t.TempDir(), "")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	if plan.Outcome != "done" {
		t.Fatalf("outcome = %q, want done", plan.Outcome)
	}
	if len(plan.Outputs) != 0 || len(plan.Artifacts) != 0 {
		t.Fatalf("plan = %+v, want no outputs or artifacts", plan)
	}
}

func TestEvaluateAutoCompletionTerminalOutcomeVocabulary(t *testing.T) {
	// No branches: the template's terminal outcomes are the vocabulary.
	tmpl := evalTemplate(autoEvalNode(nil, nil, nil), "done")
	plan, err := EvaluateAutoCompletion(tmpl, &tmpl.Nodes[0], t.TempDir(), "")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	if plan.Outcome != "done" {
		t.Fatalf("outcome = %q, want done", plan.Outcome)
	}

	// A node that declares its own outcomes is not widened by the template's
	// terminal outcomes: one declared branch stays the single candidate even
	// in a multi-terminal template.
	tmpl = evalTemplate(autoEvalNode(nil, nil, map[OutcomeName]NodeID{"done": "next"}), "done", "abandoned")
	plan, err = EvaluateAutoCompletion(tmpl, &tmpl.Nodes[0], t.TempDir(), "")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	if plan.Outcome != "done" {
		t.Fatalf("outcome = %q, want done (the node's only declared outcome)", plan.Outcome)
	}

	// Two candidates and no marker: contract unmet.
	tmpl = evalTemplate(autoEvalNode(nil, nil, map[OutcomeName]NodeID{"passed": "x", "failed": "x"}), "done")
	if _, err := EvaluateAutoCompletion(tmpl, &tmpl.Nodes[0], t.TempDir(), ""); err == nil ||
		!strings.Contains(err.Error(), "no terminal marker selected") {
		t.Fatalf("EvaluateAutoCompletion error = %v, want marker-selection rejection", err)
	}

	// The marker label selects a declared outcome.
	plan, err = EvaluateAutoCompletion(tmpl, &tmpl.Nodes[0], t.TempDir(), "failed")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion with marker: %v", err)
	}
	if plan.Outcome != "failed" {
		t.Fatalf("outcome = %q, want failed (the marker label)", plan.Outcome)
	}

	// A marker label outside the vocabulary is rejected.
	if _, err := EvaluateAutoCompletion(tmpl, &tmpl.Nodes[0], t.TempDir(), "checkpoint"); err == nil {
		t.Fatal("EvaluateAutoCompletion accepted an undeclared marker label")
	}
}

func TestEvaluateAutoCompletionFilesContract(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "result.md"), []byte("the summary\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	node := autoEvalNode(nil, &CompletionContract{
		Kind:      CompletionFiles,
		Artifacts: []ArtifactRequirement{{Path: "result.md", NonEmpty: true}},
	}, map[OutcomeName]NodeID{"done": "next"})

	tmpl := evalTemplate(node, "done")
	plan, err := EvaluateAutoCompletion(tmpl, node, dir, "")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	if len(plan.Artifacts) != 1 {
		t.Fatalf("artifacts = %+v, want one", plan.Artifacts)
	}
	if plan.Artifacts[0].SrcPath != filepath.Join(dir, "result.md") || plan.Artifacts[0].StoredPath != "result.md" {
		t.Fatalf("artifact = %+v, want src in the working dir and the declared stored path", plan.Artifacts[0])
	}

	if err := os.WriteFile(filepath.Join(dir, "result.md"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := EvaluateAutoCompletion(tmpl, node, dir, ""); err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("empty artifact error = %v, want empty rejection", err)
	}
	if err := os.Remove(filepath.Join(dir, "result.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := EvaluateAutoCompletion(tmpl, node, dir, ""); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("missing artifact error = %v, want missing rejection", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "result.md"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.md", "inner"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := EvaluateAutoCompletion(tmpl, node, dir, ""); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("directory artifact error = %v, want non-regular rejection", err)
	}
}

func TestEvaluateAutoCompletionFilesContractSHA256(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "result.md"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := sha256File(filepath.Join(dir, "result.md"))
	if err != nil {
		t.Fatal(err)
	}
	node := autoEvalNode(nil, &CompletionContract{
		Kind:      CompletionFiles,
		Artifacts: []ArtifactRequirement{{Path: "result.md", SHA256: digest}},
	}, map[OutcomeName]NodeID{"done": "next"})
	plan, err := EvaluateAutoCompletion(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	if len(plan.Artifacts) != 1 || plan.Artifacts[0].SHA256 != digest {
		t.Fatalf("artifacts = %+v, want the matching digest", plan.Artifacts)
	}

	node.Completion.Artifacts[0].SHA256 = strings.Repeat("0", 64)
	if _, err := EvaluateAutoCompletion(evalTemplate(node, "done"), node, dir, ""); err == nil ||
		!strings.Contains(err.Error(), "does not match the declared sha256") {
		t.Fatalf("digest mismatch error = %v, want sha256 rejection", err)
	}
}

func TestEvaluateAutoCompletionGitContract(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return string(out)
	}
	git("init")
	if err := os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "tracked.txt")
	git("commit", "-m", "base")
	head := strings.TrimSpace(git("rev-parse", "HEAD"))

	node := autoEvalNode(nil, &CompletionContract{Kind: CompletionGit, Git: &GitRequirement{Clean: true, Head: head}}, map[OutcomeName]NodeID{"done": "next"})
	tmpl := evalTemplate(node, "done")
	if _, err := EvaluateAutoCompletion(tmpl, node, dir, ""); err != nil {
		t.Fatalf("clean repo at head: %v", err)
	}

	node.Completion.Git.Head = strings.Repeat("a", 40)
	if _, err := EvaluateAutoCompletion(tmpl, node, dir, ""); err == nil ||
		!strings.Contains(err.Error(), "does not match the declared head") {
		t.Fatalf("head mismatch error = %v, want head rejection", err)
	}

	node.Completion.Git.Head = head
	if err := os.WriteFile(filepath.Join(dir, "dirty.txt"), []byte("y\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := EvaluateAutoCompletion(tmpl, node, dir, ""); err == nil ||
		!strings.Contains(err.Error(), "uncommitted changes") {
		t.Fatalf("dirty repo error = %v, want clean rejection", err)
	}

	notRepo := t.TempDir()
	for _, req := range []*GitRequirement{{Clean: true}, {Head: head}} {
		node.Completion.Git = req
		if _, err := EvaluateAutoCompletion(tmpl, node, notRepo, ""); err == nil ||
			!strings.Contains(err.Error(), "could not be evaluated") {
			t.Fatalf("git contract %+v outside a repository: error = %v, want could-not-be-evaluated rejection", *req, err)
		}
	}

	node.Completion.Git = &GitRequirement{ChangedFromBase: true}
	if _, err := EvaluateAutoCompletion(tmpl, node, dir, ""); err == nil ||
		!strings.Contains(err.Error(), "no recorded base commit") {
		t.Fatalf("changed_from_base error = %v, want no-base rejection", err)
	}
}

func TestEvaluateAutoCompletionOutputSources(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pr-info.json"), []byte(`{"repository":"org/repo","pr":{"number":3}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pr-info.md"), []byte("# PR\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	node := autoEvalNode([]OutputDefinition{
		stringOutput("repository", true, &OutputSource{Artifact: "pr-info.json", Pointer: "/repository"}),
		{ID: "pr_number", Name: "PR number", Type: OutputNumber, Required: true, Source: &OutputSource{Artifact: "pr-info.json", Pointer: "/pr/number"}},
		{ID: "pr_url", Name: "PR URL", Type: OutputString, Source: &OutputSource{Artifact: "pr-info.json", Pointer: "/missing"}},
		{ID: "pr_doc", Name: "PR doc", Type: OutputFile, Source: &OutputSource{Artifact: "pr-info.md"}},
	}, &CompletionContract{
		Kind:      CompletionFiles,
		Artifacts: []ArtifactRequirement{{Path: "pr-info.json"}, {Path: "pr-info.md"}},
	}, map[OutcomeName]NodeID{"done": "next"})
	plan, err := EvaluateAutoCompletion(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	values := map[OutputID]json.RawMessage{}
	for _, o := range plan.Outputs {
		values[o.DefinitionID] = o.Value
	}
	if string(values["repository"]) != `"org/repo"` {
		t.Fatalf("repository = %s, want \"org/repo\"", values["repository"])
	}
	if string(values["pr_number"]) != "3" {
		t.Fatalf("pr_number = %s, want 3", values["pr_number"])
	}
	// The optional output with an unresolvable pointer is omitted; the whole
	// file output resolves to its declared artifact path.
	if _, ok := values["pr_url"]; ok {
		t.Fatalf("optional unresolvable output recorded: %+v", values)
	}
	if string(values["pr_doc"]) != `"pr-info.md"` {
		t.Fatalf("pr_doc = %s, want the declared artifact path", values["pr_doc"])
	}
}

func TestEvaluateAutoCompletionGitHeadSource(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return string(out)
	}
	git("init")
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "f.txt")
	git("commit", "-m", "base")
	head := strings.TrimSpace(git("rev-parse", "HEAD"))

	node := autoEvalNode([]OutputDefinition{
		stringOutput("head", true, &OutputSource{Git: "head"}),
	}, &CompletionContract{Kind: CompletionGit, Git: &GitRequirement{Clean: true}}, map[OutcomeName]NodeID{"done": "next"})
	plan, err := EvaluateAutoCompletion(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	if len(plan.Outputs) != 1 {
		t.Fatalf("outputs = %+v, want one", plan.Outputs)
	}
	var value string
	if err := json.Unmarshal(plan.Outputs[0].Value, &value); err != nil {
		t.Fatalf("head value %s is not a JSON string: %v", plan.Outputs[0].Value, err)
	}
	if value != head {
		t.Fatalf("head = %q, want %q", value, head)
	}
}

// TestEvaluateAutoCompletionGitHeadSourceOutsideRepo pins the git-head
// output source's failure path: outside a repository a required output
// rejects the contract naming the git failure, and an optional one is
// omitted rather than recorded as the error text.
func TestEvaluateAutoCompletionGitHeadSourceOutsideRepo(t *testing.T) {
	dir := t.TempDir()
	required := autoEvalNode([]OutputDefinition{
		stringOutput("head", true, &OutputSource{Git: "head"}),
	}, nil, map[OutcomeName]NodeID{"done": "next"})
	if _, err := EvaluateAutoCompletion(evalTemplate(required, "done"), required, dir, ""); err == nil ||
		!strings.Contains(err.Error(), "git-head source") {
		t.Fatalf("required git-head output outside a repository: error = %v, want git-head source rejection", err)
	}

	optional := autoEvalNode([]OutputDefinition{
		stringOutput("head", false, &OutputSource{Git: "head"}),
	}, nil, map[OutcomeName]NodeID{"done": "next"})
	plan, err := EvaluateAutoCompletion(evalTemplate(optional, "done"), optional, dir, "")
	if err != nil {
		t.Fatalf("optional git-head output outside a repository: %v", err)
	}
	if len(plan.Outputs) != 0 {
		t.Fatalf("outputs = %+v, want the unresolvable optional output omitted", plan.Outputs)
	}
}

func TestEvaluateAutoCompletionRequiredOutputUnresolvable(t *testing.T) {
	dir := t.TempDir()
	// The contract artifact exists; the required string output has no source
	// and no default binding, so it cannot resolve.
	if err := os.WriteFile(filepath.Join(dir, "result.md"), []byte("content\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	node := autoEvalNode([]OutputDefinition{
		stringOutput("summary", true, nil),
	}, &CompletionContract{
		Kind:      CompletionFiles,
		Artifacts: []ArtifactRequirement{{Path: "result.md"}},
	}, map[OutcomeName]NodeID{"done": "next"})
	if _, err := EvaluateAutoCompletion(evalTemplate(node, "done"), node, dir, ""); err == nil ||
		!strings.Contains(err.Error(), "cannot be resolved") {
		t.Fatalf("unresolvable required output error = %v, want rejection", err)
	}
}

func TestEvaluateAutoCompletionDefaultFileBinding(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "result.md"), []byte("content\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	node := autoEvalNode([]OutputDefinition{
		{ID: "log", Name: "Log", Type: OutputFile, Required: true},
	}, &CompletionContract{
		Kind:      CompletionFiles,
		Artifacts: []ArtifactRequirement{{Path: "result.md", NonEmpty: true}},
	}, map[OutcomeName]NodeID{"done": "next"})
	plan, err := EvaluateAutoCompletion(evalTemplate(node, "done"), node, dir, "")
	if err != nil {
		t.Fatalf("EvaluateAutoCompletion: %v", err)
	}
	if len(plan.Outputs) != 1 || plan.Outputs[0].DefinitionID != "log" {
		t.Fatalf("outputs = %+v, want the default-bound file output", plan.Outputs)
	}
	var value string
	if err := json.Unmarshal(plan.Outputs[0].Value, &value); err != nil {
		t.Fatalf("file output value %s is not a JSON string: %v", plan.Outputs[0].Value, err)
	}
	if value != "result.md" {
		t.Fatalf("file output value = %q, want the declared artifact path", value)
	}
}

func TestEvaluateAutoCompletionPointerTypeMismatch(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data.json"), []byte(`{"n":3}`), 0o600); err != nil {
		t.Fatal(err)
	}
	node := autoEvalNode([]OutputDefinition{
		stringOutput("n", true, &OutputSource{Artifact: "data.json", Pointer: "/n"}),
	}, &CompletionContract{
		Kind:      CompletionFiles,
		Artifacts: []ArtifactRequirement{{Path: "data.json"}},
	}, map[OutcomeName]NodeID{"done": "next"})
	if _, err := EvaluateAutoCompletion(evalTemplate(node, "done"), node, dir, ""); err == nil ||
		!strings.Contains(err.Error(), "requires a string value") {
		t.Fatalf("type mismatch error = %v, want string-type rejection", err)
	}
}

func TestResolveJSONPointer(t *testing.T) {
	data := []byte(`{"a": {"b": [10, {"c": "deep"}]}, "s": "x", "n": 1.5, "t": true, "sl/ash": "esc", "ti~lde": "til"}`)
	tests := []struct {
		pointer string
		want    string
	}{
		{"/s", `"x"`},
		{"/a/b/0", "10"},
		{"/a/b/1/c", `"deep"`},
		{"/n", "1.5"},
		{"/t", "true"},
		{"/sl~1ash", `"esc"`},
		{"/ti~0lde", `"til"`},
	}
	for _, test := range tests {
		got, err := resolveJSONPointer(data, test.pointer)
		if err != nil {
			t.Fatalf("resolveJSONPointer(%q): %v", test.pointer, err)
		}
		if string(got) != test.want {
			t.Fatalf("resolveJSONPointer(%q) = %s, want %s", test.pointer, got, test.want)
		}
	}
	for _, pointer := range []string{"", "/missing", "/a/b/9", "no-slash", "/s/x"} {
		if _, err := resolveJSONPointer(data, pointer); err == nil {
			t.Fatalf("resolveJSONPointer(%q) succeeded, want error", pointer)
		}
	}
	if _, err := resolveJSONPointer([]byte(`{"a": `), "/a"); err == nil || !strings.Contains(err.Error(), "not valid JSON") {
		t.Fatalf("resolveJSONPointer on invalid JSON: error = %v, want invalid-JSON rejection", err)
	}
}

// TestEvaluateAutoCompletionRejectsPathsOutsideWorkingDir pins the
// working-directory containment of declared artifact and output-source
// paths: a traversal and an absolute path are both rejected.
func TestEvaluateAutoCompletionRejectsPathsOutsideWorkingDir(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "work")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(parent, "secret")
	if err := os.WriteFile(secret, []byte(`{"v":"s"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../secret", secret} {
		node := autoEvalNode(nil, &CompletionContract{
			Kind:      CompletionFiles,
			Artifacts: []ArtifactRequirement{{Path: path}},
		}, map[OutcomeName]NodeID{"done": "next"})
		if _, err := EvaluateAutoCompletion(evalTemplate(node, "done"), node, dir, ""); err == nil ||
			!(strings.Contains(err.Error(), "escapes the working directory") || strings.Contains(err.Error(), "must be relative")) {
			t.Fatalf("artifact %q error = %v, want a working-directory rejection", path, err)
		}

		def := stringOutput("v", true, &OutputSource{Artifact: path, Pointer: "/v"})
		if _, _, err := resolveAutoOutput(def, "", dir); err == nil ||
			!(strings.Contains(err.Error(), "escapes the working directory") || strings.Contains(err.Error(), "must be relative")) {
			t.Fatalf("output source %q error = %v, want a working-directory rejection", path, err)
		}
	}
}

// TestResolveAutoOutputRejectsMismatchedSourceShape pins the runtime guard
// behind the validation rule: a pointer source on a file output and a
// whole-artifact source on a non-file output never resolve.
func TestResolveAutoOutputRejectsMismatchedSourceShape(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data.json"), []byte(`{"n":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	filePointer := OutputDefinition{ID: "f", Name: "F", Type: OutputFile, Required: true,
		Source: &OutputSource{Artifact: "data.json", Pointer: "/n"}}
	if _, _, err := resolveAutoOutput(filePointer, "", dir); err == nil ||
		!strings.Contains(err.Error(), "a pointer source requires a non-file output") {
		t.Fatalf("file output with pointer error = %v, want rejection", err)
	}
	stringWhole := stringOutput("s", true, &OutputSource{Artifact: "data.json"})
	if _, _, err := resolveAutoOutput(stringWhole, "", dir); err == nil ||
		!strings.Contains(err.Error(), "whole-artifact source requires a file output") {
		t.Fatalf("string output with whole artifact error = %v, want rejection", err)
	}
}

// TestDecideAutoCompletionReportsUnreadableTemplateAsError proves a template
// that cannot be read is an error, not a contract rejection, so the
// supervisor never records a successful run as contract_unmet and burns a
// retry because the store could not be read.
func TestDecideAutoCompletionReportsUnreadableTemplateAsError(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "auto-decide-unreadable", "ctl-a", 50)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	act := activationByNode(&snap.Instance, "start")
	if err := os.Remove(filepath.Join(s.root, "templates", string(snap.Instance.TemplateID), string(snap.Instance.TemplateVersion)+".json")); err != nil {
		t.Fatalf("remove template: %v", err)
	}

	eligible, plan, rejection, err := m.DecideAutoCompletion(wf, "start", act.ID, t.TempDir(), "")
	if err == nil {
		t.Fatal("DecideAutoCompletion with an unreadable template returned no error")
	}
	if eligible || plan != nil || rejection != "" {
		t.Fatalf("DecideAutoCompletion = eligible %v, plan %+v, rejection %q; want an error only", eligible, plan, rejection)
	}
}
