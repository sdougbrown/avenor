package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// autocompletion.go implements the supervisor-side evaluation that turns a
// successfully exiting auto-dispatched provider worker into a
// workflow.complete request. Completion is derived from the node's declared
// contract (outcome vocabulary, files/git requirements, output sources) and
// the work product in the attempt's working directory — never from anything
// the worker writes about itself. EvaluateAutoCompletion is the one
// evaluation seam; a future judge or classifier plugs in there.

// CompleteOutput is one resolved output value of a completion request.
type CompleteOutput struct {
	DefinitionID OutputID        `json:"definition_id"`
	Value        json.RawMessage `json:"value"`
}

// CompleteArtifact is one artifact of a completion request: the source path
// to stage, its stored name, and the declared liveness/digest requirements.
type CompleteArtifact struct {
	SrcPath    string `json:"src_path"`
	StoredPath string `json:"stored_path"`
	NonEmpty   bool   `json:"non_empty"`
	SHA256     string `json:"sha256,omitempty"`
}

// AutoCompletion is a complete supervisor-side completion plan: the selected
// outcome, the resolved output values, and the artifacts to stage as
// evidence.
type AutoCompletion struct {
	Outcome   OutcomeName        `json:"outcome"`
	Outputs   []CompleteOutput   `json:"outputs,omitempty"`
	Artifacts []CompleteArtifact `json:"artifacts,omitempty"`
}

// EvaluateAutoCompletion is the one supervisor-side evaluation function for
// an auto-dispatched provider node whose attempt exited successfully. It
// returns the completion plan, or an error naming why the declared contract
// is unmet. The checks mirror what commandComplete applies to an explicit
// request (declared outputs, output value types, declared outcome), so a
// passing evaluation cannot be rejected by the completion path.
func EvaluateAutoCompletion(tmpl *Template, node *NodeDefinition, workingDir, markerLabel string) (*AutoCompletion, error) {
	outcome, err := autoCompletionOutcome(tmpl, node, markerLabel)
	if err != nil {
		return nil, err
	}
	plan := &AutoCompletion{Outcome: outcome}
	if node.Completion != nil {
		switch node.Completion.Kind {
		case CompletionFiles:
			artifacts, err := evalFilesContract(node.Completion.Artifacts, workingDir)
			if err != nil {
				return nil, err
			}
			plan.Artifacts = artifacts
		case CompletionGit:
			if err := evalGitContract(node.Completion.Git, workingDir); err != nil {
				return nil, err
			}
		case CompletionExplicit:
			// Rejected at validation for auto provider nodes; defensive here.
			return nil, fmt.Errorf("explicit completion cannot be evaluated by the supervisor")
		}
	}
	outputs, err := resolveAutoOutputs(node, workingDir)
	if err != nil {
		return nil, err
	}
	plan.Outputs = outputs
	declared := make([]OutputValue, len(outputs))
	for i, o := range outputs {
		declared[i] = OutputValue{DefinitionID: o.DefinitionID}
	}
	if err := validateDeclaredOutputs(node, declared); err != nil {
		return nil, err
	}
	if err := validateCompleteOutputValues(tmpl, node, outputs); err != nil {
		return nil, err
	}
	if err := validateDeclaredOutcome(tmpl, node, outcome); err != nil {
		return nil, err
	}
	return plan, nil
}

// autoCompletionOutcome selects the completion outcome. When the node's
// declared outcome vocabulary has exactly one candidate it is used;
// otherwise the attempt's terminal marker label selects a declared outcome.
// Anything else leaves the contract unmet.
func autoCompletionOutcome(tmpl *Template, node *NodeDefinition, markerLabel string) (OutcomeName, error) {
	vocabulary := declaredOutcomeVocabulary(tmpl, node)
	if len(vocabulary) == 1 {
		return vocabulary[0], nil
	}
	label := OutcomeName(strings.TrimSpace(markerLabel))
	if label != "" {
		for _, name := range vocabulary {
			if name == label {
				return label, nil
			}
		}
	}
	return "", fmt.Errorf("node declares %d candidate outcomes and no terminal marker selected one of them", len(vocabulary))
}

// declaredOutcomeVocabulary lists, in no particular order, the outcome names
// the completion is selected from: the node's own declared outcomes (branch
// keys, node outcomes, checkpoint exits) when it declares any, falling back
// to the template's terminal outcomes for a node with no declared outcomes —
// the same names resolveOutcome accepts.
func declaredOutcomeVocabulary(tmpl *Template, node *NodeDefinition) []OutcomeName {
	seen := make(map[OutcomeName]bool)
	var out []OutcomeName
	add := func(name OutcomeName) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		out = append(out, name)
	}
	for name := range node.Branches {
		add(name)
	}
	for _, def := range node.Outcomes {
		add(def.Name)
	}
	if node.Checkpoint != nil {
		for _, name := range node.Checkpoint.ExitOutcomes {
			add(name)
		}
	}
	if len(out) == 0 {
		for _, name := range tmpl.TerminalOutcomes {
			add(name)
		}
	}
	return out
}

// evalFilesContract checks every declared artifact against the attempt's
// working directory and returns the artifacts to stage as evidence.
func evalFilesContract(requirements []ArtifactRequirement, workingDir string) ([]CompleteArtifact, error) {
	out := make([]CompleteArtifact, 0, len(requirements))
	for _, req := range requirements {
		if filepath.IsAbs(req.Path) {
			return nil, fmt.Errorf("completion artifact %q must be relative to the attempt's working directory", req.Path)
		}
		abs, err := secureJoin(workingDir, req.Path)
		if err != nil {
			return nil, fmt.Errorf("completion artifact %q: %w", req.Path, err)
		}
		info, err := os.Stat(abs)
		if err != nil {
			return nil, fmt.Errorf("completion artifact %q does not exist in the working directory", req.Path)
		}
		if req.NonEmpty && info.Size() == 0 {
			return nil, fmt.Errorf("completion artifact %q is empty", req.Path)
		}
		artifact := CompleteArtifact{SrcPath: abs, StoredPath: req.Path, NonEmpty: req.NonEmpty, SHA256: req.SHA256}
		if req.SHA256 != "" {
			digest, err := sha256File(abs)
			if err != nil {
				return nil, fmt.Errorf("completion artifact %q: %w", req.Path, err)
			}
			if digest != req.SHA256 {
				return nil, fmt.Errorf("completion artifact %q digest %s does not match the declared sha256 %s", req.Path, digest, req.SHA256)
			}
			artifact.SHA256 = digest
		}
		out = append(out, artifact)
	}
	return out, nil
}

// evalGitContract evaluates a git completion requirement in the attempt's
// working directory. changed_from_base has no available base on this path —
// the attempt's base SHA is not recorded — so it is reported rather than
// inventing one.
func evalGitContract(req *GitRequirement, workingDir string) error {
	if req == nil {
		return nil
	}
	if req.ChangedFromBase {
		return fmt.Errorf("completion git.changed_from_base has no recorded base commit to compare against")
	}
	if req.Clean {
		out, err := gitOutput(workingDir, "status", "--porcelain")
		if err != nil {
			return fmt.Errorf("completion git.clean could not be evaluated: %w", err)
		}
		if strings.TrimSpace(out) != "" {
			return fmt.Errorf("completion git.clean: the working directory has uncommitted changes")
		}
	}
	if req.Head != "" {
		out, err := gitOutput(workingDir, "rev-parse", "HEAD")
		if err != nil {
			return fmt.Errorf("completion git.head could not be evaluated: %w", err)
		}
		if head := strings.TrimSpace(out); head != strings.TrimSpace(req.Head) {
			return fmt.Errorf("completion git.head: HEAD %s does not match the declared head %s", head, req.Head)
		}
	}
	return nil
}

// gitOutput runs one git command in dir and returns its stdout. Errors carry
// git's stderr so a contract rejection names the actual failure.
func gitOutput(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", errors.New(msg)
	}
	return string(out), nil
}

// resolveAutoOutputs resolves every declared output from its source (or the
// default binding). Required outputs that do not resolve leave the contract
// unmet; optional ones that do not resolve are omitted.
func resolveAutoOutputs(node *NodeDefinition, workingDir string) ([]CompleteOutput, error) {
	singleArtifact := ""
	if node.Completion != nil && node.Completion.Kind == CompletionFiles && len(node.Completion.Artifacts) == 1 {
		singleArtifact = node.Completion.Artifacts[0].Path
	}
	out := make([]CompleteOutput, 0, len(node.Outputs))
	for _, def := range node.Outputs {
		value, ok, err := resolveAutoOutput(def, singleArtifact, workingDir)
		if err != nil {
			if !def.Required {
				// An optional output whose source does not resolve is omitted.
				continue
			}
			return nil, err
		}
		if !ok {
			if def.Required {
				return nil, fmt.Errorf("required output %q cannot be resolved from the node's declared contract", def.ID)
			}
			continue
		}
		out = append(out, CompleteOutput{DefinitionID: def.ID, Value: value})
	}
	return out, nil
}

// resolveAutoOutput resolves one output. ok=false with a nil error means the
// output does not resolve and may be omitted (optional only).
func resolveAutoOutput(def OutputDefinition, singleArtifact, workingDir string) (json.RawMessage, bool, error) {
	src := def.Source
	if src == nil {
		// Default binding: a file output on a files contract with exactly one
		// declared artifact binds to that artifact.
		if def.Type == OutputFile && singleArtifact != "" {
			value, err := json.Marshal(singleArtifact)
			return value, err == nil, err
		}
		return nil, false, nil
	}
	switch {
	case src.Git != "":
		if def.Type != OutputString {
			return nil, false, fmt.Errorf("output %q: git-head source requires a string output", def.ID)
		}
		out, err := gitOutput(workingDir, "rev-parse", "HEAD")
		if err != nil {
			return nil, false, fmt.Errorf("output %q: git-head source: %w", def.ID, err)
		}
		value, err := json.Marshal(strings.TrimSpace(out))
		if err != nil {
			return nil, false, err
		}
		return value, true, nil
	default:
		if filepath.IsAbs(src.Artifact) {
			return nil, false, fmt.Errorf("output %q: artifact source %q must be relative to the attempt's working directory", def.ID, src.Artifact)
		}
		abs, err := secureJoin(workingDir, src.Artifact)
		if err != nil {
			return nil, false, fmt.Errorf("output %q: artifact source %q: %w", def.ID, src.Artifact, err)
		}
		if src.Pointer != "" {
			if def.Type == OutputFile {
				return nil, false, fmt.Errorf("output %q: a pointer source requires a non-file output", def.ID)
			}
			data, err := os.ReadFile(abs)
			if err != nil {
				return nil, false, fmt.Errorf("output %q: artifact source %q: %w", def.ID, src.Artifact, err)
			}
			resolved, err := resolveJSONPointer(data, src.Pointer)
			if err != nil {
				return nil, false, fmt.Errorf("output %q: pointer %q into %q: %w", def.ID, src.Pointer, src.Artifact, err)
			}
			return resolved, true, nil
		}
		// Whole declared artifact: matches how file outputs are represented as
		// completion values (the artifact's declared path) and how evidence is
		// attached to them.
		if def.Type != OutputFile {
			return nil, false, fmt.Errorf("output %q: whole-artifact source requires a file output", def.ID)
		}
		value, err := json.Marshal(src.Artifact)
		if err != nil {
			return nil, false, err
		}
		return value, true, nil
	}
}

// secureJoin joins a declared relative path onto the working directory and
// rejects any result that escapes it.
func secureJoin(workingDir, rel string) (string, error) {
	abs := filepath.Join(workingDir, rel)
	inside, err := filepath.Rel(workingDir, abs)
	if err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes the working directory", rel)
	}
	return abs, nil
}

// resolveJSONPointer resolves an RFC 6901 JSON Pointer against data and
// returns the raw JSON at the target location.
func resolveJSONPointer(data []byte, pointer string) (json.RawMessage, error) {
	if pointer == "" {
		return json.RawMessage(bytes.TrimSpace(data)), nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, fmt.Errorf("pointer must begin with /")
	}
	var doc any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("artifact is not valid JSON: %w", err)
	}
	cur := doc
	for _, token := range strings.Split(pointer[1:], "/") {
		token = strings.ReplaceAll(token, "~1", "/")
		token = strings.ReplaceAll(token, "~0", "~")
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[token]
			if !ok {
				return nil, fmt.Errorf("member %q not found", token)
			}
			cur = next
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(node) {
				return nil, fmt.Errorf("index %q out of range", token)
			}
			cur = node[index]
		default:
			return nil, fmt.Errorf("cannot descend into scalar")
		}
	}
	raw, err := json.Marshal(cur)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// DecideAutoCompletion is the supervisor-side decision for one successfully
// exiting attempt: eligible reports whether the node is an auto-dispatched
// provider node the supervisor completes itself; plan carries the completion
// request; rejection names why the declared contract is unmet. Exactly one of
// plan and rejection is set whenever eligible is true. err reports that the
// workflow or activation could not be read, so eligibility is unknown.
func (m *Manager) DecideAutoCompletion(wf WorkflowID, nodeID NodeID, activationID ActivationID, workingDir, markerLabel string) (eligible bool, plan *AutoCompletion, rejection string, err error) {
	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return false, nil, "", fmt.Errorf("load workflow %s: %w", wf, err)
	}
	if !exists {
		return false, nil, "", fmt.Errorf("workflow %s not found", wf)
	}
	act, err := findActivation(&snap.Instance, nodeID, activationID)
	if err != nil {
		return false, nil, "", err
	}
	if act == nil {
		return false, nil, "", fmt.Errorf("activation %s for node %s not found", activationID, nodeID)
	}
	if act.Dispatch == nil || !act.Dispatch.IsAuto() {
		return false, nil, "", nil
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return true, nil, fmt.Sprintf("template unreadable: %v", err), nil
	}
	node, err := findNode(tmpl, nodeID)
	if err != nil {
		return true, nil, err.Error(), nil
	}
	switch node.Action.Kind {
	case ActionRun, ActionLoop, ActionTeam:
	default:
		return false, nil, "", nil
	}
	plan2, evalErr := EvaluateAutoCompletion(tmpl, node, workingDir, markerLabel)
	if evalErr != nil {
		return true, nil, evalErr.Error(), nil
	}
	return true, plan2, "", nil
}

// CompleteAuto issues the workflow.complete command for a supervisor-side
// auto-completion plan: same atomic command, evidence staging, output
// recording, and lease release as an explicit worker handoff.
func (m *Manager) CompleteAuto(wf WorkflowID, nodeID NodeID, activationID ActivationID, attemptID AttemptID, leaseID LeaseID, ownerToken string, plan *AutoCompletion) (any, error) {
	payload, err := json.Marshal(struct {
		Op           string       `json:"op"`
		NodeID       NodeID       `json:"node_id"`
		ActivationID ActivationID `json:"activation_id"`
		AttemptID    AttemptID    `json:"attempt_id"`
		LeaseID      LeaseID      `json:"lease_id"`
		OwnerToken   string       `json:"owner_token"`
		AutoCompletion
	}{
		Op:             "complete",
		NodeID:         nodeID,
		ActivationID:   activationID,
		AttemptID:      attemptID,
		LeaseID:        leaseID,
		OwnerToken:     ownerToken,
		AutoCompletion: *plan,
	})
	if err != nil {
		return nil, err
	}
	return m.WorkflowCommand(string(wf), payload)
}
