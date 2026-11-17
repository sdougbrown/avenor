package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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

// CompleteArtifact is one artifact of a completion request: the source path to
// stage, its stored name, and the declared liveness/digest requirements.
type CompleteArtifact struct {
	SrcPath    string `json:"src_path"`
	StoredPath string `json:"stored_path"`
	NonEmpty   bool   `json:"non_empty"`
	SHA256     string `json:"sha256,omitempty"`
}

// CommandCapture is one bounded document a supervisor captured for evidence,
// held in memory until the manager stages it. It is in-process only: it never
// travels on the complete command, so the public surface cannot supply inline
// evidence bytes. The check's own stream caps bound the capture.
type CommandCapture struct {
	// StoredPath is the evidence name the capture is staged under.
	StoredPath string
	// Data is the exact captured bytes.
	Data []byte
	// Result is inert provenance recorded on the staged evidence entry.
	Result json.RawMessage
}

// AutoCompletion is a complete supervisor-side completion plan. It carries the
// selected outcome, the resolved output values, the artifacts to stage from the
// attempt working directory, and the supervisor's own captures.
type AutoCompletion struct {
	Outcome   OutcomeName        `json:"outcome"`
	Outputs   []CompleteOutput   `json:"outputs,omitempty"`
	Artifacts []CompleteArtifact `json:"artifacts,omitempty"`
	// Captures are the supervisor's in-memory evidence documents. They are
	// staged by CompleteAuto and never serialized onto the complete command.
	Captures []CommandCapture `json:"-"`
}

// AutoCompletionInput is the input to one supervisor-side evaluation. It names
// the template, the node, the instance parameters an argv element may resolve
// from, the attempt's working directory, and the terminal marker label.
// TimeoutC supplies a completion check's deadline signal for a caller that owns
// it.
type AutoCompletionInput struct {
	Template    *Template
	Node        *NodeDefinition
	Params      map[string]string
	WorkingDir  string
	MarkerLabel string
	// TimeoutC, when non-nil, replaces a completion check's deadline timer. A
	// caller that owns a scheduler supplies the signal itself, as does a test
	// that must fire the deadline deterministically.
	TimeoutC <-chan time.Time
}

// EvaluateAutoCompletion is the one supervisor-side evaluation for an
// auto-dispatched provider node whose attempt exited successfully. It returns
// the completion plan, or an error naming why the declared contract is unmet. A
// rejection that ran a completion check returns a *ContractUnmetError carrying
// that check's evidence. The checks mirror what applyComplete applies to an
// explicit request (declared outputs, output value types, declared outcome). A
// passing evaluation cannot then be rejected by the completion path.
func EvaluateAutoCompletion(ctx context.Context, in AutoCompletionInput) (*AutoCompletion, error) {
	outcome, err := autoCompletionOutcome(in.Template, in.Node, in.MarkerLabel)
	if err != nil {
		return nil, err
	}
	plan := &AutoCompletion{Outcome: outcome}
	sources := autoOutputSources{WorkingDir: in.WorkingDir}
	// A check that ran before a later rejection is evidence of that rejection
	// too. The captured run rides the rejection instead of surviving only in the
	// supervisor's log.
	var captures []CommandCapture
	reject := func(err error) error {
		if len(captures) == 0 {
			return err
		}
		return &ContractUnmetError{Reason: err.Error(), Evidence: captures}
	}
	if in.Node.Completion != nil {
		switch in.Node.Completion.Kind {
		case CompletionFiles:
			artifacts, err := evalFilesContract(in.Node.Completion.Artifacts, in.WorkingDir)
			if err != nil {
				return nil, err
			}
			plan.Artifacts = artifacts
			sources.SingleArtifact = singleDeclaredArtifact(in.Node.Completion)
		case CompletionGit:
			if err := evalGitContract(in.Node.Completion.Git, in.WorkingDir); err != nil {
				return nil, err
			}
		case CompletionCommand:
			check, err := evalCommandContract(ctx, in.Node.Completion.Command, in.Params, in.WorkingDir, in.TimeoutC)
			if err != nil {
				// A failed check's captures ride the rejection, so the caller can
				// record them with the attempt's terminal fact.
				return nil, err
			}
			plan.Captures = check.captures
			captures = check.captures
			if check.streamsHeld && declaresCommandStdoutOutputs(in.Node) {
				// The check exit status met the contract, but a descendant it left
				// behind kept its output open. A declared value read from that
				// stdout could then be a truncation artifact. The exit status alone
				// carries no such risk, which is why this rejects only when an
				// output depends on the truncated stream.
				return nil, reject(fmt.Errorf("completion command %s left its output streams open, so the node's declared stdout outputs may be truncated", check.argv[0]))
			}
			sources.CommandStdout = check.stdout
		case CompletionExplicit:
			// Rejected at validation for auto provider nodes; defensive here.
			return nil, fmt.Errorf("explicit completion cannot be evaluated by the supervisor")
		default:
			// Fail closed: an unknown kind is never treated as met.
			return nil, fmt.Errorf("completion.kind %q cannot be evaluated by the supervisor", in.Node.Completion.Kind)
		}
	}
	outputs, err := resolveAutoOutputs(in.Node, sources)
	if err != nil {
		return nil, reject(err)
	}
	plan.Outputs = outputs
	declared := make([]OutputValue, len(outputs))
	for i, o := range outputs {
		declared[i] = OutputValue{DefinitionID: o.DefinitionID}
	}
	if err := validateDeclaredOutputs(in.Node, declared); err != nil {
		return nil, reject(err)
	}
	if err := validateCompleteOutputValues(in.Template, in.Node, outputs); err != nil {
		return nil, reject(err)
	}
	if err := validateDeclaredOutcome(in.Template, in.Node, outcome); err != nil {
		return nil, reject(err)
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
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("completion artifact %q is not a regular file", req.Path)
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
func resolveAutoOutputs(node *NodeDefinition, sources autoOutputSources) ([]CompleteOutput, error) {
	out := make([]CompleteOutput, 0, len(node.Outputs))
	for _, def := range node.Outputs {
		value, ok, err := resolveAutoOutput(def, sources)
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

// autoOutputSources is the work product one auto-completion resolves declared
// outputs from. It holds the attempt's working directory, the single
// files-contract artifact a file output defaults to, and the completion
// command's captured stdout.
type autoOutputSources struct {
	WorkingDir     string
	SingleArtifact string
	CommandStdout  []byte
}

// declaresCommandStdoutOutputs reports whether any declared output reads a value
// from the completion command's stdout.
func declaresCommandStdoutOutputs(node *NodeDefinition) bool {
	for _, def := range node.Outputs {
		if def.Source != nil && def.Source.Command != "" {
			return true
		}
	}
	return false
}

// singleDeclaredArtifact returns the one artifact path a files contract
// declares, or an empty string when it declares none or several.
func singleDeclaredArtifact(contract *CompletionContract) string {
	if contract == nil || contract.Kind != CompletionFiles || len(contract.Artifacts) != 1 {
		return ""
	}
	return contract.Artifacts[0].Path
}

// resolveAutoOutput resolves one output. ok=false with a nil error means the
// output does not resolve and may be omitted (optional only).
func resolveAutoOutput(def OutputDefinition, sources autoOutputSources) (json.RawMessage, bool, error) {
	src := def.Source
	if src == nil {
		// Default binding: a file output on a files contract with exactly one
		// declared artifact binds to that artifact.
		if def.Type == OutputFile && sources.SingleArtifact != "" {
			value, err := json.Marshal(sources.SingleArtifact)
			return value, err == nil, err
		}
		return nil, false, nil
	}
	switch {
	case src.Git != "":
		if def.Type != OutputString {
			return nil, false, fmt.Errorf("output %q: git-head source requires a string output", def.ID)
		}
		out, err := gitOutput(sources.WorkingDir, "rev-parse", "HEAD")
		if err != nil {
			return nil, false, fmt.Errorf("output %q: git-head source: %w", def.ID, err)
		}
		value, err := json.Marshal(strings.TrimSpace(out))
		if err != nil {
			return nil, false, err
		}
		return value, true, nil
	case src.Command != "":
		if def.Type == OutputFile {
			return nil, false, fmt.Errorf("output %q: a pointer source requires a non-file output", def.ID)
		}
		if src.Command != "stdout" {
			return nil, false, fmt.Errorf("output %q: command source %q is not supported", def.ID, src.Command)
		}
		resolved, err := resolveJSONPointer(sources.CommandStdout, src.Pointer)
		if err != nil {
			return nil, false, fmt.Errorf("output %q: pointer %q into the completion command's stdout: %w", def.ID, src.Pointer, err)
		}
		return resolved, true, nil
	default:
		if filepath.IsAbs(src.Artifact) {
			return nil, false, fmt.Errorf("output %q: artifact source %q must be relative to the attempt's working directory", def.ID, src.Artifact)
		}
		abs, err := secureJoin(sources.WorkingDir, src.Artifact)
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

// resolveJSONPointer resolves a non-empty RFC 6901 JSON Pointer against data
// and returns the raw JSON at the target location. An output source without a
// pointer never reaches it.
func resolveJSONPointer(data []byte, pointer string) (json.RawMessage, error) {
	if !strings.HasPrefix(pointer, "/") {
		return nil, fmt.Errorf("pointer must begin with /")
	}
	doc, err := decodeJSONDocument(data)
	if err != nil {
		return nil, fmt.Errorf("document is not valid JSON: %w", err)
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

// AutoCompletionDecision is the supervisor-side decision for one successfully
// exiting attempt. Exactly one of Plan and Rejection is set whenever Eligible
// is true.
type AutoCompletionDecision struct {
	// Eligible reports whether the node is an auto-dispatched provider node the
	// supervisor completes itself.
	Eligible bool
	// Plan carries the completion request.
	Plan *AutoCompletion
	// Rejection names why the declared contract is unmet.
	Rejection string
	// FailureEvidence is the inspection evidence a completion check captured
	// before its contract was rejected. It is recorded with the attempt's
	// terminal fact, so a failed check is as inspectable as a passing one. It is
	// empty for every rejection that ran no check.
	FailureEvidence []CommandCapture
	// Canceled reports that the rejection is the attempt's own cancellation
	// reaching the check, not a judgment on its result.
	Canceled bool
}

// DecideAutoCompletion is the supervisor-side decision for one successfully
// exiting attempt. err reports that the workflow, activation, template, or node
// definition could not be read, so eligibility is unknown. A rejection is only
// ever a contract the work did not meet.
func (m *Manager) DecideAutoCompletion(ctx context.Context, wf WorkflowID, nodeID NodeID, activationID ActivationID, workingDir, markerLabel string) (AutoCompletionDecision, error) {
	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return AutoCompletionDecision{}, fmt.Errorf("load workflow %s: %w", wf, err)
	}
	if !exists {
		return AutoCompletionDecision{}, fmt.Errorf("workflow %s not found", wf)
	}
	act, err := findActivation(&snap.Instance, nodeID, activationID)
	if err != nil {
		return AutoCompletionDecision{}, err
	}
	if act == nil {
		return AutoCompletionDecision{}, fmt.Errorf("activation %s for node %s not found", activationID, nodeID)
	}
	if act.Dispatch == nil || !act.Dispatch.IsAuto() {
		return AutoCompletionDecision{}, nil
	}
	tmpl, err := m.templateFor(&snap)
	if err != nil {
		return AutoCompletionDecision{}, fmt.Errorf("load template for workflow %s: %w", wf, err)
	}
	node, err := findNode(tmpl, nodeID)
	if err != nil {
		return AutoCompletionDecision{}, err
	}
	switch node.Action.Kind {
	case ActionRun, ActionLoop, ActionTeam:
	default:
		return AutoCompletionDecision{}, nil
	}
	plan, evalErr := EvaluateAutoCompletion(ctx, AutoCompletionInput{
		Template:    tmpl,
		Node:        node,
		Params:      snap.Instance.Params,
		WorkingDir:  workingDir,
		MarkerLabel: markerLabel,
	})
	if evalErr != nil {
		decision := AutoCompletionDecision{Eligible: true, Rejection: evalErr.Error()}
		var unmet *ContractUnmetError
		if errors.As(evalErr, &unmet) {
			decision.FailureEvidence = unmet.Evidence
			decision.Canceled = unmet.Canceled
		}
		return decision, nil
	}
	return AutoCompletionDecision{Eligible: true, Plan: plan}, nil
}

// CompleteAuto completes a supervisor-side auto-completion plan through the
// same atomic completion every handoff uses. A supervisor capture cannot take a
// different path from a declared one. The plan's captures are staged from memory
// in the same call, which is why they never travel on a command payload.
func (m *Manager) CompleteAuto(wf WorkflowID, nodeID NodeID, activationID ActivationID, attemptID AttemptID, leaseID LeaseID, ownerToken string, plan *AutoCompletion) (any, error) {
	return m.applyComplete(wf, commandCompleteRequest{
		NodeID:       nodeID,
		ActivationID: activationID,
		AttemptID:    attemptID,
		LeaseID:      leaseID,
		OwnerToken:   ownerToken,
		Outcome:      plan.Outcome,
		Outputs:      plan.Outputs,
		Artifacts:    plan.Artifacts,
		captures:     plan.Captures,
		supervised:   true,
	})
}
