package workflow

// Manager is the workflow-store API surface for the workflow manager. It
// wraps a Store and exposes create/instantiate/read/wait operations. It is
// stdlib-only and imports nothing from internal/control so it can later be
// registered directly as the control package's WorkflowHandler.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"
)

type Manager struct {
	store *Store
}

func NewManager(store *Store) *Manager {
	return &Manager{store: store}
}

// safeComponent rejects empty, ".", "..", and any component containing a path
// separator or NUL byte so ids can never escape their template/instance dirs.
func safeComponent(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	return !strings.ContainsAny(s, "/\\\x00")
}

func (m *Manager) checkWorkflowID(id string) (WorkflowID, error) {
	if !safeComponent(id) {
		return "", errors.New("invalid workflow id")
	}
	return WorkflowID(id), nil
}

// WorkflowCreate validates and stores a versioned template.
func (m *Manager) WorkflowCreate(payload json.RawMessage) (any, error) {
	if err := ValidateTemplateJSON(payload); err != nil {
		return nil, err
	}
	var template Template
	if err := json.Unmarshal(payload, &template); err != nil {
		return nil, err
	}
	if !safeComponent(string(template.TemplateID)) {
		return nil, errors.New("invalid template id")
	}
	if !safeComponent(string(template.TemplateVersion)) {
		return nil, errors.New("invalid template version")
	}
	if err := m.store.StoreTemplate(template.TemplateID, template.TemplateVersion, template); err != nil {
		return nil, err
	}
	return map[string]any{
		"template_id":      string(template.TemplateID),
		"template_version": string(template.TemplateVersion),
	}, nil
}

// WorkflowInstantiate instantiates a stored template as a new active workflow.
func (m *Manager) WorkflowInstantiate(payload json.RawMessage) (any, error) {
	var req struct {
		TemplateID      TemplateID      `json:"template_id"`
		TemplateVersion TemplateVersion `json:"template_version"`
		Metadata        map[string]any  `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("instantiate payload: %w", err)
	}
	if req.TemplateID == "" || req.TemplateVersion == "" {
		return nil, errors.New("template_id and template_version are required")
	}
	if !safeComponent(string(req.TemplateID)) {
		return nil, errors.New("invalid template id")
	}
	if !safeComponent(string(req.TemplateVersion)) {
		return nil, errors.New("invalid template version")
	}
	template, err := m.store.LoadTemplate(req.TemplateID, req.TemplateVersion)
	if err != nil {
		return nil, err
	}
	for _, node := range template.Nodes {
		if node.Action.Kind == ActionWorkflow {
			return nil, fmt.Errorf("template %s@%s contains workflow actions, which are unsupported until the composition stage", req.TemplateID, req.TemplateVersion)
		}
	}
	wf := NewWorkflowID()
	record := InstanceRecord{
		TemplateID:       template.TemplateID,
		TemplateVersion:  template.TemplateVersion,
		TerminalOutcomes: template.TerminalOutcomes,
		EntryNodes:       template.EntryNodes,
	}
	recordJSON, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	snap, err := m.store.ApplyCommand(wf, Command{
		Kind:             CommandInstantiate,
		ExpectedRevision: 0,
		IdempotencyKey:   "inst-" + string(wf),
		Identity:         ExecutionIdentity{WorkflowID: wf},
		Payload:          recordJSON,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"workflow_id": string(wf),
		"revision":    snap.Instance.Revision,
	}, nil
}

// WorkflowStatus returns a compact status view of a workflow instance.
func (m *Manager) WorkflowStatus(id string) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", id)
	}
	inst := snap.Instance
	return map[string]any{
		"workflow_id":      string(wf),
		"template_id":      string(inst.TemplateID),
		"template_version": string(inst.TemplateVersion),
		"status":           string(inst.Status),
		"revision":         inst.Revision,
		"terminal_outcome": string(inst.TerminalOutcome),
	}, nil
}

// inspectMap builds the full-detail inspect shape for a snapshot.
func inspectMap(snap Snapshot) map[string]any {
	inst := snap.Instance
	return map[string]any{
		"workflow_id": string(inst.WorkflowID),
		"instance":    inst,
		"revision":    inst.Revision,
		"activations": inst.Activations,
		"attempts":    inst.Attempts,
		"evidence":    inst.Evidence,
		"gates":       inst.Gates,
		"outputs":     inst.Outputs,
	}
}

// WorkflowInspect returns the full instance detail.
func (m *Manager) WorkflowInspect(id string) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	snap, exists, err := m.store.loadCurrent(wf)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", id)
	}
	return inspectMap(snap), nil
}

func isTerminalStatus(status WorkflowStatus) bool {
	switch status {
	case WorkflowCompleted, WorkflowFailed, WorkflowCanceled:
		return true
	}
	return false
}

// WorkflowWait polls the instance until its status is terminal or the timeout
// elapses. A timeout <= 0 returns after the first poll.
func (m *Manager) WorkflowWait(id string, timeout time.Duration) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, exists, err := m.store.loadCurrent(wf)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("workflow not found: %s", id)
		}
		terminal := isTerminalStatus(snap.Instance.Status)
		timedOut := !terminal && time.Now().After(deadline)
		result := inspectMap(snap)
		result["terminal"] = terminal
		result["timed_out"] = timedOut
		if terminal || timeout <= 0 || timedOut {
			return result, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return result, nil
		}
		select {
		case <-ticker.C:
		case <-time.After(remaining):
		}
	}
}

// WorkflowEvents returns log events with Sequence > afterSeq, capped at limit
// (limit <= 0 caps at 1000).
func (m *Manager) WorkflowEvents(id string, afterSeq int64, limit int) (any, error) {
	wf, err := m.checkWorkflowID(id)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1000
	}
	data, err := os.ReadFile(m.store.eventsPath(wf))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	events := make([]map[string]any, 0)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			continue
		}
		if e.Sequence <= afterSeq {
			continue
		}
		encoded, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		var asMap map[string]any
		if err := json.Unmarshal(encoded, &asMap); err != nil {
			return nil, err
		}
		events = append(events, asMap)
		if len(events) >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return map[string]any{
		"workflow_id": string(wf),
		"after_seq":   afterSeq,
		"limit":       limit,
		"events":      events,
	}, nil
}

// WorkflowCommand is a stub: instance commands are a later stage.
func (m *Manager) WorkflowCommand(id string, payload json.RawMessage) (any, error) {
	return nil, errors.New("workflow commands (claim/start/complete/gate/skip/unblock/reroute/heartbeat) are unsupported until a later stage")
}
