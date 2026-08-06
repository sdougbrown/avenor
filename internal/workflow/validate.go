package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
)

// templateEnvelope is the temporary Stage 1 wire shape. Stage 2 replaces it
// with the canonical serializable Template model while retaining the
// ValidateTemplateJSON boundary.
type templateEnvelope struct {
	SchemaVersion     json.RawMessage   `json:"schema_version"`
	TemplateID        string            `json:"template_id"`
	TemplateVersion   string            `json:"template_version"`
	Metadata          map[string]any    `json:"metadata,omitempty"`
	EntryNodes        []string          `json:"entry_nodes"`
	Nodes             []nodeEnvelope    `json:"nodes"`
	TerminalOutcomes  []string          `json:"terminal_outcomes"`
	BoundedLoops      []json.RawMessage `json:"bounded_loops,omitempty"`
	DefaultLease      map[string]any    `json:"default_lease_policy,omitempty"`
	DefaultRetry      map[string]any    `json:"default_retry_policy,omitempty"`
	CompositionLimits map[string]any    `json:"composition_limits,omitempty"`
}

type nodeEnvelope struct {
	ID           string            `json:"id"`
	Name         string            `json:"name,omitempty"`
	Dependencies []string          `json:"dependencies,omitempty"`
	Outcomes     []json.RawMessage `json:"outcomes,omitempty"`
	Branches     map[string]string `json:"branches,omitempty"`
	Action       json.RawMessage   `json:"action"`
	Assignment   json.RawMessage   `json:"assignment,omitempty"`
	Completion   json.RawMessage   `json:"completion,omitempty"`
	Outputs      []json.RawMessage `json:"outputs,omitempty"`
	Gates        []json.RawMessage `json:"gates,omitempty"`
	RetryPolicy  json.RawMessage   `json:"retry_policy,omitempty"`
	LoopID       string            `json:"loop_id,omitempty"`
	Checkpoint   json.RawMessage   `json:"checkpoint,omitempty"`
	LeasePolicy  json.RawMessage   `json:"lease_policy,omitempty"`
	SkipRule     json.RawMessage   `json:"skip_rule,omitempty"`
	WaiveRules   []json.RawMessage `json:"waive_rules,omitempty"`
}

var actionKinds = map[string]struct{}{
	"run":      {},
	"loop":     {},
	"team":     {},
	"manual":   {},
	"external": {},
	"workflow": {},
}

const (
	maxTemplateBytes  = 4 << 20
	maxJSONDepth      = 64
	maxContainerItems = 10_000
)

// ValidateTemplateJSON strictly decodes a workflow template, evaluates the
// generated portable Umpire contract, and applies the Stage 1 wire checks.
// Graph and context-dependent validation is added after the canonical model
// lands in Stages 2 and 3.
func ValidateTemplateJSON(data []byte) error {
	var template templateEnvelope
	if err := decodeStrict(data, &template); err != nil {
		return fmt.Errorf("invalid workflow template: %w", err)
	}

	schemaVersion, err := exactSchemaVersion(template.SchemaVersion)
	if err != nil {
		return fmt.Errorf("invalid workflow template: schema_version: %w", err)
	}

	fields := WorkflowFields{
		BoundedLoops:       projectIDs(template.BoundedLoops),
		CompositionLimits:  template.CompositionLimits,
		DefaultLeasePolicy: template.DefaultLease,
		DefaultRetryPolicy: template.DefaultRetry,
		EntryNodes:         template.EntryNodes,
		Metadata:           template.Metadata,
		Nodes:              projectNodeIDs(template.Nodes),
		SchemaVersion:      schemaVersion,
		TemplateId:         stringValue(template.TemplateID),
		TemplateVersion:    stringValue(template.TemplateVersion),
		TerminalOutcomes:   template.TerminalOutcomes,
	}
	availability := Check(fields, WorkflowConditions{}, WorkflowFields{})
	for _, field := range []struct {
		name   string
		status FieldStatus
	}{
		{name: "schema_version", status: availability.SchemaVersion},
		{name: "template_id", status: availability.TemplateId},
		{name: "template_version", status: availability.TemplateVersion},
		{name: "metadata", status: availability.Metadata},
		{name: "entry_nodes", status: availability.EntryNodes},
		{name: "nodes", status: availability.Nodes},
		{name: "terminal_outcomes", status: availability.TerminalOutcomes},
		{name: "bounded_loops", status: availability.BoundedLoops},
		{name: "default_lease_policy", status: availability.DefaultLeasePolicy},
		{name: "default_retry_policy", status: availability.DefaultRetryPolicy},
		{name: "composition_limits", status: availability.CompositionLimits},
	} {
		if err := validateFieldStatus(field.name, field.status); err != nil {
			return err
		}
	}

	if strings.TrimSpace(template.TemplateID) == "" {
		return fmt.Errorf("invalid workflow template: template_id is required")
	}
	if strings.TrimSpace(template.TemplateVersion) == "" {
		return fmt.Errorf("invalid workflow template: template_version is required")
	}

	for index, node := range template.Nodes {
		if strings.TrimSpace(node.ID) == "" {
			return fmt.Errorf("invalid workflow template: nodes[%d].id is required", index)
		}
		if len(node.Action) == 0 || string(node.Action) == "null" {
			return fmt.Errorf("invalid workflow template: node %q action is required", node.ID)
		}
		var actionFields map[string]json.RawMessage
		if err := json.Unmarshal(node.Action, &actionFields); err != nil {
			return fmt.Errorf("invalid workflow template: node %q action: %w", node.ID, err)
		}
		typeJSON, ok := actionFields["type"]
		if !ok {
			return fmt.Errorf("invalid workflow template: node %q action.type is required", node.ID)
		}
		var actionType string
		if err := json.Unmarshal(typeJSON, &actionType); err != nil {
			return fmt.Errorf("invalid workflow template: node %q action.type: %w", node.ID, err)
		}
		if strings.TrimSpace(actionType) == "" {
			return fmt.Errorf("invalid workflow template: node %q action.type is required", node.ID)
		}
		if _, ok := actionKinds[actionType]; !ok {
			return fmt.Errorf("invalid workflow template: node %q has unsupported action %q", node.ID, actionType)
		}
	}

	for index, id := range template.EntryNodes {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("invalid workflow template: entry_nodes[%d] is empty", index)
		}
	}
	for index, outcome := range template.TerminalOutcomes {
		if strings.TrimSpace(outcome) == "" {
			return fmt.Errorf("invalid workflow template: terminal_outcomes[%d] is empty", index)
		}
	}
	return nil
}

func decodeStrict(data []byte, target any) error {
	if len(data) > maxTemplateBytes {
		return fmt.Errorf("template exceeds %d-byte limit", maxTemplateBytes)
	}
	if err := preflightJSON(data); err != nil {
		return err
	}
	if err := rejectNonCanonicalKeys(data); err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	return decoder.Decode(target)
}

// preflightJSON bounds work before the typed decoder allocates the template
// envelope and rejects duplicate keys that encoding/json otherwise accepts.
func preflightJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("multiple JSON values")
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds depth limit %d", maxJSONDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}

	switch delim {
	case '{':
		seen := make(map[string]struct{})
		count := 0
		for decoder.More() {
			count++
			if count > maxContainerItems {
				return fmt.Errorf("JSON object exceeds %d-member limit", maxContainerItems)
			}
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("JSON object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closeToken, err := decoder.Token()
		if err != nil {
			return err
		}
		if closeToken != json.Delim('}') {
			return fmt.Errorf("malformed JSON object")
		}
	case '[':
		count := 0
		for decoder.More() {
			count++
			if count > maxContainerItems {
				return fmt.Errorf("JSON array exceeds %d-item limit", maxContainerItems)
			}
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closeToken, err := decoder.Token()
		if err != nil {
			return err
		}
		if closeToken != json.Delim(']') {
			return fmt.Errorf("malformed JSON array")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	return nil
}

func rejectNonCanonicalKeys(data []byte) error {
	var templateFields map[string]json.RawMessage
	if err := json.Unmarshal(data, &templateFields); err != nil {
		return err
	}
	if err := requireCanonicalKeys(templateFields, []string{
		"schema_version", "template_id", "template_version", "metadata",
		"entry_nodes", "nodes", "terminal_outcomes", "bounded_loops",
		"default_lease_policy", "default_retry_policy", "composition_limits",
	}); err != nil {
		return err
	}

	nodesJSON, ok := templateFields["nodes"]
	if !ok {
		return nil
	}
	var nodes []json.RawMessage
	if err := json.Unmarshal(nodesJSON, &nodes); err != nil {
		return nil // The typed decoder reports the more specific type error.
	}
	for index, nodeJSON := range nodes {
		var nodeFields map[string]json.RawMessage
		if err := json.Unmarshal(nodeJSON, &nodeFields); err != nil {
			continue // The typed decoder reports the more specific type error.
		}
		if err := requireCanonicalKeys(nodeFields, []string{
			"id", "name", "dependencies", "outcomes", "branches", "action",
			"assignment", "completion", "outputs", "gates", "retry_policy",
			"loop_id", "checkpoint", "lease_policy", "skip_rule", "waive_rules",
		}); err != nil {
			return fmt.Errorf("nodes[%d]: %w", index, err)
		}
		actionJSON, ok := nodeFields["action"]
		if !ok {
			continue
		}
		var actionFields map[string]json.RawMessage
		if json.Unmarshal(actionJSON, &actionFields) == nil {
			if err := requireCanonicalKeys(actionFields, []string{"type"}); err != nil {
				return fmt.Errorf("nodes[%d].action: %w", index, err)
			}
		}
	}
	return nil
}

func requireCanonicalKeys(fields map[string]json.RawMessage, canonical []string) error {
	for key, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("field %q cannot be null", key)
		}
		for _, expected := range canonical {
			if key != expected && strings.EqualFold(key, expected) {
				return fmt.Errorf("non-canonical field %q; use %q", key, expected)
			}
		}
	}
	return nil
}

func validateFieldStatus(name string, status FieldStatus) error {
	if status.Required && !status.Satisfied {
		return fmt.Errorf("invalid workflow template: %s is required", name)
	}
	if !status.Enabled || !status.Fair {
		if status.Reason != nil {
			return fmt.Errorf("invalid workflow template: %s: %s", name, *status.Reason)
		}
		return fmt.Errorf("invalid workflow template: %s is unavailable", name)
	}
	if status.Valid != nil && !*status.Valid {
		if status.Error != "" {
			return fmt.Errorf("invalid workflow template: %s: %s", name, status.Error)
		}
		return fmt.Errorf("invalid workflow template: %s is invalid", name)
	}
	return nil
}

func projectNodeIDs(nodes []nodeEnvelope) []string {
	ids := make([]string, len(nodes))
	for index := range nodes {
		ids[index] = nodes[index].ID
	}
	return ids
}

func projectIDs(values []json.RawMessage) []string {
	ids := make([]string, 0, len(values))
	for _, value := range values {
		var named struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(value, &named) == nil {
			ids = append(ids, named.ID)
		}
	}
	return ids
}

func stringValue(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func exactSchemaVersion(value json.RawMessage) (*float64, error) {
	value = bytes.TrimSpace(value)
	if len(value) == 0 {
		return nil, nil
	}
	if len(value) > 64 || !isJSONNumberStart(value[0]) {
		return nil, fmt.Errorf("workflow schema_version must be the JSON number 1")
	}
	if exponentIndex := bytes.IndexAny(value, "eE"); exponentIndex >= 0 {
		exponent, err := strconv.ParseInt(string(value[exponentIndex+1:]), 10, 16)
		if err != nil || exponent < -1024 || exponent > 1024 {
			return nil, fmt.Errorf("workflow schema_version must be the JSON number 1")
		}
	}
	rational, ok := new(big.Rat).SetString(string(value))
	if !ok || rational.Cmp(big.NewRat(1, 1)) != 0 {
		return nil, fmt.Errorf("workflow schema_version must be 1")
	}
	version := 1.0
	return &version, nil
}

func isJSONNumberStart(value byte) bool {
	return value == '-' || value >= '0' && value <= '9'
}
