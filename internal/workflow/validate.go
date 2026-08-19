package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

const (
	maxTemplateBytes  = 4 << 20
	maxJSONDepth      = 64
	maxContainerItems = 10_000
)

// ValidateTemplateJSON strictly decodes and validates a workflow template.
func ValidateTemplateJSON(data []byte) error {
	// 1. Bounds and lexical structure first, so their exact messages win and no
	// work is spent parsing hostile input beyond the strict limits.
	if err := preflightJSON(data); err != nil {
		return fmt.Errorf("invalid workflow template: %w", err)
	}
	// 2. Canonical-key case-fold rejection, top-level/nested null rejection, and
	// the schema_version == JSON integer 1 canonical form. These must run before
	// the generated structural validator so their specific messages are kept.
	if err := rejectNonCanonicalKeys(data); err != nil {
		return fmt.Errorf("invalid workflow template: %w", err)
	}

	// 3. The generated Profile structural validator is now the authority for
	// additionalProperties, required, type mismatches, and the action
	// discriminator. Issues rooted at the arbitrary-JSON leaves that typed Go
	// governs are dropped so the closed Profile vocabulary does not govern them.
	issues, err := ValidateWorkflowProfileJSON(data)
	if err != nil {
		return fmt.Errorf("invalid workflow template: %w", err)
	}
	reported := issues[:0]
	for _, issue := range issues {
		if !isOpenLeafPath(issue.Path) {
			reported = append(reported, issue)
		}
	}
	if len(reported) > 0 {
		parts := make([]string, 0, len(reported))
		for _, issue := range reported {
			parts = append(parts, fmt.Sprintf("%s at %s", issue.Code, issue.Path))
		}
		return fmt.Errorf("invalid workflow template: %s", strings.Join(parts, "; "))
	}

	// 4. Strict typed decode as a backstop. model.go's custom Action decoder runs
	// its own checks and must still reject anything that slipped past the
	// structural validator.
	var template Template
	if err := decodeStrict(data, &template); err != nil {
		return fmt.Errorf("invalid workflow template: %w", err)
	}

	// 5. The in-memory rules (Umpire presence/availability plus typed Go checks).
	return ValidateTemplate(template)
}

// isOpenLeafPath reports whether a generated-structural-validator issue path is
// rooted at one of the arbitrary-JSON leaves that typed Go owns exclusively.
// Those leaves are metadata, node branches, the workflow-action outcome_map,
// and the workflow-action input_bindings[*].value. Their structural issues are
// suppressed by path so the closed Profile vocabulary does not reject
// arbitrary-JSON content that only typed Go can interpret.
func isOpenLeafPath(path string) bool {
	segs := strings.Split(path, "/")
	if len(segs) > 0 && segs[0] == "" {
		segs = segs[1:]
	}
	if len(segs) == 0 {
		return false
	}
	if segs[0] == "metadata" {
		return true
	}
	if len(segs) < 3 || segs[0] != "nodes" || !isJSONArrayIndex(segs[1]) {
		return false
	}
	switch {
	case segs[2] == "branches":
		return true
	case len(segs) >= 4 && segs[2] == "action" && segs[3] == "outcome_map":
		return true
	case len(segs) >= 6 && segs[2] == "action" && segs[3] == "input_bindings" && isJSONArrayIndex(segs[4]) && segs[5] == "value":
		return true
	}
	return false
}

func isJSONArrayIndex(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ValidateTemplate evaluates the generated portable Umpire contract and the
// typed structural checks. Stage 3 adds graph- and context-dependent rules.
func ValidateTemplate(template Template) error {
	fields := WorkflowProfileFields{
		// Only required, presence-driven fields are projected; the five optional
		// fields (metadata, bounded_loops, lease/retry policies, composition
		// limits) carry no Umpire validators, so their status is trivially
		// available and typed Go governs their content.
		EntryNodes:       stringSlicePtr(nodeIDs(template.EntryNodes)),
		Nodes:            nodeSlicePtr(len(template.Nodes)),
		SchemaVersion:    integerValue(template.SchemaVersion),
		TemplateId:       stringValue(string(template.TemplateID)),
		TemplateVersion:  stringValue(string(template.TemplateVersion)),
		TerminalOutcomes: stringSlicePtr(outcomeNames(template.TerminalOutcomes)),
	}
	availability := Check(fields, WorkflowProfileConditions{}, WorkflowProfileFields{})
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
	if strings.TrimSpace(string(template.TemplateID)) == "" {
		return fmt.Errorf("invalid workflow template: template_id is required")
	}
	if strings.TrimSpace(string(template.TemplateVersion)) == "" {
		return fmt.Errorf("invalid workflow template: template_version is required")
	}
	for index, node := range template.Nodes {
		if strings.TrimSpace(string(node.ID)) == "" {
			return fmt.Errorf("invalid workflow template: nodes[%d].id is required", index)
		}
		if err := validateAction(node.Action); err != nil {
			return fmt.Errorf("invalid workflow template: node %q: %w", node.ID, err)
		}
	}
	for index, id := range template.EntryNodes {
		if strings.TrimSpace(string(id)) == "" {
			return fmt.Errorf("invalid workflow template: entry_nodes[%d] is empty", index)
		}
	}
	for index, outcome := range template.TerminalOutcomes {
		if strings.TrimSpace(string(outcome)) == "" {
			return fmt.Errorf("invalid workflow template: terminal_outcomes[%d] is empty", index)
		}
	}
	if err := ValidateGraph(template); err != nil {
		return err
	}
	return nil
}

func decodeStrict(data []byte, target any) error {
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
	if len(data) > maxTemplateBytes {
		return fmt.Errorf("template exceeds %d-byte limit", maxTemplateBytes)
	}
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
	if version, ok := templateFields["schema_version"]; ok && !bytes.Equal(bytes.TrimSpace(version), []byte("1")) {
		return fmt.Errorf("workflow schema_version must be the JSON integer 1")
	}
	if err := requireCanonicalTypedValue(data, reflect.TypeOf(Template{}), ""); err != nil {
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

func requireCanonicalActionKeys(data []byte) error {
	return requireCanonicalTypedValue(data, reflect.TypeOf(Action{}), "")
}

func requireCanonicalSnapshotKeys(data []byte) error {
	return requireCanonicalTypedValue(data, reflect.TypeOf(Snapshot{}), "")
}

func requireCanonicalTypedValue(data []byte, valueType reflect.Type, path string) error {
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	if valueType == reflect.TypeOf(json.RawMessage{}) || valueType.Kind() == reflect.Interface {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return atJSONPath(path, fmt.Errorf("value cannot be null"))
	}

	switch valueType.Kind() {
	case reflect.Struct:
		if valueType == reflect.TypeOf(Action{}) {
			var discriminator struct {
				Type ActionKind `json:"type"`
			}
			if json.Unmarshal(data, &discriminator) != nil {
				return nil
			}
			switch discriminator.Type {
			case ActionRun:
				valueType = reflect.TypeOf(RunAction{})
			case ActionLoop:
				valueType = reflect.TypeOf(LoopAction{})
			case ActionTeam:
				valueType = reflect.TypeOf(TeamAction{})
			case ActionManual:
				valueType = reflect.TypeOf(ManualAction{})
			case ActionExternal:
				valueType = reflect.TypeOf(ExternalAction{})
			case ActionWorkflow:
				valueType = reflect.TypeOf(WorkflowAction{})
			default:
				return nil
			}
		}

		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil {
			return nil
		}
		canonical := make([]string, 0, valueType.NumField())
		fieldTypes := make(map[string]reflect.Type, valueType.NumField())
		for index := 0; index < valueType.NumField(); index++ {
			field := valueType.Field(index)
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			canonical = append(canonical, name)
			fieldTypes[name] = field.Type
		}
		if err := requireCanonicalKeys(fields, canonical); err != nil {
			return atJSONPath(path, err)
		}
		for name, raw := range fields {
			fieldType, ok := fieldTypes[name]
			if !ok {
				continue
			}
			if err := requireCanonicalTypedValue(raw, fieldType, appendJSONPath(path, name)); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		var values []json.RawMessage
		if json.Unmarshal(data, &values) != nil {
			return nil
		}
		for index, raw := range values {
			if err := requireCanonicalTypedValue(raw, valueType.Elem(), fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	case reflect.Map:
		if valueType.Key().Kind() != reflect.String {
			return nil
		}
		var values map[string]json.RawMessage
		if json.Unmarshal(data, &values) != nil {
			return nil
		}
		for key, raw := range values {
			if err := requireCanonicalTypedValue(raw, valueType.Elem(), appendJSONPath(path, key)); err != nil {
				return err
			}
		}
	}
	return nil
}

func appendJSONPath(path, field string) string {
	if path == "" {
		return field
	}
	return path + "." + field
}

func atJSONPath(path string, err error) error {
	if path == "" {
		return err
	}
	return fmt.Errorf("%s: %w", path, err)
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

func validateAction(action Action) error {
	variants := 0
	for _, present := range []bool{
		action.Run != nil, action.Loop != nil, action.Team != nil,
		action.Manual != nil, action.External != nil, action.Workflow != nil,
	} {
		if present {
			variants++
		}
	}
	if action.Kind == "" || variants == 0 {
		return fmt.Errorf("action is required")
	}
	if variants != 1 {
		return fmt.Errorf("action %q must contain exactly one variant", action.Kind)
	}
	matches := action.Kind == ActionRun && action.Run != nil ||
		action.Kind == ActionLoop && action.Loop != nil ||
		action.Kind == ActionTeam && action.Team != nil ||
		action.Kind == ActionManual && action.Manual != nil ||
		action.Kind == ActionExternal && action.External != nil ||
		action.Kind == ActionWorkflow && action.Workflow != nil
	if !matches {
		return fmt.Errorf("action %q does not match its variant", action.Kind)
	}

	switch action.Kind {
	case ActionRun:
		hasPrompt := strings.TrimSpace(action.Run.Prompt) != ""
		hasPromptFile := strings.TrimSpace(action.Run.PromptFile) != ""
		if hasPrompt == hasPromptFile {
			return fmt.Errorf("run action requires exactly one of prompt or prompt_file")
		}
	case ActionLoop:
		if strings.TrimSpace(action.Loop.LoopFile) == "" {
			return fmt.Errorf("loop action requires loop_file")
		}
	case ActionTeam:
		if strings.TrimSpace(action.Team.TeamFile) == "" {
			return fmt.Errorf("team action requires team_file")
		}
	case ActionExternal:
		if strings.TrimSpace(action.External.Source) == "" {
			return fmt.Errorf("external action requires source")
		}
	case ActionWorkflow:
		if strings.TrimSpace(string(action.Workflow.TemplateID)) == "" {
			return fmt.Errorf("workflow action requires template_id")
		}
		if strings.TrimSpace(string(action.Workflow.TemplateVersion)) == "" {
			return fmt.Errorf("workflow action requires template_version")
		}
		if strings.TrimSpace(action.Workflow.ChildKey) == "" {
			return fmt.Errorf("workflow action requires child_key")
		}
		if len(action.Workflow.OutcomeMap) == 0 {
			return fmt.Errorf("workflow action requires outcome_map")
		}
		for index, binding := range action.Workflow.InputBindings {
			if strings.TrimSpace(binding.Input) == "" {
				return fmt.Errorf("workflow action input_bindings[%d] requires input", index)
			}
			hasValue := len(bytes.TrimSpace(binding.Value)) != 0
			hasReference := binding.From != nil
			if hasValue == hasReference {
				return fmt.Errorf("workflow action input_bindings[%d] requires exactly one of value or from", index)
			}
			if hasValue {
				if bytes.Equal(bytes.TrimSpace(binding.Value), []byte("null")) {
					return fmt.Errorf("workflow action input_bindings[%d].value cannot be null", index)
				}
				if err := preflightJSON(binding.Value); err != nil {
					return fmt.Errorf("workflow action input_bindings[%d].value: %w", index, err)
				}
			}
			if hasReference {
				if strings.TrimSpace(string(binding.From.NodeID)) == "" {
					return fmt.Errorf("workflow action input_bindings[%d].from requires node_id", index)
				}
				if strings.TrimSpace(string(binding.From.OutputID)) == "" {
					return fmt.Errorf("workflow action input_bindings[%d].from requires output_id", index)
				}
			}
		}
		for index, binding := range action.Workflow.OutputBindings {
			if strings.TrimSpace(binding.ChildOutput) == "" {
				return fmt.Errorf("workflow action output_bindings[%d] requires child_output", index)
			}
			if strings.TrimSpace(binding.ParentOutput) == "" {
				return fmt.Errorf("workflow action output_bindings[%d] requires parent_output", index)
			}
		}
		for child, parent := range action.Workflow.OutcomeMap {
			if strings.TrimSpace(string(child)) == "" {
				return fmt.Errorf("workflow action outcome_map contains a blank child outcome")
			}
			if strings.TrimSpace(string(parent)) == "" {
				return fmt.Errorf("workflow action outcome_map[%q] contains a blank parent outcome", child)
			}
		}
	}
	return nil
}

func definitionIDs(nodes []NodeDefinition) []string {
	ids := make([]string, len(nodes))
	for index := range nodes {
		ids[index] = string(nodes[index].ID)
	}
	return ids
}

func nodeIDs(nodes []NodeID) []string {
	ids := make([]string, len(nodes))
	for index := range nodes {
		ids[index] = string(nodes[index])
	}
	return ids
}

func loopIDs(loops []BoundedLoopDefinition) []string {
	ids := make([]string, len(loops))
	for index := range loops {
		ids[index] = string(loops[index].ID)
	}
	return ids
}

func outcomeNames(outcomes []OutcomeName) []string {
	names := make([]string, len(outcomes))
	for index := range outcomes {
		names[index] = string(outcomes[index])
	}
	return names
}

func compositionFields(limits *CompositionLimits) map[string]any {
	if limits == nil {
		return nil
	}
	return map[string]any{"max_depth": limits.MaximumDepth, "max_children": limits.MaximumChildren}
}

func leaseFields(policy *LeasePolicy) map[string]any {
	if policy == nil {
		return nil
	}
	return map[string]any{"ttl_seconds": policy.TTLSeconds, "heartbeat_interval_seconds": policy.HeartbeatIntervalSeconds}
}

func retryFields(policy *RetryPolicy) map[string]any {
	if policy == nil {
		return nil
	}
	return map[string]any{"max_attempts": policy.MaximumAttempts, "exhaustion": policy.Exhaustion, "outcome": policy.Outcome}
}

func integerValue(value int) *int64 {
	if value == 0 {
		return nil
	}
	converted := int64(value)
	return &converted
}

func stringValue(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringSlicePtr(values []string) *[]string {
	if len(values) == 0 {
		return nil
	}
	copy := append([]string(nil), values...)
	return &copy
}

func nodeSlicePtr(count int) *[]WorkflowProfileNode {
	if count == 0 {
		return nil
	}
	empty := make([]WorkflowProfileNode, count)
	return &empty
}
