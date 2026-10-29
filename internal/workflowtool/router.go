package workflowtool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ActionApplicability is the per-action input contract. It is the one table
// both the Go and the TypeScript router validate against; the JSON Schema
// variants are proven equal to it by TestSchemaMatchesRouteTable.
type ActionApplicability struct {
	Permitted   []string                       // every property the action accepts, including "action"
	Required    []string                       // the subset that must be present
	Types       map[string]string              // declared wire type per permitted property: string, integer, object, array or json
	Enums       map[string][]string            // declared closed value set per property
	Constraints map[string]PropertyConstraints // declared limits per property
}

// PropertyConstraints is one property's declared limits, compiled from the
// published JSON Schema so the router and the schema enforce the same rule from
// one declaration. A zero length means no length limit; HasMinimum and
// HasMaximum keep a declared bound of zero distinct from an absent one.
type PropertyConstraints struct {
	MinLength  int
	MaxLength  int
	Pattern    string
	Minimum    int64 // inclusive lower bound for an integer property
	HasMinimum bool
	Maximum    int64 // inclusive upper bound for an integer property
	HasMaximum bool
	ItemType   string // declared wire type of an array property's items
}

// Result is one tool call's outcome: the action that ran and the control
// method's result object, unchanged. The surface renders Value as its
// structured content and Text as its human content.
type Result struct {
	Action string
	Value  map[string]any
}

// Text renders the result object as indented JSON, the shape every surface
// publishes as the tool's text content.
func (r Result) Text() string {
	data, err := json.MarshalIndent(r.Value, "", "  ")
	if err != nil {
		return ""
	}
	return string(data)
}

// ToolError is a typed tool denial. Code is one of the fixed error codes; the
// message is the rendered template.
type ToolError struct {
	Code    string
	Message string
	Facts   map[string]string
}

// ToolCode lets a consumer outside this package classify the error through a
// local interface instead of importing internal/workflowtool.
func (e *ToolError) ToolCode() string { return e.Code }

func (e *ToolError) Error() string { return e.Message }

// Client is the control surface the router dispatches to. It is the workflow
// subset of the supervisor control client, so the MCP server and the tests can
// supply their own implementation.
type Client interface {
	WorkflowStatus(workflowID string) (map[string]any, error)
	WorkflowWait(workflowID string, timeout time.Duration) (map[string]any, error)
	WorkflowInspect(workflowID string) (map[string]any, error)
	WorkflowEvents(workflowID string, afterSeq int64, limit int) (map[string]any, error)
	WorkflowComplete(workflowID string, fields map[string]any) (map[string]any, error)
	WorkflowGate(workflowID string, fields map[string]any) (map[string]any, error)
	WorkflowCancel(workflowID, actor, reason string) (map[string]any, error)
	WorkflowControllerStatus(controllerID string) (map[string]any, error)
	WorkflowControllerList() (map[string]any, error)
	WorkflowReady(controllerID string, limit int) (map[string]any, error)
}

// Options carries the surface-specific policy the router applies. The zero
// value is the unbounded policy the TypeScript surfaces use.
type Options struct {
	// ClampWait bounds a wait request. Nil means no bound. The returned bool
	// reports whether the budget bit, which marks a timed-out result as clamped.
	ClampWait func(time.Duration) (time.Duration, bool)
}

// Execute routes one avenor_workflow call: decode, validate against the
// action's applicability entry, then dispatch to the control client.
func Execute(client Client, input json.RawMessage, opts Options) (Result, error) {
	fields, err := decodeObject(input)
	if err != nil {
		return Result{}, invalidAction()
	}
	action, ok := fieldString(fields, "action")
	if !ok {
		return Result{}, invalidAction()
	}
	entry, known := workflowApplicability[action]
	if !known {
		return Result{}, invalidAction()
	}
	if detail := validateFields(fields, entry); detail != "" {
		return Result{}, invalidRequest(action, detail)
	}

	var typed workflowInput
	if err := json.Unmarshal(input, &typed); err != nil {
		return Result{}, invalidRequest(action, "input is not a valid request object")
	}

	switch action {
	case actionStatus:
		return call(action, func() (map[string]any, error) {
			return client.WorkflowStatus(typed.WorkflowID)
		})
	case actionInspect:
		return call(action, func() (map[string]any, error) {
			return client.WorkflowInspect(typed.WorkflowID)
		})
	case actionEvents:
		afterSeq, _ := fieldInt64(fields, "after_seq")
		limit, _ := fieldInt(fields, "limit")
		return call(action, func() (map[string]any, error) {
			return client.WorkflowEvents(typed.WorkflowID, afterSeq, limit)
		})
	case actionWait:
		return wait(client, typed, opts)
	case actionReady:
		limit, _ := fieldInt(fields, "limit")
		return call(action, func() (map[string]any, error) {
			return client.WorkflowReady(typed.ControllerID, limit)
		})
	case actionControllerStatus:
		if typed.ControllerID == "" {
			return call(action, client.WorkflowControllerList)
		}
		return call(action, func() (map[string]any, error) {
			return client.WorkflowControllerStatus(typed.ControllerID)
		})
	case actionGate:
		return gate(client, typed)
	case actionComplete:
		return complete(client, typed)
	case actionCancel:
		return call(action, func() (map[string]any, error) {
			return client.WorkflowCancel(typed.WorkflowID, typed.Actor, typed.Reason)
		})
	}
	return Result{}, invalidAction()
}

func call(action string, work func() (map[string]any, error)) (Result, error) {
	value, err := work()
	if err != nil {
		return Result{}, refused(err)
	}
	return Result{Action: action, Value: value}, nil
}

func wait(client Client, input workflowInput, opts Options) (Result, error) {
	requested := 30 * time.Second
	if input.Timeout != "" {
		seconds, err := parseTimeoutSeconds(input.Timeout)
		if err != nil {
			return Result{}, invalidRequest(actionWait, Repair("invalid_timeout", map[string]string{"value": quote(input.Timeout)}))
		}
		requested = time.Duration(seconds) * time.Second
	}
	effective := requested
	clamped := false
	if opts.ClampWait != nil {
		effective, clamped = opts.ClampWait(requested)
	}
	value, err := client.WorkflowWait(input.WorkflowID, effective)
	if err != nil {
		return Result{}, refused(err)
	}
	if clamped {
		timedOut, _ := value["timed_out"].(bool)
		terminal, _ := value["terminal"].(bool)
		if timedOut && !terminal {
			value["wait_clamped"] = true
		}
	}
	return Result{Action: actionWait, Value: value}, nil
}

func gate(client Client, input workflowInput) (Result, error) {
	fields := map[string]any{
		"node_id":       input.NodeID,
		"activation_id": input.ActivationID,
		"gate_id":       input.GateID,
		"operation":     input.Operation,
	}
	for key, value := range map[string]string{
		"actor":         input.Actor,
		"reason":        input.Reason,
		"outcome":       input.Outcome,
		"poll_id":       input.PollID,
		"source":        input.Source,
		"result":        input.Result,
		"response_hash": input.ResponseHash,
	} {
		if value != "" {
			fields[key] = value
		}
	}
	if input.ObservedAt != "" {
		if _, err := time.Parse(time.RFC3339, input.ObservedAt); err != nil {
			return Result{}, invalidRequest(actionGate, Repair("invalid_observed_at", map[string]string{"name": quote("observed_at")}))
		}
		fields["observed_at"] = input.ObservedAt
	}
	if len(input.Subject) > 0 {
		var subject any
		if err := json.Unmarshal(input.Subject, &subject); err != nil {
			return Result{}, invalidRequest(actionGate, "subject must be valid JSON")
		}
		fields["subject"] = subject
	}
	if len(input.EvidenceIDs) > 0 {
		fields["evidence_ids"] = input.EvidenceIDs
	}
	return call(actionGate, func() (map[string]any, error) {
		return client.WorkflowGate(input.WorkflowID, fields)
	})
}

func complete(client Client, input workflowInput) (Result, error) {
	fields := map[string]any{
		"node_id":       input.NodeID,
		"activation_id": input.ActivationID,
		"attempt_id":    input.AttemptID,
		"lease_id":      input.LeaseID,
		"owner_token":   input.OwnerToken,
		"outcome":       input.Outcome,
	}
	if len(input.Outputs) > 0 {
		var outputs any
		if err := json.Unmarshal(input.Outputs, &outputs); err != nil {
			return Result{}, invalidRequest(actionComplete, "outputs must be valid JSON")
		}
		fields["outputs"] = outputs
	}
	if len(input.Artifacts) > 0 {
		var artifacts any
		if err := json.Unmarshal(input.Artifacts, &artifacts); err != nil {
			return Result{}, invalidRequest(actionComplete, "artifacts must be valid JSON")
		}
		fields["artifacts"] = artifacts
	}
	return call(actionComplete, func() (map[string]any, error) {
		return client.WorkflowComplete(input.WorkflowID, fields)
	})
}

// workflowInput is the decoded avenor_workflow call. Admission has already been
// decided against the applicability table, so every field here is well typed.
type workflowInput struct {
	Action       string          `json:"action"`
	WorkflowID   string          `json:"workflow_id"`
	ControllerID string          `json:"controller_id"`
	SupervisorID string          `json:"supervisor_id"`
	Timeout      string          `json:"timeout"`
	NodeID       string          `json:"node_id"`
	GateID       string          `json:"gate_id"`
	ActivationID string          `json:"activation_id"`
	AttemptID    string          `json:"attempt_id"`
	LeaseID      string          `json:"lease_id"`
	OwnerToken   string          `json:"owner_token"`
	Operation    string          `json:"operation"`
	Actor        string          `json:"actor"`
	Reason       string          `json:"reason"`
	Outcome      string          `json:"outcome"`
	Subject      json.RawMessage `json:"subject"`
	PollID       string          `json:"poll_id"`
	Source       string          `json:"source"`
	Result       string          `json:"result"`
	ResponseHash string          `json:"response_hash"`
	ObservedAt   string          `json:"observed_at"`
	EvidenceIDs  []string        `json:"evidence_ids"`
	Outputs      json.RawMessage `json:"outputs"`
	Artifacts    json.RawMessage `json:"artifacts"`
}

// SupervisorIDOf returns the supervisor_id a call names, or the empty string.
// The surface reads it before dispatch to acquire the right control client; the
// router validates it as a string but never uses it.
func SupervisorIDOf(input json.RawMessage) string {
	var typed workflowInput
	_ = json.Unmarshal(input, &typed)
	return typed.SupervisorID
}

// ActionOf returns the action a call names, or the empty string when the input
// is not an object with a string action.
func ActionOf(input json.RawMessage) string {
	fields, err := decodeObject(input)
	if err != nil {
		return ""
	}
	action, _ := fieldString(fields, "action")
	return action
}

// InputSchema returns the avenor_workflow input schema as a JSON value.
func InputSchema() map[string]any { return decodeSchema(workflowInputSchemaJSON) }

// InputFlatSchema returns the flat object schema MCP surfaces publish for
// avenor_workflow: it is compiled from the per-action union, keeps every
// action's properties and their declared types, enums and limits, and requires
// only the keys every action requires — the action itself.
func InputFlatSchema() map[string]any { return decodeSchema(workflowInputFlatSchemaJSON) }

// ApplicabilityTable returns the per-action input contract.
func ApplicabilityTable() map[string]ActionApplicability { return workflowApplicability }

// Repair renders one repair template with its facts. An unknown code returns
// the code itself, so a missing template is visible rather than silent.
func Repair(code string, facts map[string]string) string {
	template, ok := repairsTemplates[code]
	if !ok {
		return code
	}
	out := template
	for name, value := range facts {
		out = strings.ReplaceAll(out, "{"+name+"}", value)
	}
	return out
}

func decodeSchema(raw string) map[string]any {
	var schema map[string]any
	if err := json.Unmarshal([]byte(raw), &schema); err != nil {
		panic(err)
	}
	return schema
}

func decodeObject(input json.RawMessage) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(input, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		fields = map[string]json.RawMessage{}
	}
	return fields, nil
}

func fieldString(fields map[string]json.RawMessage, name string) (string, bool) {
	raw, ok := fields[name]
	if !ok {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

func fieldInt64(fields map[string]json.RawMessage, name string) (int64, bool) {
	raw, ok := fields[name]
	if !ok {
		return 0, false
	}
	rational, ok := exactInteger(raw)
	if !ok || !rational.Num().IsInt64() {
		return 0, false
	}
	return rational.Num().Int64(), true
}

func fieldInt(fields map[string]json.RawMessage, name string) (int, bool) {
	value, ok := fieldInt64(fields, name)
	return int(value), ok
}

// validateFields applies one action's applicability entry in the fixed order:
// a property outside Permitted, then a missing Required property, then a value
// whose wire type, enum or declared constraint is not met. The returned
// sentence is the invalid_request template's {detail} fact, rendered from the
// shared repairs table so no failure sentence is composed here.
func validateFields(fields map[string]json.RawMessage, entry ActionApplicability) string {
	permitted := make(map[string]bool, len(entry.Permitted))
	for _, name := range entry.Permitted {
		permitted[name] = true
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !permitted[name] {
			return Repair("unknown_property", map[string]string{"name": quote(name)})
		}
	}
	for _, name := range entry.Required {
		if _, ok := fields[name]; !ok {
			return Repair("missing_required_property", map[string]string{"name": quote(name)})
		}
	}
	for _, name := range entry.Permitted {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		kind := entry.Types[name]
		if !hasWireType(raw, kind) {
			switch kind {
			case "integer":
				return Repair("wrong_type_integer", map[string]string{"name": quote(name)})
			case "object":
				return Repair("wrong_type_object", map[string]string{"name": quote(name)})
			case "array":
				return Repair("wrong_type_array", map[string]string{"name": quote(name)})
			default:
				return Repair("wrong_type_string", map[string]string{"name": quote(name)})
			}
		}
		constraint := entry.Constraints[name]
		if kind == "array" && constraint.ItemType != "" {
			if detail := validateArrayItems(name, raw, constraint.ItemType); detail != "" {
				return detail
			}
		}
		if values, ok := entry.Enums[name]; ok {
			value, _ := fieldString(fields, name)
			if !contains(values, value) {
				return Repair("invalid_enum", map[string]string{
					"name":   quote(name),
					"values": quoteList(values),
				})
			}
		}
		switch kind {
		case "string":
			value, _ := fieldString(fields, name)
			if detail := validateStringConstraint(name, value, constraint); detail != "" {
				return detail
			}
		case "integer":
			if detail := validateIntegerConstraint(name, raw, constraint); detail != "" {
				return detail
			}
		}
	}
	return ""
}

func hasWireType(raw json.RawMessage, kind string) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	switch kind {
	case "json":
		return true
	case "string":
		_, ok := value.(string)
		return ok
	case "integer":
		rational, ok := exactInteger(raw)
		return ok && rational.Num().IsInt64()
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	}
	return false
}

// exactInteger returns raw's exact rational value when it is a mathematical
// integer, so an integer-valued exponential such as 1e3 is admitted.
func exactInteger(raw json.RawMessage) (*big.Rat, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, false
	}
	number, ok := value.(json.Number)
	if !ok {
		return nil, false
	}
	rational, ok := new(big.Rat).SetString(number.String())
	if !ok || !rational.IsInt() {
		return nil, false
	}
	return rational, true
}

func validateArrayItems(name string, raw json.RawMessage, itemType string) string {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return Repair("wrong_type_array", map[string]string{"name": quote(name)})
	}
	for _, item := range items {
		if !hasWireType(item, itemType) {
			return Repair("array_item_type", map[string]string{"name": quote(name)})
		}
	}
	return ""
}

func validateStringConstraint(name, value string, constraint PropertyConstraints) string {
	length := utf8.RuneCountInString(value)
	if constraint.MinLength > 0 && length < constraint.MinLength {
		return Repair("string_too_short", map[string]string{"name": quote(name), "min": fmt.Sprint(constraint.MinLength)})
	}
	if constraint.MaxLength > 0 && length > constraint.MaxLength {
		return Repair("string_too_long", map[string]string{"name": quote(name), "max": fmt.Sprint(constraint.MaxLength)})
	}
	if constraint.Pattern != "" {
		re, err := regexp.Compile(constraint.Pattern)
		if err == nil && !re.MatchString(value) {
			return Repair("string_pattern", map[string]string{"name": quote(name)})
		}
	}
	return ""
}

func validateIntegerConstraint(name string, raw json.RawMessage, constraint PropertyConstraints) string {
	rational, ok := exactInteger(raw)
	if !ok || !rational.Num().IsInt64() {
		return Repair("wrong_type_integer", map[string]string{"name": quote(name)})
	}
	value := rational.Num().Int64()
	if constraint.HasMinimum && value < constraint.Minimum {
		return Repair("integer_too_small", map[string]string{"name": quote(name), "min": fmt.Sprint(constraint.Minimum)})
	}
	if constraint.HasMaximum && value > constraint.Maximum {
		return Repair("integer_too_large", map[string]string{"name": quote(name), "max": fmt.Sprint(constraint.Maximum)})
	}
	return ""
}

func invalidAction() *ToolError {
	return &ToolError{Code: "invalid_action", Message: Repair("workflow_invalid_action", nil), Facts: map[string]string{}}
}

func invalidRequest(action, detail string) *ToolError {
	facts := map[string]string{"action": quote(action), "detail": detail}
	return &ToolError{Code: "invalid_request", Message: Repair("invalid_request", facts), Facts: facts}
}

func refused(err error) *ToolError {
	return &ToolError{Code: "refused", Message: err.Error(), Facts: map[string]string{}}
}

func quote(value string) string { return strconv.Quote(value) }

func quoteList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = strconv.Quote(value)
	}
	return strings.Join(quoted, ", ")
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

var timeoutRE = regexp.MustCompile(`^(\d+)(s|m|h)?$`)

func parseTimeoutSeconds(value string) (int, error) {
	trimmed := strings.TrimSpace(value)
	match := timeoutRE.FindStringSubmatch(trimmed)
	if match == nil {
		return 0, fmt.Errorf("invalid timeout: %s", value)
	}
	amount, err := strconv.Atoi(match[1])
	if err != nil || amount <= 0 {
		return 0, fmt.Errorf("invalid timeout: %s", value)
	}
	switch match[2] {
	case "m":
		return amount * 60, nil
	case "h":
		return amount * 3600, nil
	default:
		return amount, nil
	}
}

// The nine workflow actions. The spellings are the schema's action enum.
const (
	actionStatus           = "status"
	actionInspect          = "inspect"
	actionEvents           = "events"
	actionWait             = "wait"
	actionReady            = "ready"
	actionControllerStatus = "controller_status"
	actionGate             = "gate"
	actionComplete         = "complete"
	actionCancel           = "cancel"
)
