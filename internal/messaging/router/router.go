// Package router is the one Go implementation of the merged messaging tools'
// routing: input validation, target resolution, the permission and state rules,
// the provider call, and the outcome text.
//
// Every Go model-facing surface calls Router.Execute for avenor_message and
// ExecutePeers for avenor_peers, so the branch logic exists once and the host
// and the children cannot disagree. The input contract, the repair templates
// and the render templates come from schemas/ (through internal/messaging); the
// two permission reasons and the invalid-action reason come from the generated
// route table. No surface composes a failure sentence of its own.
package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askclient"
)

// Provider is what one surface supplies to the router: the shared transport,
// the caller and surface it speaks as, and the runtime operations only that
// surface can perform.
type Provider interface {
	messaging.Transport
	// Resolve resolves every rung of the shared ladder except the reserved
	// `self` alias, which the router substitutes before calling.
	Resolve(ctx context.Context, ref string) (messaging.Resolution, error)
	Caller() messaging.Caller
	Surface() messaging.Surface
	Role() string
	RunState(ctx context.Context, runID string) (messaging.RunFacts, error)
	Resume(ctx context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error)
	Rename(ctx context.Context, req messaging.RenameRequest) (messaging.RenameResult, error)
	// Last reads the target's recorded last agent message. It is the peers
	// `last` action's only provider call and performs no write.
	Last(ctx context.Context, runID string) (messaging.PeerLast, error)
}

// AskClaimer is implemented only by the surfaces that keep a pending-ask map
// fed by their inbound poll. Surfaces without inbound delivery do not
// implement it, so `reply` is deterministically edge-absent there.
type AskClaimer interface {
	ClaimAsk(messageID string) (askerRunID string, ok bool)
	RestoreAsk(messageID, askerRunID string)
}

// ControlReplier is implemented only by the claude-channel sidecar provider.
type ControlReplier interface {
	ControlReply(ctx context.Context, target string, payload json.RawMessage) error
}

// Result is one tool call's outcome. Text is the (action, outcome) render
// template, already substituted, and is set on every successful call so no
// surface builds a sentence and the caller always learns whether an ask can
// still be withdrawn.
type Result struct {
	Action    string
	Outcome   messaging.Outcome
	MessageID string
	RunID     string // public run id when the surface has one, else the broker run id
	RuntimeID string // broker/runtime id of the resolved target (peers `last`)
	Label     string // the resolved target's display name (SessionInfo.Label)
	Reply     string // the reply body, ask/replied only; empty everywhere else
	Text      string
	// Peers is the peers `list` action's output: one record per visible peer,
	// already decoded from the broker's peer listing. It is empty for every
	// other action, and `list` sets no text.
	Peers []PeerRecord
	// PreviousLabel is the label before a peers `rename`, which the
	// peers_rename:renamed template and the surface's result object report.
	PreviousLabel string
	// peers `last` only; zero values everywhere else.
	FinalOutput string
	StopReason  string
	Truncated   bool
}

// PeerRecord is one peers `list` entry. There is no name key: Label is the
// display name, so the list is also the rename's read path.
type PeerRecord struct {
	RunID   string `json:"run_id"`
	Label   string `json:"label,omitempty"`
	Status  string `json:"status,omitempty"`
	Backend string `json:"backend,omitempty"`
	Model   string `json:"model,omitempty"`
	Dir     string `json:"dir,omitempty"`
	Kind    string `json:"kind,omitempty"`
}

// ToolError is a typed tool denial. Code is one of the fixed error codes; the
// message is the rendered template (or, for a refusal, the provider's text).
type ToolError struct {
	Code    string
	Message string
	Facts   map[string]string
}

// ToolCode lets a consumer outside this package classify the error through a
// local interface instead of importing internal/messaging/router.
func (e *ToolError) ToolCode() string { return e.Code }

func (e *ToolError) Error() string { return e.Message }

// Router routes avenor_message calls through one provider.
type Router struct {
	provider Provider
	client   *askclient.Client
}

// NewRouter builds a router over p. The ask client is bound to the same
// provider and to one options value for the router's lifetime.
func NewRouter(p Provider) *Router {
	return &Router{provider: p, client: askclient.New(p, askclient.Options{})}
}

// Execute routes one avenor_message call. Every call runs four phases and
// stops at the first failure: decode/validate, resolve, permission, then state
// and the provider call.
func (r *Router) Execute(ctx context.Context, input json.RawMessage) (Result, error) {
	fields, err := decodeObject(input)
	if err != nil {
		return Result{}, invalidAction(messaging.MessageRouteFields{})
	}

	action, ok := fieldString(fields, "action")
	if !ok {
		return Result{}, invalidAction(messaging.MessageRouteFields{})
	}
	table := messaging.ApplicabilityTable()
	entry, known := table[action]
	if !known {
		return Result{}, invalidAction(messaging.MessageRouteFields{Action: &action})
	}
	if detail := validateFields(fields, entry); detail != "" {
		return Result{}, invalidRequest(action, detail)
	}

	var typed messageInput
	if err := json.Unmarshal(input, &typed); err != nil {
		return Result{}, invalidRequest(action, "input is not a valid request object")
	}

	if action == actionReply {
		return r.reply(ctx, typed)
	}
	if action == actionWithdraw {
		return r.withdraw(ctx, typed)
	}
	if action == actionControlReply {
		return r.controlReply(ctx, typed)
	}

	res, err := r.resolve(ctx, typed.Target)
	if err != nil {
		return Result{}, err
	}
	facts, err := r.provider.RunState(ctx, res.RunID)
	if err != nil {
		return Result{}, refused(err)
	}
	conditions := messaging.MessageRouteConditions{
		CallerKind:            string(r.provider.Caller().Kind),
		Surface:               string(r.provider.Surface()),
		TargetLive:            facts.Live,
		TargetResumable:       facts.Resumable,
		TargetMatch:           string(res.Match),
		ControlReplySupported: r.provider.Surface() == messaging.SurfaceSidecar,
	}
	if denial := permissionDenial(action, conditions); denial != nil {
		return Result{}, denial
	}
	if denial := stateDenial(action, conditions, res.RunID, string(facts.Status)); denial != nil {
		return Result{}, denial
	}

	switch action {
	case actionTell:
		return r.tell(ctx, typed, res)
	case actionAsk:
		return r.ask(ctx, typed, res)
	case actionResume:
		return r.resume(ctx, typed, res)
	}
	return Result{}, invalidRequest(action, "input is not a valid request object")
}

// permissionDenial reports the phase-3 rows: control_reply support and the
// resume caller rule. Both reasons are read from the generated route table.
func permissionDenial(action string, conditions messaging.MessageRouteConditions) *ToolError {
	if action == actionControlReply && !conditions.ControlReplySupported {
		return permissionError(action, conditions, "unsupported_action")
	}
	if action == actionResume && conditions.CallerKind == string(messaging.CallerBroker) {
		return permissionError(action, conditions, "not_permitted")
	}
	return nil
}

func permissionError(action string, conditions messaging.MessageRouteConditions, code string) *ToolError {
	availability := messaging.Check(messaging.MessageRouteFields{Action: &action}, conditions, messaging.MessageRouteFields{})
	return &ToolError{Code: code, Message: firstReason(availability.Target), Facts: map[string]string{}}
}

// stateDenial reports the phase-4 state rows. Their text is template-rendered,
// not read from the route table, whose reasons for these rows are static and
// never user-visible.
func stateDenial(action string, conditions messaging.MessageRouteConditions, runID, status string) *ToolError {
	if action != actionTell && action != actionAsk && action != actionResume {
		return nil
	}
	facts := map[string]string{"run_id": quote(runID), "status": status}
	switch action {
	case actionTell, actionAsk:
		if conditions.TargetLive {
			return nil
		}
		if conditions.TargetResumable {
			facts["repair"] = `use action "resume" to continue it`
		} else {
			facts["repair"] = "ask the supervisor to continue this run"
		}
		return &ToolError{Code: "wrong_action_for_state", Message: messaging.Repair("wrong_action_for_state", facts), Facts: facts}
	case actionResume:
		if conditions.TargetLive {
			return &ToolError{Code: "target_not_terminal", Message: messaging.Repair("target_not_terminal", facts), Facts: facts}
		}
		if !conditions.TargetResumable {
			return &ToolError{Code: "not_resumable", Message: messaging.Repair("not_resumable", facts), Facts: facts}
		}
	}
	return nil
}

func (r *Router) tell(ctx context.Context, input messageInput, res messaging.Resolution) (Result, error) {
	label := resolutionLabel(res, input.Target)
	result := Result{Action: actionTell, Outcome: messaging.OutcomeNotSent, Label: label}
	pre, err := r.provider.Preflight(ctx)
	if err != nil {
		return Result{}, refused(err)
	}
	msgID, err := messaging.NewMsgID(time.Now(), time.Time{})
	if err != nil {
		result.Text = messaging.RenderOutcome(actionTell, result.Outcome, map[string]string{
			"message_id": quote(""),
			"target":     quote(label),
		})
		return result, nil
	}
	body, err := messaging.SendBody(r.provider.Caller().RunID, res.RunID, input.Message, r.provider.Role())
	if err != nil {
		result.MessageID = msgID
		result.Text = messaging.RenderOutcome(actionTell, result.Outcome, map[string]string{
			"message_id": quote(msgID),
			"target":     quote(label),
		})
		return result, nil
	}
	sent, err := r.client.Send(ctx, messaging.SendRequest{
		From:       messaging.Identity{RunID: r.provider.Caller().RunID},
		To:         messaging.Identity{RunID: res.RunID},
		MessageID:  msgID,
		Generation: pre.Generation,
		Body:       string(body),
		NoReply:    true,
	})
	if err != nil && sent.Outcome == "" {
		return Result{}, refused(err)
	}
	result.MessageID = msgID
	result.Outcome, result.Text = outcomeText(actionTell, sent.Outcome, map[string]string{
		"message_id": quote(msgID),
		"target":     quote(label),
	})
	return result, nil
}

func (r *Router) ask(ctx context.Context, input messageInput, res messaging.Resolution) (Result, error) {
	label := resolutionLabel(res, input.Target)
	result := Result{Action: actionAsk, Label: label}
	pre, err := r.provider.Preflight(ctx)
	if err != nil {
		return Result{}, refused(err)
	}
	from := messaging.Identity{RunID: r.provider.Caller().RunID}
	send, wait, err := r.client.AskRequest(from, from.RunID, res.RunID, input.Message, r.provider.Role())
	if err != nil {
		result.Outcome = messaging.OutcomeNotSent
		result.MessageID = send.MessageID
		result.Text = messaging.RenderOutcome(actionAsk, result.Outcome, map[string]string{
			"message_id": quote(send.MessageID),
			"target":     quote(label),
		})
		return result, nil
	}
	send.Generation = pre.Generation
	wait.Generation = pre.Generation
	wait.Timeout = time.Duration(input.TimeoutMS) * time.Millisecond

	answered, askErr := r.client.Ask(ctx, send, wait)
	if askErr != nil {
		var clientErr *askclient.Error
		if !errors.As(askErr, &clientErr) {
			return Result{}, refused(askErr)
		}
		result.MessageID = clientErr.MessageID
		result.Outcome, result.Text = outcomeText(actionAsk, clientErr.Outcome, map[string]string{
			"message_id": quote(result.MessageID),
			"target":     quote(label),
		})
		return result, nil
	}
	result.Outcome = answered.Outcome
	result.MessageID = answered.MessageID
	reply, renderErr := askclient.RenderReply(messaging.WaitResult{Outcome: answered.Outcome, Reply: answered.Reply})
	if renderErr != nil {
		return Result{}, refused(renderErr)
	}
	result.Reply = reply
	result.Outcome, result.Text = outcomeText(actionAsk, answered.Outcome, map[string]string{
		"message_id": quote(result.MessageID),
		"target":     quote(label),
		"reply":      reply,
	})
	return result, nil
}

func (r *Router) reply(ctx context.Context, input messageInput) (Result, error) {
	result := Result{Action: actionReply, MessageID: input.ReplyTo}
	facts := map[string]string{"message_id": quote(input.ReplyTo)}
	edgeAbsent := func() (Result, error) {
		result.Outcome = messaging.OutcomeEdgeAbsent
		result.Text = messaging.RenderOutcome(actionReply, result.Outcome, facts)
		return result, nil
	}
	claimer, ok := r.provider.(AskClaimer)
	if !ok {
		return edgeAbsent()
	}
	asker, claimed := claimer.ClaimAsk(input.ReplyTo)
	if !claimed {
		return edgeAbsent()
	}
	body, err := json.Marshal(struct {
		Message string `json:"message"`
		Role    string `json:"role"`
	}{Message: input.Message, Role: r.provider.Role()})
	if err != nil {
		claimer.RestoreAsk(input.ReplyTo, asker)
		return Result{}, refused(err)
	}
	sent, err := r.provider.Reply(ctx, messaging.ReplyRequest{
		From:          messaging.Identity{RunID: r.provider.Caller().RunID},
		ToRunID:       asker,
		MessageID:     input.ReplyTo,
		CorrelationID: input.ReplyTo,
		Body:          string(body),
	})
	if err != nil {
		claimer.RestoreAsk(input.ReplyTo, asker)
	}
	result.Outcome, result.Text = outcomeText(actionReply, sent.Outcome, facts)
	return result, nil
}

func (r *Router) withdraw(ctx context.Context, input messageInput) (Result, error) {
	result := Result{Action: actionWithdraw, MessageID: input.MessageID}
	facts := map[string]string{"message_id": quote(input.MessageID)}
	pre, err := r.provider.Preflight(ctx)
	if err != nil {
		return Result{}, refused(err)
	}
	withdrawn, err := r.provider.Withdraw(ctx, messaging.WithdrawRequest{
		Sender:     messaging.Identity{RunID: r.provider.Caller().RunID},
		MessageID:  input.MessageID,
		Generation: pre.Generation,
	})
	if err != nil && withdrawn.Outcome == "" {
		return Result{}, refused(err)
	}
	result.Outcome, result.Text = outcomeText(actionWithdraw, withdrawn.Outcome, facts)
	return result, nil
}

func (r *Router) resume(ctx context.Context, input messageInput, res messaging.Resolution) (Result, error) {
	// RunID, SentinelFile and EventLog stay empty on purpose: the provider mints
	// them and fills them into the control call.
	resumed, err := r.provider.Resume(ctx, messaging.ResumeRequest{
		Target:         res.RunID,
		Message:        input.Message,
		Label:          input.Label,
		IdempotencyKey: input.IdempotencyKey,
	})
	if err != nil {
		return Result{}, asToolError(err)
	}
	result := Result{
		Action:  actionResume,
		Outcome: outcomeResumed,
		RunID:   resumed.RunID,
		Label:   resumed.Label,
	}
	result.Text = messaging.RenderOutcome(actionResume, result.Outcome, map[string]string{
		"run_id": quote(result.RunID),
		"label":  quote(result.Label),
	})
	return result, nil
}

func (r *Router) controlReply(ctx context.Context, input messageInput) (Result, error) {
	conditions := messaging.MessageRouteConditions{
		CallerKind:            string(r.provider.Caller().Kind),
		Surface:               string(r.provider.Surface()),
		ControlReplySupported: r.provider.Surface() == messaging.SurfaceSidecar,
	}
	if !conditions.ControlReplySupported {
		return Result{}, permissionError(actionControlReply, conditions, "unsupported_action")
	}
	replier, ok := r.provider.(ControlReplier)
	if !ok {
		return Result{}, permissionError(actionControlReply, conditions, "unsupported_action")
	}
	payload := input.Payload
	if len(bytes.TrimSpace(payload)) == 0 {
		payload = json.RawMessage("{}")
	}
	if err := replier.ControlReply(ctx, input.Target, payload); err != nil {
		return Result{}, refused(err)
	}
	result := Result{Action: actionControlReply, Outcome: messaging.OutcomeSent}
	result.Text = messaging.RenderOutcome(actionControlReply, result.Outcome, map[string]string{
		"target": quote(input.Target),
	})
	return result, nil
}

// resolve substitutes the reserved `self` alias and hands the rest of the
// ladder to the provider, then turns a non-unique match into its typed error.
func (r *Router) resolve(ctx context.Context, ref string) (messaging.Resolution, error) {
	return resolveTarget(ctx, r.provider, ref)
}

// resolveTarget is the one resolution entry point: rung 1 (the reserved `self`
// alias) is substituted here so every provider owns only rungs 2-5, and a
// non-unique match becomes its typed tool error.
func resolveTarget(ctx context.Context, p Provider, ref string) (messaging.Resolution, error) {
	target := ref
	if ref == aliasSelf {
		target = p.Caller().RunID
	}
	res, err := p.Resolve(ctx, target)
	if err != nil {
		return messaging.Resolution{}, asToolError(err)
	}
	switch res.Match {
	case messaging.MatchNone:
		return messaging.Resolution{}, &ToolError{
			Code:    "unknown_target",
			Message: messaging.Repair("unknown_target", map[string]string{"target": quote(ref)}),
			Facts:   map[string]string{"target": quote(ref)},
		}
	case messaging.MatchAmbiguous:
		facts := map[string]string{
			"target":     quote(ref),
			"count":      fmt.Sprint(len(res.Candidates)),
			"candidates": quoteList(res.Candidates),
		}
		return messaging.Resolution{}, &ToolError{
			Code:    "ambiguous_name",
			Message: messaging.Repair("ambiguous_name", facts),
			Facts:   facts,
		}
	}
	return res, nil
}

func resolutionLabel(res messaging.Resolution, ref string) string {
	if res.Label != "" {
		return res.Label
	}
	return ref
}

// outcomeText keeps every reported outcome inside the contract. An outcome the
// matrix does not list for this action is a transport bug rather than a design
// case, and is reported as possibly-pending: the conservative reading, which
// still carries the message id, so a result's text is never empty.
func outcomeText(action string, outcome messaging.Outcome, facts map[string]string) (messaging.Outcome, string) {
	if !rendersOutcome(action, outcome) {
		outcome = messaging.OutcomePossiblyPending
	}
	return outcome, messaging.RenderOutcome(action, outcome, facts)
}

// rendersOutcome reports whether the outcome contract has a template for the
// (action, outcome) pair.
func rendersOutcome(action string, outcome messaging.Outcome) bool {
	return renderableOutcomes()[action+":"+string(outcome)]
}

// renderableOutcomes is the contract's (action, outcome) set, read once.
var renderableOutcomes = sync.OnceValue(func() map[string]bool {
	spec := messaging.LoadOutcomeSpec()
	renderable := map[string]bool{}
	for _, row := range append(append([]messaging.OutcomeRow(nil), spec.Messaging...), spec.Tool...) {
		for action := range row.Render {
			renderable[action+":"+row.Outcome] = true
		}
	}
	return renderable
})

func firstReason(status messaging.FieldStatus) string {
	if status.Reason != nil {
		return *status.Reason
	}
	if len(status.Reasons) > 0 {
		return status.Reasons[0]
	}
	return ""
}

func invalidAction(fields messaging.MessageRouteFields) *ToolError {
	availability := messaging.Check(fields, messaging.MessageRouteConditions{}, messaging.MessageRouteFields{})
	return &ToolError{Code: "invalid_action", Message: firstReason(availability.Action), Facts: map[string]string{}}
}

func invalidRequest(action, detail string) *ToolError {
	facts := map[string]string{"action": quote(action), "detail": detail}
	return &ToolError{Code: "invalid_request", Message: messaging.Repair("invalid_request", facts), Facts: facts}
}

func refused(err error) *ToolError {
	return &ToolError{Code: "refused", Message: err.Error(), Facts: map[string]string{}}
}

func asToolError(err error) *ToolError {
	var toolErr *ToolError
	if errors.As(err, &toolErr) {
		return toolErr
	}
	return refused(err)
}

func quote(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	return string(encoded)
}

func quoteList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, quote(value))
	}
	return strings.Join(quoted, ", ")
}

// The six message actions, the reserved self alias, and the two tool-level
// outcomes. The action spellings are the schema's action enum.
const (
	actionTell         = "tell"
	actionAsk          = "ask"
	actionReply        = "reply"
	actionResume       = "resume"
	actionWithdraw     = "withdraw"
	actionControlReply = "control_reply"

	aliasSelf = "self"

	outcomeResumed = messaging.Outcome("resumed")
	outcomeRenamed = messaging.Outcome("renamed")
)

// messageInput is the decoded avenor_message call. Admission has already been
// decided against the applicability table, so every field here is well typed.
type messageInput struct {
	Action         string          `json:"action"`
	Target         string          `json:"target"`
	Message        string          `json:"message"`
	ReplyTo        string          `json:"reply_to"`
	MessageID      string          `json:"message_id"`
	TimeoutMS      int             `json:"timeout_ms"`
	Label          string          `json:"label"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}

// decodeObject decodes one tool input as a JSON object keyed by property name,
// so validation sees which keys are present and each value's exact wire type.
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

// validateFields applies one action's applicability entry in the fixed order
// phase 1 uses: a property outside Permitted, then a missing Required property,
// then a value whose wire type is not the declared one. The returned sentence is
// the invalid_request template's {detail} fact.
func validateFields(fields map[string]json.RawMessage, entry messaging.ActionApplicability) string {
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
			return fmt.Sprintf("unknown property %q", name)
		}
	}
	for _, name := range entry.Required {
		if _, ok := fields[name]; !ok {
			return fmt.Sprintf("missing required property %q", name)
		}
	}
	for _, name := range entry.Permitted {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		kind := entry.Types[name]
		if hasWireType(raw, kind) {
			continue
		}
		switch kind {
		case "integer":
			return fmt.Sprintf("property %q must be an integer", name)
		case "object":
			return fmt.Sprintf("property %q must be an object", name)
		default:
			return fmt.Sprintf("property %q must be a string", name)
		}
	}
	return ""
}

// hasWireType reports whether raw carries the declared wire type. An integer
// means an integer, not merely a JSON number: a non-integral value such as 1.5
// is rejected by the table and by the JSON Schema alike.
func hasWireType(raw json.RawMessage, kind string) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	switch kind {
	case "string":
		_, ok := value.(string)
		return ok
	case "integer":
		number, ok := value.(json.Number)
		if !ok {
			return false
		}
		_, err := number.Int64()
		return err == nil
	case "object":
		_, ok := value.(map[string]any)
		return ok
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
