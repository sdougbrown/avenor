// Package router implements routing for the merged messaging tools: input
// validation, target resolution, the permission and state rules, the provider
// call, and the outcome text.
//
// Router.Execute routes avenor_message and ExecutePeers routes avenor_peers, so
// the branch logic exists once. The input contract, the repair templates and
// the render templates come from schemas/ (through internal/messaging); the two
// permission reasons and the invalid-action reason come from the generated
// route table. Failure sentences are composed here from those templates.
package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"regexp"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

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
	// Generation reports the generation the provider is currently bound to; the
	// router reads it on every request and never writes it, so it never
	// retargets a provider mid-call. Staleness is provider-owned policy: the
	// child provider pins the generation it read at construction, and the Go
	// host provider re-preflights after the store reports a mismatch, since the
	// control store mints a new generation on every restart.
	Generation() string
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
	Label     string // the resolved target's display name (harness name, else SessionInfo.Label)
	Reply     string // the reply body, ask/replied only; empty everywhere else
	Text      string
	// Peers is the peers `list` action's output: one record per visible peer,
	// already decoded from the broker's peer listing. It is empty for every
	// other action. `list` sets no text for a complete listing; an incomplete
	// one keeps the peers it read and carries the incomplete-listing text.
	Peers []messaging.PeerRecord
	// PreviousLabel is the label before a peers `rename`, which the
	// peers_rename:renamed template and the surface's result object report.
	PreviousLabel string
	// resume only: the paths a ResumeResult minted for the new run.
	SentinelFile string
	OnEvent      string
	// peers `last` only; zero values everywhere else.
	FinalOutput string
	StopReason  string
	Truncated   bool
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

// ErrNoGeneration reports a provider whose construction-time generation binding
// is empty. A provider that cannot name the generation it is bound to cannot
// build any request, so it fails at construction instead of falling back to a
// per-message preflight.
var ErrNoGeneration = errors.New("router: provider has no bound generation")

// NewRouter builds a router over p. The ask client is bound to the same
// provider and to one options value for the router's lifetime. A provider with
// no bound generation is rejected here, so every later request carries a
// non-empty generation and the router never preflights per message.
func NewRouter(p Provider) (*Router, error) {
	if p.Generation() == "" {
		return nil, ErrNoGeneration
	}
	return &Router{provider: p, client: askclient.New(p, askclient.Options{})}, nil
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

	conditions := messaging.MessageRouteConditions{
		CallerKind:            string(r.provider.Caller().Kind),
		Surface:               string(r.provider.Surface()),
		ControlReplySupported: r.provider.Surface() == messaging.SurfaceSidecar,
	}
	if action == actionControlReply {
		if denial := permissionDenial(action, conditions); denial != nil {
			return Result{}, denial
		}
		return r.controlReply(ctx, typed)
	}

	res, err := r.resolve(ctx, typed.Target)
	if err != nil {
		return Result{}, err
	}
	conditions.TargetMatch = string(res.Match)
	if denial := permissionDenial(action, conditions); denial != nil {
		return Result{}, denial
	}
	facts, err := r.provider.RunState(ctx, res.RunID)
	if err != nil {
		return Result{}, refused(err)
	}
	conditions.TargetLive = facts.Live
	conditions.TargetResumable = facts.Resumable
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
	switch action {
	case actionControlReply:
		if !conditions.ControlReplySupported {
			return permissionError(action, conditions, "unsupported_action")
		}
	case actionResume:
		// The generated fairWhen slot is the allowlist: a resume is fair only
		// when the caller is the owner, so an empty or future caller kind is
		// denied here rather than reaching the provider unchecked.
		availability := messaging.Check(messaging.MessageRouteFields{Action: &action}, conditions, messaging.MessageRouteFields{})
		if !availability.Target.Fair {
			return &ToolError{Code: "not_permitted", Message: firstReason(availability.Target), Facts: map[string]string{}}
		}
	}
	return nil
}

func permissionError(action string, conditions messaging.MessageRouteConditions, code string) *ToolError {
	availability := messaging.Check(messaging.MessageRouteFields{Action: &action}, conditions, messaging.MessageRouteFields{})
	return &ToolError{Code: code, Message: firstReason(availability.Target), Facts: map[string]string{}}
}

// stateDenial reports the phase-4 state rows. The tell/ask live rule and the
// resume resumable rule are read from the generated route table, so editing the
// Umpire spec changes what the router enforces. Their text is template-rendered
// because the generated reasons are static and never user-visible.
func stateDenial(action string, conditions messaging.MessageRouteConditions, runID, status string) *ToolError {
	if action != actionTell && action != actionAsk && action != actionResume {
		return nil
	}
	availability := messaging.Check(messaging.MessageRouteFields{Action: &action}, conditions, messaging.MessageRouteFields{})
	facts := map[string]string{"run_id": quote(runID), "status": sanitize(status)}
	switch action {
	case actionTell, actionAsk:
		if availability.Message.Enabled {
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
		if !availability.Message.Fair {
			return &ToolError{Code: "not_resumable", Message: messaging.Repair("not_resumable", facts), Facts: facts}
		}
	}
	return nil
}

func (r *Router) tell(ctx context.Context, input messageInput, res messaging.Resolution) (Result, error) {
	label := resolutionLabel(res, input.Target)
	result := Result{Action: actionTell, Outcome: messaging.OutcomeNotSent, Label: label}
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
		Generation: r.provider.Generation(),
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
	send.Generation = r.provider.Generation()
	wait.Generation = send.Generation
	wait.Timeout = time.Duration(input.timeoutMS()) * time.Millisecond

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
	// The claim is restored unless the outcome proves delivery. An outcome that
	// is not replied — including a nil-error not-sent, edge-absent or
	// possibly-pending — never proves the asker received the answer, and a
	// possibly-pending reply must leave the ask answerable rather than consume
	// the claim.
	if sent.Outcome != messaging.OutcomeReplied {
		claimer.RestoreAsk(input.ReplyTo, asker)
	}
	result.Outcome, result.Text = outcomeText(actionReply, sent.Outcome, facts)
	return result, nil
}

func (r *Router) withdraw(ctx context.Context, input messageInput) (Result, error) {
	result := Result{Action: actionWithdraw, MessageID: input.MessageID}
	facts := map[string]string{"message_id": quote(input.MessageID)}
	withdrawn, err := r.provider.Withdraw(ctx, messaging.WithdrawRequest{
		Sender:     messaging.Identity{RunID: r.provider.Caller().RunID},
		MessageID:  input.MessageID,
		Generation: r.provider.Generation(),
	})
	if err != nil && withdrawn.Outcome == "" {
		return Result{}, refused(err)
	}
	if errors.Is(err, messaging.ErrFenceCapacity) && withdrawn.Outcome == messaging.OutcomeEdgeAbsent {
		// The absence fence could not be installed, so a delayed send can still
		// be admitted: reporting "nothing is pending" would be a lie.
		result.Outcome, result.Text = outcomeText(actionWithdraw, messaging.OutcomePossiblyPending, facts)
		return result, nil
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
		Action:       actionResume,
		Outcome:      outcomeResumed,
		RunID:        resumed.RunID,
		Label:        resumed.Label,
		SentinelFile: resumed.SentinelFile,
		OnEvent:      resumed.OnEvent,
	}
	result.Text = messaging.RenderOutcome(actionResume, result.Outcome, map[string]string{
		"run_id": quote(result.RunID),
		"label":  quote(result.Label),
	})
	return result, nil
}

func (r *Router) controlReply(ctx context.Context, input messageInput) (Result, error) {
	// Execute has already applied the generated control_reply support rule at
	// the permission phase, so the only remaining case is a sidecar provider
	// that still cannot deliver the reply.
	replier, ok := r.provider.(ControlReplier)
	if !ok {
		// The support rule is satisfied on the sidecar, so it names no reason
		// for a provider that still cannot deliver a control reply. This row
		// has its own shared template.
		return Result{}, &ToolError{
			Code:    "unsupported_action",
			Message: messaging.Repair("control_reply_unsupported", nil),
			Facts:   map[string]string{},
		}
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

// IsReservedTarget reports whether ref is a reserved alias or the supervisor's
// own run id, so a caller binding a supervisor must not treat it as a run key.
func IsReservedTarget(ref string) bool {
	return ref == "" || ref == aliasSelf || ref == messaging.SupervisorRunID
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

// outcomeText renders the contract's (action, outcome) text. An outcome outside
// the vocabulary entirely is a transport bug with no meaningful text, so it is
// reported as possibly-pending: the conservative reading, which still carries
// the message id. An in-vocabulary outcome the matrix does not list is a
// contract gap, not a transport bug: it is returned unrendered so the gap is
// visible (and a conformance test fails) instead of being silently rewritten.
func outcomeText(action string, outcome messaging.Outcome, facts map[string]string) (messaging.Outcome, string) {
	if rendersOutcome(action, outcome) {
		return outcome, messaging.RenderOutcome(action, outcome, facts)
	}
	if isVocabularyOutcome(outcome) {
		return outcome, ""
	}
	return messaging.OutcomePossiblyPending, messaging.RenderOutcome(action, messaging.OutcomePossiblyPending, facts)
}

// isVocabularyOutcome reports whether outcome is one the contract declares for
// messaging. Anything else came from outside the transport contract.
func isVocabularyOutcome(outcome messaging.Outcome) bool {
	return outcomeSets().vocabulary[string(outcome)]
}

// rendersOutcome reports whether the outcome contract has a template for the
// (action, outcome) pair.
func rendersOutcome(action string, outcome messaging.Outcome) bool {
	return outcomeSets().renderable[action+":"+string(outcome)]
}

// outcomeSet is the outcome contract in lookup form.
type outcomeSet struct {
	renderable map[string]bool // action:outcome pair that has a template
	vocabulary map[string]bool // outcome string the contract declares
}

// outcomeSets reads the contract once. The vocabulary is the messaging rows and
// the render set adds the tool rows, so an outcome, action or template added to
// the contract is recognized without editing the router.
var outcomeSets = sync.OnceValue(func() outcomeSet {
	spec := messaging.LoadOutcomeSpec()
	sets := outcomeSet{renderable: map[string]bool{}, vocabulary: map[string]bool{}}
	for _, row := range spec.Messaging {
		sets.vocabulary[row.Outcome] = true
	}
	for _, row := range append(append([]messaging.OutcomeRow(nil), spec.Messaging...), spec.Tool...) {
		for action := range row.Render {
			sets.renderable[action+":"+row.Outcome] = true
		}
	}
	return sets
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
	// A filesystem error names an absolute path that must not reach a model or a
	// peer, so it is classified to a shared sentence instead of passed through.
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
		return &ToolError{Code: "refused", Message: messaging.Repair("refused_io", nil), Facts: map[string]string{}}
	}
	return &ToolError{Code: "refused", Message: err.Error(), Facts: map[string]string{}}
}

func asToolError(err error) *ToolError {
	var toolErr *ToolError
	if errors.As(err, &toolErr) {
		return toolErr
	}
	return refused(err)
}

// quote renders value for a template fact through the shared escaping rule, so
// the router and the rename authority cannot disagree on how an id is quoted.
func quote(value string) string { return messaging.Quote(value) }

// sanitize renders a bare template fact through the shared escaping rule.
func sanitize(value string) string { return messaging.Sanitize(value) }

func quoteList(values []string) string { return messaging.QuoteList(values) }

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

	// peersListOutcomeKey is the outcome matrix's key for the peers list
	// action, whose one non-clean outcome is an incomplete listing.
	peersListOutcomeKey = "peers_list"

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
	TimeoutMS      json.Number     `json:"timeout_ms"`
	Label          string          `json:"label"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}

// SupervisorKeys returns the target and session_id of one message or peers
// call, in probe order, decoded through the same input types the routers
// decode so a schema field rename has one definition. Empty keys are dropped.
func SupervisorKeys(input json.RawMessage) []string {
	var message messageInput
	_ = json.Unmarshal(input, &message)
	var peers peersInput
	_ = json.Unmarshal(input, &peers)
	keys := make([]string, 0, 2)
	for _, key := range []string{message.Target, peers.SessionID} {
		if key != "" {
			keys = append(keys, key)
		}
	}
	return keys
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
// then a value whose wire type or declared constraint is not met. The returned
// sentence is the invalid_request template's {detail} fact, rendered from the
// shared repairs table so no failure sentence is composed here.
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
			return messaging.Repair("unknown_property", map[string]string{"name": quote(name)})
		}
	}
	for _, name := range entry.Required {
		if _, ok := fields[name]; !ok {
			return messaging.Repair("missing_required_property", map[string]string{"name": quote(name)})
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
				return messaging.Repair("wrong_type_integer", map[string]string{"name": quote(name)})
			case "object":
				return messaging.Repair("wrong_type_object", map[string]string{"name": quote(name)})
			default:
				return messaging.Repair("wrong_type_string", map[string]string{"name": quote(name)})
			}
		}
		constraint := entry.Constraints[name]
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

// validateStringConstraint applies the string limits the published JSON Schema
// declares. It reads the compiled constraints, so the schema and the router
// cannot disagree about length or allowed characters.
func validateStringConstraint(name, value string, constraint messaging.PropertyConstraints) string {
	length := utf8.RuneCountInString(value)
	if constraint.MinLength > 0 && length < constraint.MinLength {
		return messaging.Repair("string_too_short", map[string]string{
			"name": quote(name),
			"min":  fmt.Sprint(constraint.MinLength),
		})
	}
	if constraint.MaxLength > 0 && length > constraint.MaxLength {
		return messaging.Repair("string_too_long", map[string]string{
			"name": quote(name),
			"max":  fmt.Sprint(constraint.MaxLength),
		})
	}
	if constraint.Pattern != "" {
		if pattern, ok := constraintPatterns()[constraint.Pattern]; !ok || !pattern.MatchString(value) {
			return messaging.Repair("string_pattern", map[string]string{"name": quote(name)})
		}
	}
	return ""
}

// constraintPatterns is every pattern the two applicability tables declare,
// compiled once. The tables are the only source of a constraint's Pattern, so a
// missing entry means the pattern did not compile and no value can match it.
var constraintPatterns = sync.OnceValue(func() map[string]*regexp.Regexp {
	compiled := map[string]*regexp.Regexp{}
	for _, table := range []map[string]messaging.ActionApplicability{
		messaging.ApplicabilityTable(),
		messaging.PeersApplicabilityTable(),
	} {
		for _, entry := range table {
			for _, constraint := range entry.Constraints {
				if constraint.Pattern == "" {
					continue
				}
				pattern, err := regexp.Compile(constraint.Pattern)
				if err != nil {
					continue
				}
				compiled[constraint.Pattern] = pattern
			}
		}
	}
	return compiled
})

// validateIntegerConstraint applies the integer limits the published JSON
// Schema declares. timeout_ms has an inclusive floor so a negative wait cannot
// fall through to the ask store's "absent or zero" maximum, and an inclusive
// ceiling that keeps the later duration conversion inside its range. Both
// bounds are read from their declared-presence flags, so a bound of zero is a
// limit and not an omission.
func validateIntegerConstraint(name string, raw json.RawMessage, constraint messaging.PropertyConstraints) string {
	if !constraint.HasMinimum && !constraint.HasMaximum {
		return ""
	}
	value, ok := exactInteger(raw)
	if !ok {
		return ""
	}
	if constraint.HasMinimum {
		minimum := new(big.Rat).SetInt64(constraint.Minimum)
		if value.Cmp(minimum) < 0 {
			return messaging.Repair("integer_too_small", map[string]string{
				"name": quote(name),
				"min":  fmt.Sprint(constraint.Minimum),
			})
		}
	}
	if constraint.HasMaximum {
		maximum := new(big.Rat).SetInt64(constraint.Maximum)
		if value.Cmp(maximum) > 0 {
			return messaging.Repair("integer_too_large", map[string]string{
				"name": quote(name),
				"max":  fmt.Sprint(constraint.Maximum),
			})
		}
	}
	return ""
}

// timeoutMS converts the validated integer-valued timeout_ms into whole
// milliseconds. Validation admits 1.0 and 1e3, which do not unmarshal into an
// int directly, so the conversion goes through the exact rational value. The
// schema's maximum keeps the result inside time.Duration's range.
func (input messageInput) timeoutMS() int {
	if input.TimeoutMS == "" {
		return 0
	}
	rational, ok := new(big.Rat).SetString(input.TimeoutMS.String())
	if !ok || !rational.IsInt() {
		return 0
	}
	numerator := rational.Num()
	if !numerator.IsInt64() {
		return 0
	}
	return int(numerator.Int64())
}

// hasWireType reports whether raw carries the declared wire type. An integer
// means a mathematical integer, matching the JSON Schema: 1.0 and 1e3 are
// accepted and a non-integral value such as 1.5 is rejected by both layers. The
// value must still fit the router's int field.
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
		rational, ok := exactInteger(raw)
		return ok && rational.Num().IsInt64()
	case "object":
		_, ok := value.(map[string]any)
		return ok
	}
	return false
}

// exactInteger returns raw's exact rational value when it is a mathematical
// integer.
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// Output is the tool result object every Go surface returns for one routed
// call. The field set is shared: code is present only on a tool error (see
// ToolError.Output), message_id for tell/ask/reply/withdraw, run_id+label for
// resume and peers rename, text always, and reply for an answered ask.
func (r Result) Output() map[string]any {
	out := map[string]any{
		"action":  r.Action,
		"outcome": string(r.Outcome),
		"text":    r.Text,
	}
	if r.MessageID != "" {
		out["message_id"] = r.MessageID
	}
	if r.RunID != "" {
		out["run_id"] = r.RunID
	}
	if r.Label != "" {
		out["label"] = r.Label
	}
	if r.Reply != "" {
		out["reply"] = r.Reply
	}
	// The resume paths travel with the result that minted them, so the caller
	// can assert the sentinel and event log the resumed run actually writes.
	if r.SentinelFile != "" {
		out["sentinel_file"] = r.SentinelFile
	}
	if r.OnEvent != "" {
		out["on_event"] = r.OnEvent
	}
	return out
}

// Output is the shared tool result object for a refused or rejected call: only
// the typed code and the rendered text, which carries the same sentence the
// model would see as content.
func (e *ToolError) Output() map[string]any {
	return map[string]any{"code": e.Code, "text": e.Message}
}
