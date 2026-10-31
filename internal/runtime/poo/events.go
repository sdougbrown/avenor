package poo

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/poo/wire/protocol"
)

// sessionEvent is one event notification from the backend: the notification's
// flat params object, with the envelope fields taken out.
type sessionEvent struct {
	SessionID string
	Seq       int64
	Type      string
	Fields    map[string]any
}

// translateEvent maps one poo event onto the avenor events it publishes. A nil
// result means the event is deliberately not delivered.
func translateEvent(ev sessionEvent) []events.Event {
	switch ev.Type {
	case "run.started":
		return []events.Event{passthrough("avenor.turn.start", ev)}
	case "run.settling":
		// The outcome is already fixed, so an input now is refused.
		return []events.Event{passthrough("avenor.run.settling", ev)}
	case "run.settled":
		return []events.Event{settledEvent(ev)}
	case "message.delta":
		return translateDelta(ev)
	case "tool.call":
		return []events.Event{toolCallEvent(ev)}
	case "tool.result":
		return []events.Event{toolResultEvent(ev)}
	case "usage":
		return []events.Event{usageEvent(ev)}
	case "observer.dropped":
		// A record-only diagnostic: the backend already coalesced it into
		// the session's log, and it is never delivered to observers.
		return nil
	default:
		// A record poo names for itself — the run's progress notes, the jobs,
		// the retries, the permission echo — passes through under the same
		// prefix every backend applies to the events it does not standardize.
		return []events.Event{passthrough("avenor."+ev.Type, ev)}
	}
}

func passthrough(name string, ev sessionEvent) events.Event {
	return events.Event{Event: name, SessionID: ev.SessionID, Fields: cloneFields(ev.Fields)}
}

func cloneFields(fields map[string]any) map[string]any {
	out := make(map[string]any, len(fields)+2)
	for key, value := range fields {
		out[key] = value
	}
	return out
}

func translateDelta(ev sessionEvent) []events.Event {
	text, _ := ev.Fields["text"].(string)
	if text == "" {
		// A delta with no text carries nothing a consumer can read, so it is
		// not published at all rather than as an event with an empty body.
		return nil
	}
	name := "agent.message_chunk"
	if kind, _ := ev.Fields["kind"].(string); kind == "thinking" {
		name = "agent.thought_chunk"
	}
	fields := map[string]any{
		"delta":   text,
		"content": map[string]any{"type": "text", "text": text},
	}
	for _, key := range []string{"run_id", "message_id", "attempt"} {
		if value, ok := ev.Fields[key]; ok {
			fields[key] = value
		}
	}
	return []events.Event{{Event: name, SessionID: ev.SessionID, Fields: fields}}
}

func toolCallEvent(ev sessionEvent) events.Event {
	fields := cloneFields(ev.Fields)
	fields["kind"] = "tool"
	if tool, _ := fields["tool"].(string); tool != "" {
		fields["title"] = tool
	}
	fields["status"] = "running"
	return events.Event{Event: "tool.call", SessionID: ev.SessionID, Fields: fields}
}

func toolResultEvent(ev sessionEvent) events.Event {
	fields := cloneFields(ev.Fields)
	fields["kind"] = "tool"
	if tool, _ := fields["tool"].(string); tool != "" {
		fields["title"] = tool
	}
	outcome, _ := fields["outcome"].(string)
	fields["status"] = toolResultStatus(outcome)
	return events.Event{Event: "tool.call_update", SessionID: ev.SessionID, Fields: fields}
}

// toolResultStatus maps poo's tool outcome onto avenor's two-state status. The
// outcome itself stays in the event so a consumer keeps the distinction.
func toolResultStatus(outcome string) string {
	switch outcome {
	case "", "ok", "skipped", "started":
		return "completed"
	default:
		return "failed"
	}
}

func usageEvent(ev sessionEvent) events.Event {
	input, _ := events.Int64(ev.Fields["input_tokens"])
	output, _ := events.Int64(ev.Fields["output_tokens"])
	cached, _ := events.Int64(ev.Fields["cache_read_tokens"])
	usage := map[string]any{
		"input_tokens":  input,
		"output_tokens": output,
		"total_tokens":  input + output,
	}
	if cached > 0 {
		usage["cached_read_tokens"] = cached
	}
	fields := map[string]any{"usage": usage}
	for _, key := range []string{"run_id", "turn", "request"} {
		if value, ok := ev.Fields[key]; ok {
			fields[key] = value
		}
	}
	return events.Event{Event: "usage", SessionID: ev.SessionID, Fields: fields}
}

// settledEvent publishes the turn's terminal record with avenor's closed stop
// reason and exit code.
func settledEvent(ev sessionEvent) events.Event {
	reason, _ := ev.Fields["reason"].(string)
	detail := settleDetail(ev.Fields["detail"])
	outcome := classifySettle(reason, detail)

	fields := map[string]any{"stop_reason": outcome.StopReason}
	if outcome.ErrorMessage != "" {
		fields["error_message"] = outcome.ErrorMessage
	}
	if outcome.ClassifiedFrom != "" {
		fields["classified_from"] = outcome.ClassifiedFrom
	}
	if outcome.RetryAfter > 0 {
		fields["retry_after"] = outcome.RetryAfter
	}
	if detail.Handoff != "" {
		fields["handoff"] = detail.Handoff
	}
	if len(detail.JobsRunning) > 0 {
		fields["jobs_running"] = detail.JobsRunning
	}
	if len(detail.UndeliveredInput) > 0 {
		fields["undelivered_input"] = detail.UndeliveredInput
	}
	if len(detail.Result) > 0 {
		fields["result"] = detail.Result
	}
	runtime.SetSessionEndExitCode(fields, outcome.StopReason)
	return events.Event{Event: "session.end", SessionID: ev.SessionID, Fields: fields}
}

// settleDetail reads the detail object a settle event carries out of the
// loosely decoded payload. A field the shape cannot hold is reported by its
// absence rather than failing the terminal event.
func settleDetail(raw any) protocol.SettleDetail {
	var detail protocol.SettleDetail
	switch value := raw.(type) {
	case map[string]any:
		detail.Text, _ = value["text"].(string)
		detail.Source, _ = value["source"].(string)
		detail.Handoff, _ = value["handoff"].(string)
		if jobs, ok := value["jobs_running"].([]any); ok {
			for _, job := range jobs {
				if name, _ := job.(string); name != "" {
					detail.JobsRunning = append(detail.JobsRunning, name)
				}
			}
		}
		if provider, ok := value["provider"].(map[string]any); ok {
			detail.Provider = &protocol.ProviderDetail{}
			detail.Provider.Kind, _ = provider["kind"].(string)
			if retry, ok := events.Int64(provider["retry_after_s"]); ok {
				detail.Provider.RetryAfterS = int(retry)
			}
		}
		if result, ok := value["result"]; ok {
			if encoded, err := json.Marshal(result); err == nil {
				detail.Result = encoded
			}
		}
		if undelivered, ok := value["undelivered_input"].([]any); ok {
			for _, item := range undelivered {
				entry, _ := item.(map[string]any)
				if entry == nil {
					continue
				}
				var input protocol.UndeliveredInput
				input.InputID, _ = entry["input_id"].(string)
				input.Mode, _ = entry["mode"].(string)
				detail.UndeliveredInput = append(detail.UndeliveredInput, input)
			}
		}
	}
	return detail
}

// settleOutcome is avenor's classification of a poo settle reason.
type settleOutcome struct {
	// StopReason is avenor's closed stop reason.
	StopReason string
	// ErrorMessage is poo's own explanation, for a failed run.
	ErrorMessage string
	// ClassifiedFrom is poo's reason when avenor reports another one.
	ClassifiedFrom string
	// RetryAfter is the provider's hint, in whole seconds.
	RetryAfter int64
}

// genericFailure is the stop reason avenor uses for a failed run that has no
// dedicated reason. Its exit code is 1.
const genericFailure = "error"

// classifySettle maps poo's settle reason onto avenor's closed set. The exit
// code follows from the avenor reason, never from poo's own table.
func classifySettle(reason string, detail protocol.SettleDetail) settleOutcome {
	out := settleOutcome{ClassifiedFrom: reason}
	switch reason {
	case "end_turn", "finished", "stop_requested":
		out.StopReason = "end_turn"
		out.ClassifiedFrom = ""
	case "output_truncated":
		out.StopReason = "max_tokens"
	case "max_turns":
		out.StopReason = "max_turn_requests"
	case "cancelled":
		out.StopReason = "cancelled"
	case "provider_limit":
		kind := ""
		if detail.Provider != nil {
			kind = detail.Provider.Kind
			out.RetryAfter = int64(detail.Provider.RetryAfterS)
		}
		switch kind {
		case "rate_limited":
			out.StopReason = runtime.RateLimitedStopReason
		case "usage_limit", "plan_inactive":
			out.StopReason = runtime.QuotaExhaustedStopReason
		default:
			out.StopReason = genericFailure
			out.ErrorMessage = providerLimitText(kind, detail.Text)
		}
	default:
		// provider_error, hook_error, hook_continue_limit, tool_fatal,
		// store_error, budget_exhausted, context_exhausted, interrupted,
		// invalid_turns and any reason a later binary adds: a generic
		// failure that names the backend's own reason in error_message.
		out.StopReason = genericFailure
		out.ErrorMessage = detail.Text
		if out.ErrorMessage == "" {
			out.ErrorMessage = fmt.Sprintf("run settled for %q", reason)
		}
	}
	if out.StopReason == reason {
		out.ClassifiedFrom = ""
	}
	return out
}

func providerLimitText(kind, text string) string {
	switch {
	case kind != "" && text != "":
		return fmt.Sprintf("provider limit (%s): %s", kind, text)
	case kind != "":
		return fmt.Sprintf("provider limit (%s)", kind)
	case text != "":
		return "provider limit: " + text
	default:
		return "provider limit"
	}
}

// classifyFromEvent is classifySettle for a run/inspect result.
func classifyFromEvent(settled *protocol.Settled) settleOutcome {
	if settled == nil {
		return settleOutcome{StopReason: genericFailure, ErrorMessage: "the run reported no outcome"}
	}
	return classifySettle(settled.Reason, settled.Detail)
}

// settleError is Prompt's result for a settled run. The terminal event carries
// the stop reason, so the error only reports the settles that are failures.
func settleError(sessionID string, outcome settleOutcome) error {
	switch outcome.StopReason {
	case "end_turn", "max_tokens", "max_turn_requests", runtime.RateLimitedStopReason, runtime.QuotaExhaustedStopReason:
		// A turn that ran out of room or hit a provider limit is not a prompt
		// failure: the run loop reads the reason off the terminal event and
		// decides there whether to continue, retry or stop.
		return nil
	case "cancelled":
		return fmt.Errorf("turn cancelled for session %s", sessionID)
	}
	message := outcome.ErrorMessage
	if message == "" {
		message = outcome.StopReason
	}
	if outcome.ClassifiedFrom != "" && outcome.StopReason == genericFailure {
		message = strings.TrimSpace(outcome.ClassifiedFrom + ": " + message)
	}
	return fmt.Errorf("turn failed for session %s: %s", sessionID, message)
}
