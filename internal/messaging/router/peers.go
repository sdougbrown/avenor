package router

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// peersActionVocabulary is the peers action union in declaration order. It is
// the peers twin of the message action enum: the schema's action enum, the
// peers applicability table's keys and this list must hold the same three names
// in the same order (TestPeersActionVocabulary), and no message action ever
// appears in any of them.
var peersActionVocabulary = []string{"list", "rename", "last"}

// peersDefaultAction is what an omitted action means. The schema publishes it
// as the list variant's default, and the executor applies it before admission,
// so {} lists peers instead of failing; a present action that is not a string
// stays an invalid action.
const peersDefaultAction = "list"

// peersInput is the decoded avenor_peers call. Admission has already been
// decided against the peers applicability table.
type peersInput struct {
	Action    string `json:"action"`
	Name      string `json:"name"`
	SessionID string `json:"session_id"`
	Target    string `json:"target"`
}

// ExecutePeers routes one avenor_peers call. An omitted action is list; a
// present but non-string or unknown action is one invalid action, the code and
// text the message router uses for an invalid action. `list` and `last` make no
// transport write and publish no event; `rename`'s single write belongs to
// SessionRename.
func ExecutePeers(ctx context.Context, p Provider, input json.RawMessage) (Result, error) {
	fields, err := decodeObject(input)
	if err != nil {
		return Result{}, peersInvalidAction()
	}
	action, ok := fieldString(fields, "action")
	if !ok {
		if _, present := fields["action"]; present {
			return Result{}, peersInvalidAction()
		}
		action = peersDefaultAction
		fields["action"] = json.RawMessage(`"` + peersDefaultAction + `"`)
	}
	table := messaging.PeersApplicabilityTable()
	entry, known := table[action]
	if !known {
		return Result{}, peersInvalidAction()
	}
	if detail := validateFields(fields, entry); detail != "" {
		return Result{}, invalidRequest(action, detail)
	}
	var typed peersInput
	if err := json.Unmarshal(input, &typed); err != nil {
		return Result{}, invalidRequest(action, "input is not a valid request object")
	}

	switch action {
	case "list":
		return peersList(ctx, p, action)
	case "rename":
		return peersRename(ctx, p, action, typed)
	case "last":
		return peersLast(ctx, p, action, typed)
	}
	return Result{}, peersInvalidAction()
}

func peersInvalidAction() *ToolError {
	return &ToolError{
		Code:    "invalid_action",
		Message: messaging.Repair("peers_invalid_action", nil),
		Facts:   map[string]string{},
	}
}

// peersList reads the broker's peer listing. There is no name key: Label is the
// display name, so the list is the rename's read path. A transport may return
// peers together with an error, which is a listing that read only part of the
// peer set: those peers are kept and the result reports the incomplete listing
// through the contract's text, so a partial read never renders as a clean list.
func peersList(ctx context.Context, p Provider, action string) (Result, error) {
	peers, err := p.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: p.Caller().RunID},
		Generation: p.Generation(),
	})
	if err != nil && len(peers.Peers) == 0 {
		return Result{}, refused(err)
	}
	records, listingErr := messaging.PeerListing(peers.Peers, err)
	if listingErr != nil && len(records) == 0 {
		return Result{}, refused(listingErr)
	}
	result := Result{Action: action, Peers: records}
	if listingErr != nil {
		// Classify through refused once so the detail carries the same
		// filesystem-path rule the empty-listing failure uses instead of naming
		// an absolute path in the text.
		result.Outcome = messaging.OutcomeLost
		result.Text = messaging.RenderOutcome(peersListOutcomeKey, result.Outcome, map[string]string{
			"detail": sanitize(refused(listingErr).Message),
		})
	}
	return result, nil
}

// peersRename forwards the rename to the provider and passes its typed error
// through unchanged. Phase-1 admission owns the name's length and pattern,
// counted in runes like the published schema; the rename authority owns every
// other name rule plus resolution and permission.
//
// The reported labels are RenameResult's stored write-tracking view, not the
// resolved display name `list` renders: a rename under a harness-supplied
// session name writes SessionInfo.Label while peers keep seeing the harness
// name until it is cleared. The rendered sentence reports the stored update as
// such; it does not claim the peer-visible name changed.
func peersRename(ctx context.Context, p Provider, action string, input peersInput) (Result, error) {
	renamed, err := p.Rename(ctx, messaging.RenameRequest{SessionID: input.SessionID, Name: input.Name})
	if err != nil {
		return Result{}, asToolError(err)
	}
	result := Result{
		Action:        action,
		Outcome:       outcomeRenamed,
		RunID:         renamed.RunID,
		Label:         renamed.Label,
		PreviousLabel: renamed.PreviousLabel,
	}
	result.Text = messaging.RenderOutcome("peers_rename", result.Outcome, map[string]string{
		"run_id":         quote(result.RunID),
		"label":          quote(result.Label),
		"previous_label": quote(result.PreviousLabel),
	})
	return result, nil
}

// peersLast resolves the target and reads its recorded last agent message.
// Reading a finished, live or never-started run's last message is the normal
// case, so there is no permission row and no state row.
func peersLast(ctx context.Context, p Provider, action string, input peersInput) (Result, error) {
	res, err := resolveTarget(ctx, p, input.Target)
	if err != nil {
		return Result{}, err
	}
	last, err := p.Last(ctx, res.RunID)
	if err != nil {
		return Result{}, asToolError(err)
	}
	label := firstNonEmpty(last.Label, res.Label, input.Target)
	result := Result{
		Action:      action,
		RunID:       firstNonEmpty(res.PublicRunID, res.RunID),
		RuntimeID:   res.RunID,
		Label:       label,
		FinalOutput: last.Text,
		StopReason:  last.StopReason,
		Truncated:   last.Truncated,
	}
	facts := LastFacts(result.RunID, label, last.StopReason)
	switch {
	case !last.HasRecord:
		result.Text = messaging.LastRender("empty", facts)
	case last.Text == "":
		result.Text = messaging.LastRender("no_text", facts)
	default:
		text := messaging.LastRender("header", facts) + "\n\n" + last.Text
		if last.Truncated {
			text += messaging.LastRender("truncated_suffix", facts)
		}
		result.Text = text
	}
	return result, nil
}

// LastFacts builds the template facts for a peers `last` result: the run id and
// label are quoted, while the stop reason is sanitized because it is a bare wire
// fact. The rule lives here so a surface test can render the expected text
// without restating it, and the templates cannot be pinned by a test that
// reproduces a wrong rule.
func LastFacts(runID, label, stopReason string) map[string]string {
	return map[string]string{
		"run_id":      quote(runID),
		"label":       quote(label),
		"stop_reason": sanitize(stopReason),
	}
}

// RenderPeers renders the peers list block a surface shows. It is the one list
// text for the records a surface routes through this package, so the surfaces
// using it cannot disagree on how a peer's display name, kind and status are
// rendered. A surface still rendering its own list will disagree on kind, and
// on the attribute block for a record with no status.
func RenderPeers(records []messaging.PeerRecord) string {
	if len(records) == 0 {
		return "No other agent sessions connected."
	}
	lines := make([]string, 0, len(records))
	for _, record := range records {
		line := "  " + record.RunID
		if record.Label != "" {
			line = "  " + record.Label + " (" + record.RunID + ")"
		}
		var attrs []string
		for _, value := range []string{record.Kind, record.Status, record.Backend, record.Model} {
			if value != "" {
				attrs = append(attrs, value)
			}
		}
		if len(attrs) > 0 {
			line += " [" + strings.Join(attrs, ", ") + "]"
		}
		lines = append(lines, line)
	}
	return "**Active sessions:**\n" + strings.Join(lines, "\n")
}

// PeersDetails is the structured peers result every Go surface returns: the
// list records, the rename triple, or the last-message object. A peers action
// is never merged into the message result object.
func PeersDetails(result Result) map[string]any {
	switch result.Action {
	case "list":
		return map[string]any{"peers": result.Peers}
	case "rename":
		return map[string]any{"renamed": map[string]any{
			"run_id":         result.RunID,
			"label":          result.Label,
			"previous_label": result.PreviousLabel,
		}}
	default:
		return map[string]any{"last": map[string]any{
			"run_id":                 result.RunID,
			"runtime_id":             result.RuntimeID,
			"label":                  result.Label,
			"final_output":           result.FinalOutput,
			"stop_reason":            result.StopReason,
			"final_output_truncated": result.Truncated,
		}}
	}
}

// PeersContent is the human text a peers call returns. `last` and `rename` are
// exactly the router's rendered text; `list` is the shared list block, keeping
// the router's incomplete-listing sentence above it.
func PeersContent(result Result) string {
	if result.Action != "list" {
		return result.Text
	}
	if result.Text != "" {
		return result.Text + "\n\n" + RenderPeers(result.Peers)
	}
	return RenderPeers(result.Peers)
}
