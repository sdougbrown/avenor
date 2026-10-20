package router

import (
	"context"
	"encoding/json"

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

// ExecutePeers routes one avenor_peers call. It is the one peers executor the
// three Go surfaces share, so `last` cannot behave differently on the host and
// on the children. An omitted action is list; a present but non-string or
// unknown action is one invalid action, the code and text the message router
// uses for an invalid action. `list` and `last` make no transport write and
// publish no event; `rename`'s single write belongs to SessionRename.
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
// display name, so the list is the rename's read path. The list carries no
// text, because it stays small.
func peersList(ctx context.Context, p Provider, action string) (Result, error) {
	peers, err := p.Peers(ctx, messaging.PeersRequest{
		Sender:     messaging.Identity{RunID: p.Caller().RunID},
		Generation: p.Generation(),
	})
	if err != nil && len(peers.Peers) == 0 {
		return Result{}, refused(err)
	}
	records := make([]messaging.PeerRecord, 0, len(peers.Peers))
	for _, peer := range peers.Peers {
		var record messaging.PeerRecord
		if len(peer.Session) > 0 {
			if err := json.Unmarshal(peer.Session, &record); err != nil {
				return Result{}, refused(err)
			}
		}
		if record.RunID == "" {
			record.RunID = peer.RunID
		}
		records = append(records, record)
	}
	return Result{Action: action, Peers: records}, nil
}

// peersRename forwards the rename to the provider and passes its typed error
// through unchanged. Phase-1 admission owns the name's length and pattern,
// counted in runes like the published schema; the rename authority owns every
// other name rule plus resolution and permission.
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
	facts := map[string]string{
		"run_id":      quote(result.RunID),
		"label":       quote(label),
		"stop_reason": sanitize(last.StopReason),
	}
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
