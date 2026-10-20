package runstate

// Follow-up conventions. A follow-up continues a prior run in a new one, so the
// supervisor's resume authority and the MCP follow-up surface must name the new
// run and its idempotency key identically: an edit to either convention has to
// reach both surfaces or a keyed retry stops replaying the first run.
const (
	// FollowUpLabelSuffix is appended to the prior display name when the caller
	// names the follow-up nothing.
	FollowUpLabelSuffix = "-followup"
	// FollowUpIdempotencyKeyPrefix namespaces a follow-up key, so a follow-up
	// keyed k never replays a spawn keyed the same string.
	FollowUpIdempotencyKeyPrefix = "follow_up:"
)
