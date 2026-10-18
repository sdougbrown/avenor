package messaging

// Outcome is the result of an ask operation. The string values are a wire
// contract shared by every transport and by the JavaScript clients, so they
// must never be renamed or reused for a different meaning.
type Outcome string

const (
	// OutcomeSent means the receiver admitted the ask and an edge exists.
	OutcomeSent Outcome = "sent"
	// OutcomeReplied means a reply was delivered to the waiting sender.
	OutcomeReplied Outcome = "replied"
	// OutcomeTimedOut means the bounded wait expired before a reply.
	OutcomeTimedOut Outcome = "timed-out"
	// OutcomeWithdrawn means the sender completed a withdrawal.
	OutcomeWithdrawn Outcome = "withdrawn"
	// OutcomeEdgeAbsent means no edge or waiter matched the request.
	OutcomeEdgeAbsent Outcome = "edge-absent"
	// OutcomeAuthRejected means the supplied credentials were refused.
	OutcomeAuthRejected Outcome = "auth-rejected"
	// OutcomeNotSent means the ask provably never reached admittance.
	OutcomeNotSent Outcome = "not-sent"
	// OutcomePossiblyPending means transmission could not be confirmed either
	// way; the message ID must be retained and never re-sent.
	OutcomePossiblyPending Outcome = "possibly-pending"
	// OutcomeLost means the ask is provably gone and cannot appear later.
	OutcomeLost Outcome = "lost"
)
