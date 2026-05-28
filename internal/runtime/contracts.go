package runtime

// Shared runtime event names.
const (
	EventChildQuestion = "child.question"
)

// ChildQuestionPayload is the shared contract for child->parent clarification
// events emitted by the supervisor and consumed by orchestrators.
type ChildQuestionPayload struct {
	RuntimeID string `json:"runtime_id"`
	SessionID string `json:"session_id"`
	ChildID   string `json:"child_id"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// Fields returns the event field map used in events.Event.Fields.
func (p ChildQuestionPayload) Fields() map[string]any {
	return map[string]any{
		"runtime_id": p.RuntimeID,
		"session_id": p.SessionID,
		"child_id":   p.ChildID,
		"message":    p.Message,
		"request_id": p.RequestID,
	}
}
