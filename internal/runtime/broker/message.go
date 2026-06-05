package broker

// AgentMessage is the payload for agent-to-agent channel messages.
// It is used as the payload of "agent_message" control messages pushed
// via SendTo.
type AgentMessage struct {
	From      string `json:"from"`
	FromRunID string `json:"from_run_id"`
	Message   string `json:"message"`
	Role      string `json:"role"`
}
