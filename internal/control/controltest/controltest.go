// Package controltest constructs control servers for tests in other packages
// without adding a test-only constructor to the control API.
package controltest

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// NewServer returns a control server and the capability it will accept, so the
// caller can authenticate as the construction owner.
func NewServer(state *control.ControlState) (*control.ControlServer, control.OwnerCapability) {
	owner, err := control.GenerateOwnerCapability()
	if err != nil {
		panic("controltest: generate owner capability: " + err.Error())
	}
	return control.NewServer(state, owner), owner
}

// SetMessageTransport installs a handler whose typed messaging verbs serve the
// given transport, so a caller in another package can drive a messaging verb
// against a real control server.
func SetMessageTransport(srv *control.ControlServer, transport messaging.Transport) {
	srv.SetStableHandler(&MessageHandler{Transport: transport})
}

// MessageHandler is a stable handler that serves the typed messaging verbs
// from one transport and stubs every other verb.
type MessageHandler struct {
	Transport messaging.Transport
}

func (h *MessageHandler) Spawn(json.RawMessage) (any, error) { return nil, nil }

func (h *MessageHandler) SpawnMCP(json.RawMessage) (any, error) { return nil, nil }

func (h *MessageHandler) List() any { return nil }

func (h *MessageHandler) Shutdown(string) error { return nil }

func (h *MessageHandler) RuntimeStatus(string) (any, error) { return nil, nil }

func (h *MessageHandler) RuntimeWaitTurn(context.Context, string, time.Duration) (any, error) {
	return nil, nil
}

func (h *MessageHandler) RuntimeCancel(string) error { return nil }

func (h *MessageHandler) RuntimePrompt(string, string, string) error { return nil }

func (h *MessageHandler) RuntimeAnswerPermission(string, string, string, string) error { return nil }

func (h *MessageHandler) RuntimeInterruptAndPrompt(string, string, bool) error { return nil }

func (h *MessageHandler) RuntimeSendToParent(string, string) error { return nil }

func (h *MessageHandler) MessageTransport() (messaging.Transport, error) { return h.Transport, nil }

func (h *MessageHandler) Resume(messaging.Caller, messaging.ResumeRequest) (messaging.ResumeResult, error) {
	return messaging.ResumeResult{}, nil
}

func (h *MessageHandler) SessionRename(messaging.Caller, string, string) (messaging.RenameResult, error) {
	return messaging.RenameResult{}, nil
}

func (h *MessageHandler) SessionLast(string) (broker.LastAgentMessage, bool, error) {
	return broker.LastAgentMessage{}, false, nil
}

func (h *MessageHandler) SessionLastSet(messaging.Caller, string, string, string, bool) error {
	return nil
}

func (h *MessageHandler) TreeBudgetStatus() any { return nil }

func (h *MessageHandler) WaitForCapacityMS(int) error { return nil }
