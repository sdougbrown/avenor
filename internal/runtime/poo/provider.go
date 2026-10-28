// Package poo drives a poo binary over its public stdio protocol.
//
// Stage 1 resolves the roster entry's executable and records it in the session
// ownership identity. The protocol client, session lifecycle and event mapping
// land in Stage 2: until then no process is started and every provider call
// reports the missing implementation.
package poo

import (
	"context"
	"errors"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

const backendID = "poo"

// ErrNotImplemented is the Stage 1 provider boundary. Start returns it before
// any process is created, so a poo rostered run fails with a clear reason
// instead of an ambiguous transport error.
var ErrNotImplemented = errors.New("the poo backend is not implemented yet")

// Provider is the poo backend. Stage 1 keeps only the start options the
// resolved launch identity travels on.
type Provider struct {
	opts runtime.StartOptions
}

// NewWithOptions builds the poo provider for one session.
func NewWithOptions(opts runtime.StartOptions) *Provider {
	return &Provider{opts: opts}
}

var _ runtime.Provider = (*Provider)(nil)

func (p *Provider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{}, ErrNotImplemented
}

func (p *Provider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{}, ErrNotImplemented
}

func (p *Provider) Prompt(context.Context, string, string) error {
	return ErrNotImplemented
}

func (p *Provider) Cancel(context.Context, string) error {
	return ErrNotImplemented
}

func (p *Provider) Events(context.Context, string) (<-chan events.Event, error) {
	return nil, ErrNotImplemented
}

func (p *Provider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return ErrNotImplemented
}

// Capabilities describes what this Stage 1 backend can do: nothing beyond
// being poo. Resume, permissions and model selection are reported when the
// protocol client that implements them lands, since Stage 1 has no handshake
// to read them from.
func (p *Provider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{Backend: backendID}, nil
}
