package stable

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// stage2Provider starts a session and parks it: the run stays running until the
// supervisor cancels it, so Stage 2's run-store tests never race a completion.
type stage2Provider struct {
	mu       sync.Mutex
	starts   []runtime.StartOptions
	resumes  []string
	sessions map[string]chan events.Event
	next     int
}

func newStage2Provider() *stage2Provider {
	return &stage2Provider{sessions: map[string]chan events.Event{}}
}

func (p *stage2Provider) Start(_ context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	sessionID := fmt.Sprintf("ses_stage2_%d", p.next)
	p.starts = append(p.starts, opts)
	p.sessions[sessionID] = make(chan events.Event)
	return runtime.Session{SessionID: sessionID, Dir: opts.Dir}, nil
}

func (p *stage2Provider) Resume(_ context.Context, sessionID string) (runtime.Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumes = append(p.resumes, sessionID)
	if p.sessions[sessionID] == nil {
		p.sessions[sessionID] = make(chan events.Event)
	}
	return runtime.Session{SessionID: sessionID}, nil
}

func (p *stage2Provider) resumesRecorded() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.resumes...)
}

func (p *stage2Provider) Prompt(context.Context, string, string) error { return nil }

func (p *stage2Provider) Cancel(_ context.Context, sessionID string) error {
	p.mu.Lock()
	ch := p.sessions[sessionID]
	delete(p.sessions, sessionID)
	p.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}

func (p *stage2Provider) Events(_ context.Context, sessionID string) (<-chan events.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := p.sessions[sessionID]
	if ch == nil {
		ch = make(chan events.Event)
		p.sessions[sessionID] = ch
	}
	return ch, nil
}

func (p *stage2Provider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}

func (p *stage2Provider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

func (p *stage2Provider) startsRecorded() []runtime.StartOptions {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]runtime.StartOptions(nil), p.starts...)
}

// newStage2Supervisor builds a supervisor with a broker and a parking provider.
func newStage2Supervisor(t *testing.T, maxRuntimes int) (*Supervisor, *stage2Provider) {
	t.Helper()
	if maxRuntimes == 0 {
		maxRuntimes = 8
	}
	sup := NewSupervisor(Config{ControlSocket: newStableSocketPath(t, "s2"), MaxRuntimes: maxRuntimes})
	if sup.broker == nil {
		t.Fatal("supervisor did not create a broker")
	}
	provider := newStage2Provider()
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		return provider, nil
	}
	t.Cleanup(func() {
		_ = sup.Shutdown("kill")
		_ = sup.broker.Stop()
		sup.stopReaper()
	})
	return sup, provider
}
