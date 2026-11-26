package launcher

import (
	"context"
	"sync"
	"time"
)

// Pending tracks client launches until they are either adopted or cleaned up.
// The provider serializes Begin and adoption with its decision to close.
type Pending struct {
	mu      sync.Mutex
	cancels map[*context.CancelFunc]struct{}
	idle    chan struct{}
}

// Begin derives the launch context and returns a function that settles it.
// Settle with keep=true when installing the client under the provider lock.
// A retained client's process still follows the original caller context.
func (p *Pending) Begin(ctx context.Context) (context.Context, func(keep bool)) {
	ctx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	if len(p.cancels) == 0 {
		p.cancels = make(map[*context.CancelFunc]struct{})
		p.idle = make(chan struct{})
	}
	p.cancels[&cancel] = struct{}{}
	p.mu.Unlock()
	return ctx, func(keep bool) {
		if !keep {
			cancel()
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.cancels, &cancel)
		if len(p.cancels) == 0 {
			close(p.idle)
			p.idle = nil
		}
	}
}

// CancelAndWait cancels unadopted launches and bounds the wait for cleanup.
// The provider must close admission before calling it.
func (p *Pending) CancelAndWait() {
	p.mu.Lock()
	for cancel := range p.cancels {
		(*cancel)()
	}
	idle := p.idle
	p.mu.Unlock()
	if idle != nil {
		select {
		case <-idle:
		case <-time.After(2 * time.Second):
		}
	}
}
