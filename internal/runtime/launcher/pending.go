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
	cancels map[*context.CancelFunc]bool
	idle    chan struct{}
}

// Begin derives a launch context and returns functions to detach it from
// shutdown and settle the attempt. Detach under the provider lock when adopting
// a client, then settle once the provider has finished using it.
func (p *Pending) Begin(ctx context.Context) (context.Context, func(), func(keep bool)) {
	ctx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	if len(p.cancels) == 0 {
		p.cancels = make(map[*context.CancelFunc]bool)
		p.idle = make(chan struct{})
	}
	p.cancels[&cancel] = true
	p.mu.Unlock()
	return ctx, func() {
			p.mu.Lock()
			if _, ok := p.cancels[&cancel]; ok {
				p.cancels[&cancel] = false
			}
			p.mu.Unlock()
		}, func(keep bool) {
			if !keep {
				cancel()
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			delete(p.cancels, &cancel)
			if len(p.cancels) == 0 && p.idle != nil {
				close(p.idle)
				p.idle = nil
			}
		}
}

// CancelAndWait cancels unadopted launches and bounds the wait for cleanup.
// The provider must close admission before calling it.
func (p *Pending) CancelAndWait() {
	p.mu.Lock()
	for cancel, attached := range p.cancels {
		if attached {
			(*cancel)()
		}
	}
	idle := p.idle
	p.mu.Unlock()
	if idle != nil {
		select {
		case <-idle:
		case <-time.After(2 * time.Second):
			p.mu.Lock()
			if p.idle == idle {
				p.idle = nil
			}
			p.mu.Unlock()
		}
	}
}
