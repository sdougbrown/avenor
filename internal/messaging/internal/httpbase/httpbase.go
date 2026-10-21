// Package httpbase holds the HTTP plumbing the messaging adapters share: one
// client that never follows a redirect, the client-side lifecycle Close
// terminates, a bounded response reader and the wire fence shape. It imports
// only the standard library and the messaging leaf.
package httpbase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

const (
	// DefaultRequestTimeout bounds one request when the caller sets none.
	DefaultRequestTimeout = 30 * time.Second

	// MaxResponseBytes caps a response body.
	MaxResponseBytes = 1 << 20
)

// ErrRedirect reports a response that would redirect the request. A client
// never follows one, so credentials cannot be offered to another host.
var ErrRedirect = errors.New("messaging http: redirect refused")

// Fence is the wire form of an ask fence. Both adapters decode it and report
// it through the shared type.
type Fence struct {
	Sender     string    `json:"sender"`
	MessageID  string    `json:"message_id"`
	Generation string    `json:"generation"`
	Deadline   time.Time `json:"deadline"`
	Terminal   bool      `json:"terminal"`
}

// Ack maps the wire fence to the shared acknowledgement.
func (f Fence) Ack() messaging.FenceAck {
	return messaging.FenceAck{
		Sender:     f.Sender,
		MessageID:  f.MessageID,
		Generation: f.Generation,
		Deadline:   f.Deadline,
		Terminal:   f.Terminal,
	}
}

// Client is the plumbing one HTTP adapter embeds.
type Client struct {
	http      *http.Client
	malformed error
	timeout   time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
}

// New builds a client whose unreadable or over-cap responses are reported
// under malformed, and whose requests default to timeout.
func New(malformed error, timeout time.Duration) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		http:      &http.Client{Transport: &http.Transport{}, CheckRedirect: RefuseRedirect},
		malformed: malformed,
		timeout:   timeout,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// RefuseRedirect is the client's CheckRedirect: it never follows a redirect.
func RefuseRedirect(req *http.Request, _ []*http.Request) error {
	return fmt.Errorf("%w: %s", ErrRedirect, req.URL.Redacted())
}

// UseHTTPClient adopts the transport and per-request timeout of a caller-owned
// client, so an adapter sends through the plumbing its surface configured. It
// must run before the client's first request. The caller's CheckRedirect is not
// adopted: an adapter never follows a redirect, so credentials cannot be
// offered to another host.
func (c *Client) UseHTTPClient(hc *http.Client) {
	if hc == nil {
		return
	}
	c.http.Transport = hc.Transport
	if hc.Timeout > 0 {
		c.timeout = hc.Timeout
	}
}

// SetTransport replaces the round tripper the client sends with.
func (c *Client) SetTransport(rt http.RoundTripper) {
	c.http.Transport = rt
}

// Do sends req on a context cancelled by ctx, the request timeout or Close, and
// returns the status and a bounded response body.
func (c *Client) Do(ctx context.Context, req *http.Request, timeout time.Duration) (int, []byte, error) {
	if timeout <= 0 {
		timeout = c.timeout
	}
	child, release, ok := c.child(ctx, timeout)
	if !ok {
		return 0, nil, messaging.ErrTransportClosed
	}
	defer release()

	resp, err := c.http.Do(req.WithContext(child))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if len(raw) > MaxResponseBytes {
		return resp.StatusCode, nil, fmt.Errorf("%w: response body exceeds %d bytes", c.malformed, MaxResponseBytes)
	}
	return resp.StatusCode, raw, nil
}

// Close releases client-side resources, unblocks in-flight requests with the
// cancellation the caller classifies, and joins its goroutines. It never stops
// the peer.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.cancel()
	c.mu.Unlock()
	c.wg.Wait()
	c.http.CloseIdleConnections()
	return nil
}

// IsClosed reports whether Close has run.
func (c *Client) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Open refuses work a closed client cannot transmit and a call that pins no
// generation.
func (c *Client) Open(generation string) error {
	if c.IsClosed() {
		return messaging.ErrTransportClosed
	}
	if generation == "" {
		return messaging.ErrGenerationMismatch
	}
	return nil
}

// child returns a request context cancelled by ctx, the request timeout or
// transport shutdown, and registers the watcher so Close can join it.
func (c *Client) child(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, false
	}
	c.wg.Add(1)
	c.mu.Unlock()

	base, stop := context.WithTimeout(ctx, timeout)
	child, cancel := context.WithCancel(base)
	go func() {
		defer c.wg.Done()
		select {
		case <-c.ctx.Done():
			cancel()
		case <-child.Done():
		}
	}()
	return child, func() { cancel(); stop() }, true
}

// NeverTransmitted reports whether a request provably never reached the peer:
// the connection was refused before any byte was written.
func NeverTransmitted(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// IsTransportCancelled reports whether err is the cancellation Close applies to
// in-flight requests rather than a fault the caller or the peer produced.
func IsTransportCancelled(err error) bool {
	return errors.Is(err, messaging.ErrTransportClosed) || errors.Is(err, context.Canceled)
}

// WaitTimeout clamps a wait to the protocol cap and floors it at the smallest
// duration the wire carries, so a positive sub-millisecond wait is not
// truncated to zero (which a peer reads as its default wait).
func WaitTimeout(d time.Duration) time.Duration {
	if d <= 0 || d > messaging.MaxWait {
		return messaging.MaxWait
	}
	if d < time.Millisecond {
		return time.Millisecond
	}
	return d
}
