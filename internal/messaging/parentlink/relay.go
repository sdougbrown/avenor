package parentlink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/internal/httpbase"
)

// relay carries a message this broker admitted into the queue of the descendant
// run that owns it. It is exactly one hop: the child publishes the message into
// the run's queue without admitting it again, so the ancestor keeps the one edge
// the cross-branch ask owns.
type relay struct {
	registry *Registry
	client   *httpbase.Client
	// token reports the credential this broker issued for a descendant run.
	token func(runID string) (string, bool)
	// generation reports this broker's current ask generation, which the child
	// records as the generation its eventual reply is bound to.
	generation func() string
}

// newRelay builds the relay that reaches the children registered with this
// broker. The relay reads the run credential and the generation on every hop,
// so a missing callback is refused here rather than at the first relay.
func newRelay(opts Options, registry *Registry) (*relay, error) {
	if opts.RunToken == nil {
		return nil, fmt.Errorf("%w: a parent broker needs the credential it authenticates a descendant run with", messaging.ErrInvalidRequest)
	}
	if opts.Generation == nil {
		return nil, fmt.Errorf("%w: a parent broker needs its current ask generation", messaging.ErrInvalidRequest)
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = httpbase.DefaultRequestTimeout
	}
	return &relay{
		registry:   registry,
		client:     httpbase.New(ErrUnsupportedParentProtocol, timeout),
		token:      opts.RunToken,
		generation: opts.Generation,
	}, nil
}

// Ask relays one ask this broker admitted. The child publishes it into the
// target run's queue and records the reply route, but admits no second edge.
func (r *relay) Ask(ctx context.Context, req messaging.SendRequest) error {
	payload, err := payloadFor(req.Body)
	if err != nil {
		return err
	}
	return r.hop(ctx, req.To.RunID, messaging.DeliverRequest{
		FromRunID:     req.From.RunID,
		ToRunID:       req.To.RunID,
		Kind:          "agent_message",
		Payload:       payload,
		CorrelationID: req.MessageID,
	}, true)
}

// Tell relays a fire-and-forget message this broker did not admit an edge for.
func (r *relay) Tell(ctx context.Context, req messaging.DeliverRequest) error {
	kind := req.Kind
	if kind == "" {
		kind = "agent_message"
	}
	payload, err := payloadFor(string(req.Payload))
	if err != nil {
		return err
	}
	return r.hop(ctx, req.ToRunID, messaging.DeliverRequest{
		FromRunID:     req.FromRunID,
		ToRunID:       req.ToRunID,
		Kind:          kind,
		Payload:       payload,
		CorrelationID: req.CorrelationID,
	}, false)
}

// hop posts one relay to the child that owns the target run.
func (r *relay) hop(ctx context.Context, target string, req messaging.DeliverRequest, expectsReply bool) error {
	next, ok := r.registry.Hop(target)
	if !ok {
		return fmt.Errorf("%w: no registered branch owns run %q", messaging.ErrTargetUnavailable, target)
	}
	token, ok := r.token(target)
	if !ok || token == "" {
		return fmt.Errorf("%w: this broker has no credential for run %q", messaging.ErrTargetUnavailable, target)
	}
	body, err := json.Marshal(map[string]any{
		"run_id":          target,
		"token":           token,
		"from_run_id":     req.FromRunID,
		"to_run_id":       target,
		"type":            req.Kind,
		"payload":         req.Payload,
		"message_id":      req.CorrelationID,
		"generation":      next.Generation,
		"edge_generation": r.generation(),
		"expects_reply":   expectsReply,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", messaging.ErrInvalidRequest, err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, next.Endpoint+pathRelay, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	status, raw, err := r.client.Do(ctx, httpReq, 0)
	if err != nil {
		// The hop never reached the child, so nothing was queued there.
		return fmt.Errorf("%w: %v", ErrParentUnavailable, err)
	}
	if _, decErr := decode(status, raw); decErr != nil {
		switch {
		case errors.Is(decErr, messaging.ErrGenerationMismatch),
			errors.Is(decErr, messaging.ErrAuthRejected):
			// The child no longer answers as the branch it registered as, so
			// the whole registration stops being a routing target instead of
			// costing one doomed round trip per run it owns.
			_, _ = r.registry.Deregister(next.ChildID)
			return fmt.Errorf("%w: the branch that owns run %q refused the relay: %v",
				messaging.ErrTargetUnavailable, target, decErr)
		case errors.Is(decErr, ErrParentUnavailable):
			// The parent failed without saying anything about the branch, so
			// the claim it registered stays and the relay is retried.
			return decErr
		case errors.Is(decErr, messaging.ErrTargetUnavailable):
			// The child retired this run, so only this run stops being routed.
			r.registry.Forget(target)
			return fmt.Errorf("%w: the branch that owns run %q refused the relay: %v",
				messaging.ErrTargetUnavailable, target, decErr)
		}
		return decErr
	}
	return nil
}

// Close releases the relay's HTTP client.
func (r *relay) Close() error {
	return r.client.Close()
}
