package parentlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// Messenger is one broker's parent-aware messaging seam. It answers from the
// broker's own queues and ask store first, forwards upward only what no local
// run can answer, and relays downward into the queue of the run a registered
// child owns. A broker with neither a parent nor a registered child composes
// nothing but its own local messenger, so its behavior is unchanged.
type Messenger struct {
	local messaging.Messenger
	// up is this branch's link to its parent broker, nil when it has none.
	up *Link
	// down relays into the children that registered with this broker. It is
	// built for every broker, because a child may register at any time.
	registry *Registry
	down     *relay
}

// New builds one broker's parent-aware messaging.
//
// A parent URL is required for the upward direction; a broker that only parents
// children is built with the same call and no parent URL. The generation is
// always required, because the relay reads it on every hop it takes into a
// child, and a parent URL adds the run set the link publishes.
func New(opts Options, local messaging.Messenger) (*Messenger, error) {
	if local == nil {
		return nil, fmt.Errorf("%w: a parent-aware messenger composes a local messenger", messaging.ErrInvalidRequest)
	}
	m := &Messenger{local: local, registry: NewRegistry()}
	if opts.ParentURL != "" {
		link, err := NewLink(opts)
		if err != nil {
			return nil, err
		}
		m.up = link
	}
	down, err := newRelay(opts, m.registry)
	if err != nil {
		return nil, err
	}
	m.down = down
	return m, nil
}

// Start publishes this branch's claim to its parent and keeps it published.
func (m *Messenger) Start(ctx context.Context) error {
	if m.up == nil {
		return nil
	}
	return m.up.Start(ctx)
}

// RunChanged asks the link to re-publish this branch's run set after a change.
func (m *Messenger) RunChanged() {
	if m.up != nil {
		m.up.RunChanged()
	}
}

// Close retracts this branch's claim and releases the links.
func (m *Messenger) Close() error {
	var err error
	if m.up != nil {
		err = m.up.Close()
	}
	if closeErr := m.down.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	return err
}

// Send admits an ask locally, in a child's queue through a relay, or in the
// parent broker's store when no branch here can answer it.
func (m *Messenger) Send(ctx context.Context, req messaging.SendRequest, legacy bool) (messaging.SendResult, error) {
	if m.registry.Owns(req.To.RunID) {
		return m.sendToChild(ctx, req, legacy)
	}
	if m.up == nil || m.local.Reachable(req.To.RunID) {
		return m.local.Send(ctx, req, legacy)
	}
	return m.up.Send(ctx, req)
}

// sendToChild admits the one edge a cross-branch ask owns in this broker's store
// and relays the message into the child's queue afterwards. The admission never
// performs network I/O, so the relay cannot run under the store's lock.
func (m *Messenger) sendToChild(ctx context.Context, req messaging.SendRequest, legacy bool) (messaging.SendResult, error) {
	if req.NoReply {
		// A tell allocates no edge, so nothing is admitted here.
		if err := m.down.Tell(ctx, messaging.DeliverRequest{
			FromRunID:     req.From.RunID,
			ToRunID:       req.To.RunID,
			Kind:          "agent_message",
			Payload:       json.RawMessage(req.Body),
			CorrelationID: req.MessageID,
		}); err != nil {
			return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID, Generation: req.Generation}, err
		}
		return messaging.SendResult{Outcome: messaging.OutcomeSent, MessageID: req.MessageID, Generation: req.Generation}, nil
	}
	res, err := m.local.Send(ctx, req, legacy)
	if err != nil || res.Outcome != messaging.OutcomeSent {
		return res, err
	}
	if err := m.down.Ask(ctx, req); err != nil {
		// The relay never reached the branch, so the edge this broker just
		// admitted is retracted instead of left waiting for a reply that cannot
		// arrive. Nothing was forwarded, so nothing is recalled.
		_, _ = m.local.Withdraw(ctx, messaging.WithdrawRequest{
			Sender:     req.From,
			MessageID:  req.MessageID,
			Generation: req.Generation,
		})
		return messaging.SendResult{Outcome: messaging.OutcomeNotSent, MessageID: req.MessageID, Generation: req.Generation}, err
	}
	return res, nil
}

// Wait blocks for the reply to an ask this messenger routed.
func (m *Messenger) Wait(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	if m.up != nil {
		if _, ok := m.up.askRoute(req.Sender.RunID, req.MessageID); ok {
			return m.up.Wait(ctx, req)
		}
	}
	return m.local.Wait(ctx, req)
}

// Withdraw withdraws an ask this messenger routed.
func (m *Messenger) Withdraw(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	if m.up != nil {
		if _, ok := m.up.askRoute(req.Sender.RunID, req.MessageID); ok {
			return m.up.Withdraw(ctx, req)
		}
	}
	return m.local.Withdraw(ctx, req)
}

// Reply completes the ask edge MessageID names. A relayed ask records its reply
// route on the hop it arrived on, so the reply follows that hop instead of
// degrading into a second delivery.
func (m *Messenger) Reply(ctx context.Context, req messaging.ReplyRequest) (messaging.Outcome, error) {
	outcome, err := m.local.Reply(ctx, req)
	if err != nil || outcome != messaging.OutcomeEdgeAbsent || m.up == nil {
		return outcome, err
	}
	return m.up.Reply(ctx, req)
}

// Deliver publishes a fire-and-forget message.
func (m *Messenger) Deliver(ctx context.Context, req messaging.DeliverRequest) error {
	if m.registry.Owns(req.ToRunID) {
		return m.down.Tell(ctx, req)
	}
	if m.up == nil || m.local.Reachable(req.ToRunID) {
		return m.local.Deliver(ctx, req)
	}
	return m.up.Deliver(ctx, req)
}

// Reachable reports whether runID can be reached from here. A branch with a
// parent reports every target it cannot answer itself as reachable: the parent
// is the authority on what its descendants own, and the ask that follows carries
// the refusal back. A local target never leaves this broker.
func (m *Messenger) Reachable(runID string) bool {
	if m.local.Reachable(runID) || m.registry.Owns(runID) {
		return true
	}
	return m.up != nil
}

// Peers lists the participants reachable from here: this broker's own runs,
// narrowed to what the authenticated caller may see, plus the runs its parent
// exports to it. A listing the parent could not finish reports the error beside
// the peers that were reached.
func (m *Messenger) Peers(ctx context.Context) ([]messaging.Peer, error) {
	peers, err := m.local.Peers(ctx)
	caller, _ := messaging.CallerRun(ctx)
	peers = m.registry.Export(peers, caller)
	if m.up == nil {
		return peers, err
	}
	remote, upErr := m.up.Peers(ctx)
	if upErr != nil {
		err = errors.Join(err, upErr)
	}
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		seen[peer.RunID] = struct{}{}
	}
	for _, peer := range remote {
		if _, ok := seen[peer.RunID]; ok {
			continue
		}
		seen[peer.RunID] = struct{}{}
		peers = append(peers, peer)
	}
	return peers, err
}

// Owns reports whether runID belongs to a registered child broker.
func (m *Messenger) Owns(runID string) bool { return m.registry.Owns(runID) }

// Register adopts one child broker's claim on the runs it owns. A run this
// broker itself owns is refused: it never belongs to a descendant, and
// routing it into a child would bypass the queue the local ask store admits
// edges for. A run the registry already records as a descendant's claim is not
// locally owned, even though the broker holds a run record it adopted for it:
// Registry.Register replaces that claim.
func (m *Messenger) Register(reg messaging.DescendantRegistration) (messaging.DescendantClaim, error) {
	for _, run := range reg.Runs {
		// A branch supervisor run is reserved in every branch, so the parent's own
		// supervisor never blocks a descendant's claim of the same ID: the
		// registry addresses it under a branch-namespaced run ID instead.
		if run.RunID != messaging.SupervisorRunID && m.local.Reachable(run.RunID) && !m.registry.Owns(run.RunID) {
			return messaging.DescendantClaim{}, fmt.Errorf("%w: %s is owned by this broker", ErrRunLocallyOwned, run.RunID)
		}
	}
	return m.registry.Register(reg)
}

// Deregister retires one child and reports the run keys it had claimed with the
// credential each claim carried, but only while the registration still resolves
// to the closing branch's view; a registration a same-identity replacement
// replaced is refused with messaging.ErrGenerationMismatch and nothing is
// retired.
func (m *Messenger) Deregister(childID string, view messaging.DescendantView) (map[string]string, error) {
	return m.registry.Deregister(childID, view)
}

// RecordRelay binds a relayed ask's reply route before the ask is exposed.
func (m *Messenger) RecordRelay(route messaging.RelayRoute) error {
	if m.up == nil {
		return fmt.Errorf("%w: this broker was relayed an ask but has no parent to answer it", messaging.ErrUnsupportedOperation)
	}
	return m.up.RecordRelay(route)
}

// Registry exposes the descendant registry for a host that reports on it.
func (m *Messenger) Registry() *Registry { return m.registry }

var (
	_ messaging.Messenger  = (*Messenger)(nil)
	_ messaging.ParentLink = (*Messenger)(nil)
)
