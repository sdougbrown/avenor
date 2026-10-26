package inprocess

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/askstore"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
)

var alice = messaging.Identity{RunID: "alice", Sender: "alice"}
var bob = messaging.Identity{RunID: "bob", Sender: "bob"}

func TestInProcessReplyRouting(t *testing.T) {
	store, _ := messagetest.NewMemoryStore(t)
	messagetest.RunReplyRoutingSuite(t, func(t *testing.T, id messaging.Identity) messaging.Transport {
		return New(store)
	}, messagetest.ReplyRoutingHooks{
		AdmitLegacy: func(t *testing.T, sender messaging.Identity, messageID, target string) {
			if _, err := store.Admit(t.Context(), askstore.AdmitRequest{
				Sender: sender, Target: target, MessageID: messageID, Legacy: true,
			}); err != nil {
				t.Fatalf("legacy Admit: %v", err)
			}
		},
		Rotate: func(t *testing.T) { store.Reset() },
	})
}

func mintID(t *testing.T, ttl time.Duration) string {
	t.Helper()
	now := time.Now().UTC()
	id, err := messaging.NewMsgID(now, now.Add(ttl))
	if err != nil {
		t.Fatalf("NewMsgID: %v", err)
	}
	return id
}

// TestWithdrawFencesPausedSend pauses a send before admission, withdraws the
// same key, and resumes the send. The resumed send must not deliver or leave an
// edge, and the fence must stay terminal.
func TestWithdrawFencesPausedSend(t *testing.T) {
	store, backend := messagetest.NewMemoryStore(t)
	messagetest.RunWithdrawFencesPausedSend(t, New(store), store, alice, bob)

	if delivered := backend.Queued(bob.RunID); delivered != 0 {
		t.Fatalf("withdrawn ask was delivered %d time(s)", delivered)
	}
	if store.PendingCount(alice.RunID) != 0 {
		t.Fatalf("pending count = %d, want 0", store.PendingCount(alice.RunID))
	}
}

func TestTransportCloseUnblocksWait(t *testing.T) {
	store, _ := messagetest.NewMemoryStore(t)
	tr := New(store)
	gen := store.Generation()
	msgID := mintID(t, time.Minute)
	if _, err := tr.Send(context.Background(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{"message":"ask"}`,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	type waitOutcome struct {
		res messaging.WaitResult
		err error
	}
	waited := make(chan waitOutcome, 1)
	go func() {
		res, err := tr.Wait(context.Background(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: gen, Timeout: time.Minute,
		})
		waited <- waitOutcome{res: res, err: err}
	}()
	time.Sleep(20 * time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- tr.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(messaging.CleanupDeadline + time.Second):
		t.Fatal("Close exceeded the cleanup deadline")
	}
	select {
	case out := <-waited:
		if out.err != nil {
			t.Fatalf("Wait: %v", out.err)
		}
		if out.res.Outcome != messaging.OutcomePossiblyPending {
			t.Fatalf("Wait outcome = %q, want possibly-pending", out.res.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not unblock on Close")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := tr.Send(context.Background(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: gen, Body: `{}`,
	}); !errors.Is(err, messaging.ErrTransportClosed) {
		t.Fatalf("Send after Close = %v, want ErrTransportClosed", err)
	}
	if _, err := tr.Reply(context.Background(), messaging.ReplyRequest{
		From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{}`,
	}); !errors.Is(err, messaging.ErrTransportClosed) {
		t.Fatalf("Reply after Close = %v, want ErrTransportClosed", err)
	}
}

// blockingMessenger holds cleanup after observing shutdown cancellation.
type blockingMessenger struct {
	messaging.Messenger
	entered        chan struct{}
	canceled       chan struct{}
	releaseCleanup chan struct{}
}

func (m blockingMessenger) block(ctx context.Context) error {
	m.entered <- struct{}{}
	<-ctx.Done()
	close(m.canceled)
	<-m.releaseCleanup
	return ctx.Err()
}

func (m blockingMessenger) Send(ctx context.Context, _ messaging.SendRequest, _ bool) (messaging.SendResult, error) {
	return messaging.SendResult{Outcome: messaging.OutcomeNotSent}, m.block(ctx)
}

func (m blockingMessenger) Wait(ctx context.Context, _ messaging.WaitRequest) (messaging.WaitResult, error) {
	return messaging.WaitResult{Outcome: messaging.OutcomeEdgeAbsent}, m.block(ctx)
}

func (m blockingMessenger) Reply(ctx context.Context, _ messaging.ReplyRequest) (messaging.Outcome, error) {
	return messaging.OutcomeNotSent, m.block(ctx)
}

func (m blockingMessenger) Withdraw(ctx context.Context, _ messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return messaging.WithdrawResult{Outcome: messaging.OutcomeNotSent}, m.block(ctx)
}

func (m blockingMessenger) Peers(ctx context.Context) ([]messaging.Peer, error) {
	return nil, m.block(ctx)
}

// TestDelegatedOperationsJoinClose pins that every delegated operation is
// bound to the transport's shutdown: Close cancels the call it is serving and
// waits for it, and no delegated operation starts once the transport closed.
func TestDelegatedOperationsJoinClose(t *testing.T) {
	store, _ := messagetest.NewMemoryStore(t)
	gen := store.Generation()
	msgID := mintID(t, time.Minute)

	type opOutcome struct {
		outcome messaging.Outcome
		err     error
	}
	cases := []struct {
		name          string
		closedOutcome messaging.Outcome
		call          func(context.Context, messaging.Transport) (messaging.Outcome, error)
	}{
		{
			name:          "send",
			closedOutcome: messaging.OutcomeNotSent,
			call: func(ctx context.Context, tr messaging.Transport) (messaging.Outcome, error) {
				res, err := tr.Send(ctx, messaging.SendRequest{
					From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{}`,
				})
				return res.Outcome, err
			},
		},
		{
			name:          "wait",
			closedOutcome: messaging.OutcomeEdgeAbsent,
			call: func(ctx context.Context, tr messaging.Transport) (messaging.Outcome, error) {
				res, err := tr.Wait(ctx, messaging.WaitRequest{
					Sender: alice, MessageID: msgID, Generation: gen, Timeout: time.Minute,
				})
				return res.Outcome, err
			},
		},
		{
			name:          "reply",
			closedOutcome: messaging.OutcomeNotSent,
			call: func(ctx context.Context, tr messaging.Transport) (messaging.Outcome, error) {
				res, err := tr.Reply(ctx, messaging.ReplyRequest{
					From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{"message":"pong"}`,
				})
				return res.Outcome, err
			},
		},
		{
			name:          "withdraw",
			closedOutcome: messaging.OutcomeNotSent,
			call: func(ctx context.Context, tr messaging.Transport) (messaging.Outcome, error) {
				res, err := tr.Withdraw(ctx, messaging.WithdrawRequest{
					Sender: alice, MessageID: msgID, Generation: gen,
				})
				return res.Outcome, err
			},
		},
		{
			name: "peers",
			call: func(ctx context.Context, tr messaging.Transport) (messaging.Outcome, error) {
				_, err := tr.Peers(ctx, messaging.PeersRequest{Sender: alice, Generation: gen})
				return "", err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{}, 1)
				canceled := make(chan struct{})
				releaseCleanup := make(chan struct{})
				defer func() {
					if releaseCleanup != nil {
						close(releaseCleanup)
					}
				}()
				tr := NewRouted(store, func() messaging.Messenger {
					return blockingMessenger{entered: entered, canceled: canceled, releaseCleanup: releaseCleanup}
				})
				called := make(chan opOutcome, 1)
				go func() {
					outcome, err := tc.call(context.Background(), tr)
					called <- opOutcome{outcome: outcome, err: err}
				}()
				<-entered

				closed := make(chan error, 1)
				go func() { closed <- tr.Close() }()
				<-canceled
				synctest.Wait()
				select {
				case err := <-closed:
					t.Fatalf("Close returned before delegated cleanup finished: %v", err)
				default:
				}
				assertRefused := func() {
					t.Helper()
					outcome, err := tc.call(context.Background(), tr)
					if !errors.Is(err, messaging.ErrTransportClosed) || outcome != tc.closedOutcome {
						t.Fatalf("%s after shutdown = (%q, %v), want (%q, ErrTransportClosed)", tc.name, outcome, err, tc.closedOutcome)
					}
					select {
					case <-entered:
						t.Fatal("delegated method started after shutdown")
					default:
					}
				}
				assertRefused()
				closedAgain := make(chan error, 1)
				go func() { closedAgain <- tr.Close() }()
				synctest.Wait()
				select {
				case err := <-closedAgain:
					t.Fatalf("concurrent Close returned before delegated cleanup finished: %v", err)
				default:
				}

				close(releaseCleanup)
				releaseCleanup = nil
				if err := <-closed; err != nil {
					t.Fatalf("Close: %v", err)
				}
				if err := <-closedAgain; err != nil {
					t.Fatalf("concurrent Close: %v", err)
				}
				if out := <-called; !errors.Is(out.err, context.Canceled) {
					t.Fatalf("delegated %s error = %v, want shutdown cancellation", tc.name, out.err)
				}
				assertRefused()
			})
		})
	}
}

func TestRoutedResolutionJoinsClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, _ := messagetest.NewMemoryStore(t)
		entered := make(chan error, 1)
		release := make(chan struct{})
		defer func() {
			if release != nil {
				close(release)
			}
		}()
		var tr messaging.Transport
		tr = NewRouted(store, func() messaging.Messenger {
			_, err := tr.Preflight(context.Background())
			entered <- err
			<-release
			return nil
		})
		called := make(chan error, 1)
		go func() {
			_, err := tr.Peers(context.Background(), messaging.PeersRequest{Generation: store.Generation()})
			called <- err
		}()
		if err := <-entered; err != nil {
			t.Fatalf("resolver's local Preflight: %v", err)
		}
		closed := make(chan error, 1)
		go func() { closed <- tr.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close returned before admitted resolution finished: %v", err)
		default:
		}
		close(release)
		release = nil
		if err := <-closed; err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := <-called; err != nil {
			t.Fatalf("Peers: %v", err)
		}
	})
}

// TestAskEpochLost pins the confirmed-generation-loss path: a replaced
// generation terminates the waiter as lost and refuses the old generation's
// requests.
func TestAskEpochLost(t *testing.T) {
	store, _ := messagetest.NewMemoryStore(t)
	tr := New(store)
	oldGen := store.Generation()
	msgID := mintID(t, time.Minute)
	if _, err := tr.Send(context.Background(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: oldGen, Body: `{"message":"ask"}`,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waited := make(chan messaging.WaitResult, 1)
	go func() {
		res, _ := tr.Wait(context.Background(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: oldGen, Timeout: time.Minute,
		})
		waited <- res
	}()
	time.Sleep(20 * time.Millisecond)

	store.Reset()

	select {
	case res := <-waited:
		if res.Outcome != messaging.OutcomeLost {
			t.Fatalf("outcome = %q, want lost", res.Outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter outlived the generation")
	}

	if pre, err := tr.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight: %v", err)
	} else if pre.Generation == oldGen {
		t.Fatal("Reset did not rotate the generation")
	}
	if _, err := tr.Send(context.Background(), messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: oldGen, Body: `{}`,
	}); !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("old-generation send error = %v", err)
	}
	if _, ok := store.Fence(messaging.FenceKey{Sender: alice.RunID, MessageID: msgID, Generation: oldGen}); ok {
		t.Fatal("old-generation fence survived rotation")
	}
}

// TestWaitResultKeepsTerminalOutcome pins that a wait which already reached a
// terminal state is not re-reported as possibly-pending when the transport
// closed while it was in flight.
func TestWaitResultKeepsTerminalOutcome(t *testing.T) {
	reply := messaging.WaitResult{
		Outcome: messaging.OutcomeReplied,
		Reply:   &messaging.InboundMessage{MessageID: "m-1", Payload: []byte(`{"message":"pong"}`)},
	}
	cases := []struct {
		name    string
		res     messaging.WaitResult
		err     error
		closed  bool
		want    messaging.Outcome
		wantErr bool
	}{
		{"replied survives close", reply, nil, true, messaging.OutcomeReplied, false},
		{"timed out survives close", messaging.WaitResult{Outcome: messaging.OutcomeTimedOut}, nil, true, messaging.OutcomeTimedOut, false},
		{"pending wait is re-reported after close", messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, context.Canceled, true, messaging.OutcomePossiblyPending, false},
		{"open transport keeps the error", messaging.WaitResult{Outcome: messaging.OutcomePossiblyPending}, context.Canceled, false, messaging.OutcomePossiblyPending, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := waitResult(tc.res, tc.err, tc.closed)
			if got.Outcome != tc.want {
				t.Fatalf("outcome = %q, want %q", got.Outcome, tc.want)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.want == messaging.OutcomeReplied && got.Reply == nil {
				t.Fatal("terminal reply was dropped")
			}
		})
	}
}

// gatedBackend holds the store's admission boundary open while its gate is
// armed, so a test can close the transport while a store call is queued.
type gatedBackend struct {
	*messagetest.MemoryBackend
	gate    chan struct{}
	reached chan struct{}
}

func (b *gatedBackend) Publish(target string, msg messaging.InboundMessage) (func(), error) {
	if b.gate != nil {
		close(b.reached)
		<-b.gate
	}
	return b.MemoryBackend.Publish(target, msg)
}

// TestWaitKeepsTerminalOutcomeAcrossClose pins the Transport.Wait wiring: a
// wait that reaches a terminal outcome keeps it when the transport closes while
// the call is still in flight, instead of being re-reported as possibly-pending.
func TestWaitKeepsTerminalOutcomeAcrossClose(t *testing.T) {
	backend := &gatedBackend{MemoryBackend: messagetest.NewMemoryBackend()}
	store := askstore.New(askstore.Options{})
	store.SetBackend(backend)
	tr := New(store)
	gen := store.Generation()

	msgID := mintID(t, time.Minute)
	if _, err := tr.Send(context.Background(), messaging.SendRequest{
		From: alice, To: bob, MessageID: msgID, Generation: gen, Body: `{"message":"ask"}`,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := tr.Reply(context.Background(), messaging.ReplyRequest{
		From: bob, ToRunID: alice.RunID, MessageID: msgID, Body: `{"message":"pong"}`,
	}); err != nil {
		t.Fatalf("Reply: %v", err)
	}

	// A second ask blocks inside Publish while holding the store lock, so the
	// wait below reaches the store only after Close.
	backend.reached = make(chan struct{})
	backend.gate = make(chan struct{})
	secondID := mintID(t, time.Minute)
	sent := make(chan error, 1)
	go func() {
		_, err := tr.Send(context.Background(), messaging.SendRequest{
			From: alice, To: bob, MessageID: secondID, Generation: gen, Body: `{"message":"second"}`,
		})
		sent <- err
	}()
	select {
	case <-backend.reached:
	case <-time.After(2 * time.Second):
		t.Fatal("second send never reached the publish gate")
	}

	type waitOutcome struct {
		res messaging.WaitResult
		err error
	}
	waited := make(chan waitOutcome, 1)
	go func() {
		res, err := tr.Wait(context.Background(), messaging.WaitRequest{
			Sender: alice, MessageID: msgID, Generation: gen, Timeout: time.Minute,
		})
		waited <- waitOutcome{res: res, err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(backend.gate)

	select {
	case out := <-waited:
		if out.err != nil {
			t.Fatalf("Wait: %v", out.err)
		}
		if out.res.Outcome != messaging.OutcomeReplied || out.res.Reply == nil {
			t.Fatalf("Wait = (%+v), want the terminal reply kept across Close", out.res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return")
	}
	if err := <-sent; err != nil {
		t.Fatalf("second Send: %v", err)
	}
}

// TestReplyRejectsNonJSONWithoutConsumingTheEdge pins the reply validation at
// the transport: a reply body that is not JSON is refused not-sent and leaves
// the edge live, so the same edge still completes on a reply that parses.
// The store owns the validation, so the bare and routed transports both
// inherit it; this test pins the bare surface.
func TestReplyRejectsNonJSONWithoutConsumingTheEdge(t *testing.T) {
	store, _ := messagetest.NewMemoryStore(t)
	tr := New(store)
	defer func() { _ = tr.Close() }()

	ctx := context.Background()
	sent, err := tr.Send(ctx, messaging.SendRequest{
		From: alice, To: bob, MessageID: mintID(t, time.Minute), Generation: store.Generation(),
		Body: `{"message":"ping"}`,
	})
	if err != nil || sent.Outcome != messaging.OutcomeSent {
		t.Fatalf("Send = (%+v, %v), want sent", sent, err)
	}
	replied, err := tr.Reply(ctx, messaging.ReplyRequest{
		From: bob, ToRunID: alice.RunID, MessageID: sent.MessageID, Body: `{"payload":`,
	})
	if replied.Outcome != messaging.OutcomeNotSent || !errors.Is(err, messaging.ErrInvalidRequest) {
		t.Fatalf("Reply(not JSON) = (%+v, %v), want not-sent invalid-request", replied, err)
	}
	replied, err = tr.Reply(ctx, messaging.ReplyRequest{
		From: bob, ToRunID: alice.RunID, MessageID: sent.MessageID, Body: `{"payload":"pong"}`,
	})
	if err != nil || replied.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Reply(valid) after the refusal = (%+v, %v), want replied", replied, err)
	}
	waited, err := tr.Wait(ctx, messaging.WaitRequest{
		Sender: alice, MessageID: sent.MessageID, Generation: store.Generation(), Timeout: time.Second,
	})
	if err != nil || waited.Outcome != messaging.OutcomeReplied {
		t.Fatalf("Wait = (%+v, %v), want the replied edge", waited, err)
	}
}
