package messagetest_test

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/hosthttp"
	"github.com/sdougbrown/avenor/internal/messaging/inprocess"
	"github.com/sdougbrown/avenor/internal/messaging/internal/hostfixture"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// TestMessagingImplementations is the combined entry point: every shipped
// transport runs the shared contract and the shared failure matrix.
func TestMessagingImplementations(t *testing.T) {
	t.Run("inprocess", func(t *testing.T) {
		store, _ := messagetest.NewMemoryStore(t)
		runImplementation(t, func(*testing.T, messaging.Identity) messaging.Transport {
			return inprocess.New(store)
		})
	})

	t.Run("localhttp", func(t *testing.T) {
		b := startBroker(t)
		runImplementation(t, func(t *testing.T, id messaging.Identity) messaging.Transport {
			registerPeers(t, b, id)
			return localTransport(t, b, id)
		})
	})

	t.Run("hosthttp", func(t *testing.T) {
		fixture := hostfixture.New(t)
		runImplementation(t, func(t *testing.T, id messaging.Identity) messaging.Transport {
			tr, err := hosthttp.New(fixture.Descriptor(id))
			if err != nil {
				t.Fatalf("hosthttp.New: %v", err)
			}
			t.Cleanup(func() { _ = tr.Close() })
			return tr
		})
	})
}

func runImplementation(t *testing.T, factory messagetest.TransportFactory) {
	t.Helper()
	messagetest.RunTransportSuite(t, factory)
	messagetest.RunFailureMatrix(t, factory)
}

func startBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b := broker.New("")
	if err := b.Start(); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

func localTransport(t *testing.T, b *broker.Broker, id messaging.Identity) messaging.Transport {
	t.Helper()
	token, _ := b.EnsureRun(id.RunID, id.Sender)
	tr, err := localhttp.New(b.Addr(), messaging.LocalCredentials{RunID: id.RunID, Token: token})
	if err != nil {
		t.Fatalf("localhttp.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

// registerPeers registers the run identities the shared suites address but
// never build a transport for; the local broker refuses delivery to a run it
// does not know.
func registerPeers(t *testing.T, b *broker.Broker, id messaging.Identity) {
	t.Helper()
	prefix, _, ok := strings.Cut(id.RunID, "-")
	if !ok {
		return
	}
	for _, peer := range []string{"alice", "bob", "eve"} {
		b.EnsureRun(prefix+"-"+peer, peer)
	}
}
