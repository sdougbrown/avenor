package parentlink

import (
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestRegistryRefusesAnEndpointItCouldNotPostTo pins the registration's
// endpoint rule: the relay posts the target run's credential to the endpoint
// a child registered, so an endpoint that is not an absolute http root
// address is refused at registration.
func TestRegistryRefusesAnEndpointItCouldNotPostTo(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
	}{
		{name: "a path", endpoint: "/relay"},
		{name: "a host without a scheme", endpoint: "child-one"},
		{name: "a scheme the relay cannot dial", endpoint: "file:///tmp/child"},
		{name: "an address without a host", endpoint: "http://"},
		{name: "an http address with a path", endpoint: "http://child-one/broker"},
		{name: "a query the relay's hop would swallow", endpoint: "http://child-one:8080?x=1"},
		{name: "userinfo the relay would carry as a credential", endpoint: "http://user@child-one:8080"},
		{name: "a port the relay cannot dial", endpoint: "http://child-one:70000"},
		{name: "a wildcard host", endpoint: "http://0.0.0.0:8080"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := NewRegistry()
			claim, err := registry.Register(messaging.DescendantRegistration{
				ChildID:    "child-one",
				Endpoint:   testCase.endpoint,
				Generation: "gen-a",
				Runs:       []messaging.RunClaim{{RunID: "run-a"}},
			})
			if !errors.Is(err, messaging.ErrInvalidRequest) {
				t.Fatalf("Register = %v, want ErrInvalidRequest", err)
			}
			if len(claim.Retired) != 0 {
				t.Fatalf("Register = %+v, want nothing retired", claim)
			}
			if registry.Owns(ParentRunKey("child-one", "run-a")) {
				t.Fatal("a refused registration adopted the run")
			}
		})
	}
}

// TestRegistryAcceptsAnHTTPRegistration pins the other side of the rule: the
// root http address the relay posts to is accepted, and the hop carries it
// unchanged.
func TestRegistryAcceptsAnHTTPRegistration(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "child-one",
		Endpoint:   "http://child-one:8080",
		Generation: "gen-a",
		Runs:       []messaging.RunClaim{{RunID: "run-a"}},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	hop, ok := registry.Hop(ParentRunKey("child-one", "run-a"))
	if !ok || hop.Endpoint != "http://child-one:8080" {
		t.Fatalf("hop = (%+v, %v), want the registered endpoint", hop, ok)
	}
}

// TestRegistryAcceptsAnEmptyEndpoint pins the relay-specific side of the rule:
// an endpoint that names no route at all is still registered, and the hop it
// leaves behind reports no route for the relay to post to.
func TestRegistryAcceptsAnEmptyEndpoint(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Register(messaging.DescendantRegistration{
		ChildID:    "child-one",
		Generation: "gen-a",
		Runs:       []messaging.RunClaim{{RunID: "run-a"}},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, ok := registry.Hop(ParentRunKey("child-one", "run-a")); ok {
		t.Fatal("an empty endpoint produced a hop for the relay to post to")
	}
}
