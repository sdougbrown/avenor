package parentlink

import (
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestRegistryRefusesAnEndpointItCouldNotPostTo pins the registration's
// endpoint rule: the relay posts the target run's credential to the endpoint
// a child registered, so an endpoint that is not an absolute http address is
// refused at registration.
func TestRegistryRefusesAnEndpointItCouldNotPostTo(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
	}{
		{name: "a path", endpoint: "/relay"},
		{name: "a host without a scheme", endpoint: "child-one"},
		{name: "a scheme the relay cannot dial", endpoint: "file:///tmp/child"},
		{name: "an address without a host", endpoint: "http://"},
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
			if len(claim.Adopted) != 0 || len(claim.Retired) != 0 {
				t.Fatalf("Register = %+v, want nothing adopted and nothing retired", claim)
			}
			if registry.Owns("run-a") {
				t.Fatal("a refused registration adopted the run")
			}
		})
	}
}

// TestRegistryAcceptsAnHTTPRegistration pins the other side of the rule: an
// absolute http address is what the relay posts to, so it is accepted.
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
	hop, ok := registry.Hop("run-a")
	if !ok || hop.Endpoint != "http://child-one:8080" {
		t.Fatalf("hop = (%+v, %v), want the registered endpoint", hop, ok)
	}
}
