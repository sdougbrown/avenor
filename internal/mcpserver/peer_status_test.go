package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/messaging"
)

const brokerOnlyRunID = "rt_plugin_session"

// brokerPeerSession is one broker session row as the peers listing carries it.
func brokerPeerSession(t *testing.T, runID, label, status string) messaging.Peer {
	t.Helper()
	session, err := json.Marshal(map[string]string{
		"run_id":  runID,
		"label":   label,
		"status":  status,
		"backend": "claude",
		"model":   "opus",
		"dir":     "/work/" + label,
	})
	if err != nil {
		t.Fatal(err)
	}
	return messaging.Peer{RunID: runID, Session: session}
}

// brokerListingServer builds a server whose supervisor has no runtime for any id
// while the broker lists peers, or fails to list them when listErr is set.
func brokerListingServer(t *testing.T, listErr error, peers ...messaging.Peer) *Server {
	t.Helper()
	cl := &fakeMessageControlClient{
		fakeClient: &fakeClient{statusFunc: func(runtimeID string) (map[string]any, error) {
			return nil, &client.RPCError{Code: -32000, Message: fmt.Sprintf("runtime %q not found", runtimeID)}
		}},
		peersFunc: func(context.Context, messaging.PeersRequest) (messaging.PeersResult, error) {
			if listErr != nil {
				return messaging.PeersResult{}, listErr
			}
			return messaging.PeersResult{Peers: peers, Generation: "gen-1"}, nil
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func brokerOnlyServer(t *testing.T) *Server {
	t.Helper()
	return brokerListingServer(t, nil, brokerPeerSession(t, brokerOnlyRunID, "plugin-claude", "idle"))
}

func strVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func TestAvenorStatusReadsABrokerOnlyPeerByRunIDOrLabel(t *testing.T) {
	for _, key := range []string{brokerOnlyRunID, "plugin-claude"} {
		t.Run(key, func(t *testing.T) {
			s := brokerOnlyServer(t)
			_, out, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: key})
			if err != nil {
				t.Fatalf("status of a broker peer: %v", err)
			}
			got := map[string]string{
				"run_id":  strVal(out.RunID),
				"label":   strVal(out.Label),
				"status":  strVal(out.Status),
				"backend": strVal(out.Backend),
				"model":   strVal(out.Model),
				"dir":     strVal(out.Dir),
			}
			want := map[string]string{
				"run_id":  brokerOnlyRunID,
				"label":   "plugin-claude",
				"status":  "running",
				"backend": "claude",
				"model":   "opus",
				"dir":     "/work/plugin-claude",
			}
			for k, v := range want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
			if out.RuntimeID != nil || out.Phase != nil || out.EventPath != nil {
				t.Errorf("a broker peer carries no runtime fields: %+v", out.statusRun)
			}
		})
	}
}

// TestAvenorStatusTranslatesAPeersStatusLikeTheListing pins that a finished
// supervisor child seen from a branch answers the lifecycle word `peers list`
// shows for it, not the broker's raw "ended".
func TestAvenorStatusTranslatesAPeersStatusLikeTheListing(t *testing.T) {
	s := brokerListingServer(t, nil, brokerPeerSession(t, "rt_done", "finished-child", "ended"))
	_, out, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt_done"})
	if err != nil {
		t.Fatalf("status of a finished broker peer: %v", err)
	}
	if got := strVal(out.Status); got != "done" {
		t.Fatalf("status = %q, want done", got)
	}
}

func TestAvenorStatusShapesABrokerPeerForTheLifecycleView(t *testing.T) {
	s := brokerOnlyServer(t)
	_, out, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: brokerOnlyRunID, View: "lifecycle"})
	if err != nil {
		t.Fatalf("status of a broker peer: %v", err)
	}
	if strVal(out.RunID) != brokerOnlyRunID || strVal(out.Status) != "running" {
		t.Fatalf("lifecycle view = %+v, want run id and status", out.statusRun)
	}
	if out.Backend != nil || out.Model != nil || out.Dir != nil {
		t.Fatalf("lifecycle view kept identity detail: %+v", out.statusRun)
	}
}

func TestAvenorStatusRefusesToWaitOnABrokerPeer(t *testing.T) {
	s := brokerOnlyServer(t)
	_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: brokerOnlyRunID, WaitFor: "terminal"})
	if !errors.Is(err, errBrokerPeerNoWait) {
		t.Fatalf("error = %v, want the broker-peer refusal to wait", err)
	}
}

func TestAvenorStatusKeepsTheRuntimeErrorForAnUnknownRun(t *testing.T) {
	s := brokerOnlyServer(t)
	_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "rt_nobody"})
	want := `status: rpc error [-32000]: runtime "rt_nobody" not found`
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// TestAvenorStatusAnswersNoPeerWhenTheListingCannotNameOne pins the two cases
// that must keep the supervisor's own error: a label two peers share names no
// single peer, and a listing that cannot be read names none.
func TestAvenorStatusAnswersNoPeerWhenTheListingCannotNameOne(t *testing.T) {
	cases := map[string]*Server{
		"shared label": brokerListingServer(t, nil,
			brokerPeerSession(t, "rt_a", "twin", "idle"),
			brokerPeerSession(t, "rt_b", "twin", "idle")),
		"listing failure": brokerListingServer(t, errors.New("listing unavailable")),
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			key := "twin"
			if name == "listing failure" {
				key = brokerOnlyRunID
			}
			_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: key})
			want := fmt.Sprintf(`status: rpc error [-32000]: runtime %q not found`, key)
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
		})
	}
}

func TestAvenorEventsRefusesABrokerPeerAndPointsAtPeersLast(t *testing.T) {
	s := brokerOnlyServer(t)
	_, _, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: brokerOnlyRunID})
	if !errors.Is(err, errBrokerPeerNoEvents) || !strings.Contains(err.Error(), "last") {
		t.Fatalf("error = %v, want the broker-peer refusal naming peers last", err)
	}
}

func TestAvenorEventsKeepsTheRegistryErrorForAnUnknownRun(t *testing.T) {
	s := brokerOnlyServer(t)
	_, _, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: "rt_nobody"})
	if err == nil || err.Error() != "run not found in registry" {
		t.Fatalf("error = %v, want the registry not-found error", err)
	}
}
