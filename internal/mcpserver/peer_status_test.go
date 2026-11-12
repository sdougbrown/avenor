package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
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
	return serverOver(t, brokerListingClient(listErr, peers...))
}

func brokerListingClient(listErr error, peers ...messaging.Peer) *fakeMessageControlClient {
	return &fakeMessageControlClient{
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
}

func serverOver(t *testing.T, cl *fakeMessageControlClient) *Server {
	t.Helper()
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// twinAmbiguityText is the sentence a label shared by rt_a and rt_b answers
// with, spelled out so a change to its wording or quoting is deliberate.
const twinAmbiguityText = `"twin" matches 2 runs: "rt_a", "rt_b"`

// twinListingServer lists two peers that share one label.
func twinListingServer(t *testing.T) *Server {
	t.Helper()
	return brokerListingServer(t, nil,
		brokerPeerSession(t, "rt_a", "twin", "idle"),
		brokerPeerSession(t, "rt_b", "twin", "idle"))
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

// TestAvenorStatusKeepsTheRuntimeErrorWhenTheListingCannotNameAPeer pins that a
// listing that cannot be read names none, so the supervisor's own error stays.
func TestAvenorStatusKeepsTheRuntimeErrorWhenTheListingCannotNameAPeer(t *testing.T) {
	s := brokerListingServer(t, errors.New("listing unavailable"))
	_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: brokerOnlyRunID})
	want := fmt.Sprintf(`status: rpc error [-32000]: runtime %q not found`, brokerOnlyRunID)
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// TestAvenorStatusKeepsTheRuntimeErrorForAPartialListing pins that a listing
// that answered some peers but failed on the rest cannot prove a label is
// shared, so the supervisor's own error stays.
func TestAvenorStatusKeepsTheRuntimeErrorForAPartialListing(t *testing.T) {
	cl := brokerListingClient(nil)
	twins := []messaging.Peer{brokerPeerSession(t, "rt_a", "twin", "idle"), brokerPeerSession(t, "rt_b", "twin", "idle")}
	cl.peersFunc = func(context.Context, messaging.PeersRequest) (messaging.PeersResult, error) {
		return messaging.PeersResult{Peers: twins}, errors.New("a child broker did not answer")
	}
	s := serverOver(t, cl)
	_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "twin"})
	want := `status: rpc error [-32000]: runtime "twin" not found`
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestAvenorStatusNamesTheCandidatesOfALabelSeveralPeersShare(t *testing.T) {
	s := twinListingServer(t)
	_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "twin"})
	if err == nil || err.Error() != twinAmbiguityText {
		t.Fatalf("error = %v, want %q", err, twinAmbiguityText)
	}
}

// TestAvenorStatusAndPeersLastAnswerASharedLabelAlike pins that the supervisor
// tools and the messaging surface say the same thing about a label that names
// several peers, with the same typed code.
func TestAvenorStatusAndPeersLastAnswerASharedLabelAlike(t *testing.T) {
	s := twinListingServer(t)
	_, _, statusErr := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "twin"})
	if statusErr == nil {
		t.Fatal("status of a shared label succeeded")
	}
	res, err := s.handleAvenorPeers(context.Background(), toolRequest(t, map[string]any{"action": "last", "target": "twin"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || len(res.Content) == 0 {
		t.Fatalf("peers last of a shared label = %+v, want an error result", res)
	}
	content, _ := res.Content[0].(*mcpsdk.TextContent)
	if content == nil || content.Text != statusErr.Error() {
		t.Fatalf("peers last text = %+v, want the status error %q", res.Content[0], statusErr)
	}
	var coded interface{ ToolCode() string }
	if !errors.As(statusErr, &coded) || coded.ToolCode() != "ambiguous_name" {
		t.Fatalf("status error = %v, want the ambiguous_name denial", statusErr)
	}
}

func TestAvenorStatusReportsAmbiguityBeforeRefusingToWait(t *testing.T) {
	s := twinListingServer(t)
	_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "twin", WaitFor: "terminal"})
	if errors.Is(err, errBrokerPeerNoWait) {
		t.Fatalf("error = %v, want the ambiguity, not the wait refusal", err)
	}
	if err == nil || err.Error() != twinAmbiguityText {
		t.Fatalf("error = %v, want %q", err, twinAmbiguityText)
	}
}

// TestAvenorStatusNeverConsultsTheBrokerForASupervisorFailure pins that only the
// supervisor's absence answer sends status to the broker: a failure to reach or
// ask the supervisor reaches the caller as it is.
func TestAvenorStatusNeverConsultsTheBrokerForASupervisorFailure(t *testing.T) {
	cl := brokerListingClient(nil, brokerPeerSession(t, "rt_a", "twin", "idle"), brokerPeerSession(t, "rt_b", "twin", "idle"))
	transport := errors.New("supervisor unreachable")
	cl.fakeClient.statusFunc = func(string) (map[string]any, error) { return nil, transport }
	listed := 0
	list := cl.peersFunc
	cl.peersFunc = func(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
		listed++
		return list(ctx, req)
	}
	s := serverOver(t, cl)
	_, _, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "twin"})
	if !errors.Is(err, transport) {
		t.Fatalf("error = %v, want the supervisor failure", err)
	}
	if listed != 0 {
		t.Fatalf("peers listed %d times, want 0", listed)
	}
}

func TestAvenorEventsRefusesABrokerPeerAndPointsAtPeersLast(t *testing.T) {
	s := brokerOnlyServer(t)
	_, _, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: brokerOnlyRunID})
	if !errors.Is(err, errBrokerPeerNoEvents) || !strings.Contains(err.Error(), "last") {
		t.Fatalf("error = %v, want the broker-peer refusal naming peers last", err)
	}
}

func TestAvenorEventsNamesTheCandidatesOfALabelSeveralPeersShare(t *testing.T) {
	s := twinListingServer(t)
	_, _, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: "twin"})
	if err == nil || err.Error() != twinAmbiguityText {
		t.Fatalf("error = %v, want %q", err, twinAmbiguityText)
	}
}

func TestAvenorEventsKeepsTheRegistryErrorWhenTheListingCannotNameAPeer(t *testing.T) {
	s := brokerListingServer(t, errors.New("listing unavailable"))
	_, _, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: brokerOnlyRunID})
	if err == nil || err.Error() != "run not found in registry" {
		t.Fatalf("error = %v, want the registry not-found error", err)
	}
}

func TestAvenorEventsKeepsTheRegistryErrorForAnUnknownRun(t *testing.T) {
	s := brokerOnlyServer(t)
	_, _, err := s.handleAvenorEvents(context.Background(), nil, eventsArgs{RunID: "rt_nobody"})
	if err == nil || err.Error() != "run not found in registry" {
		t.Fatalf("error = %v, want the registry not-found error", err)
	}
}
