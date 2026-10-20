package router

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

func TestPeersConformanceCases(t *testing.T) {
	cases, err := messaging.LoadPeersConformanceCases(conformancePath)
	if err != nil {
		t.Fatalf("load peers conformance cases: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("peers conformance fixture has no cases")
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			provider := newFake(t, c)
			result, err := ExecutePeers(t.Context(), provider, c.Input)

			if c.Expect.Code != "" {
				var toolErr *ToolError
				if !errors.As(err, &toolErr) {
					t.Fatalf("error = %v, want tool error %q", err, c.Expect.Code)
				}
				if toolErr.Code != c.Expect.Code || toolErr.Message != c.Expect.Message {
					t.Fatalf("error = %s: %q, want %s: %q", toolErr.Code, toolErr.Message, c.Expect.Code, c.Expect.Message)
				}
				if c.Expect.Code == "invalid_request" || c.Expect.Code == "invalid_action" {
					if provider.peered.Generation != "" || provider.renameReq.Name != "" || provider.resolveRef != "" {
						t.Fatal("a rejected request reached the provider")
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if got, want := result.Outcome, messaging.Outcome(c.Expect.Outcome); got != want {
				t.Fatalf("outcome = %q, want %q", got, want)
			}
			if result.Text != c.Expect.Message {
				t.Fatalf("text = %q, want %q", result.Text, c.Expect.Message)
			}
			assertPeersResult(t, c, provider, result)
		})
	}
}

func assertPeersResult(t *testing.T, c messaging.ConformanceCase, provider *fakeProvider, result Result) {
	t.Helper()
	if ref := stringOf(c.Provider["resolve_ref"]); ref != "" && provider.resolveRef != ref {
		t.Fatalf("resolved %q, want %q", provider.resolveRef, ref)
	}
	if peers, ok := c.Provider["peers"].([]any); ok {
		if len(result.Peers) != len(peers) {
			t.Fatalf("peers = %d records, want %d", len(result.Peers), len(peers))
		}
		for index, raw := range peers {
			record := objectOf(raw)
			if got, want := result.Peers[index].RunID, firstNonEmpty(stringOf(record["peer_run_id"]), stringOf(record["run_id"])); got != want {
				t.Fatalf("peer %d run_id = %q, want %q", index, got, want)
			}
			if got, want := result.Peers[index].Label, stringOf(record["label"]); got != want {
				t.Fatalf("peer %d label = %q, want %q", index, got, want)
			}
			if got, want := result.Peers[index].Status, stringOf(record["status"]); got != want {
				t.Fatalf("peer %d status = %q, want %q", index, got, want)
			}
			if got, want := result.Peers[index].Backend, stringOf(record["backend"]); got != want {
				t.Fatalf("peer %d backend = %q, want %q", index, got, want)
			}
			if got, want := result.Peers[index].Model, stringOf(record["model"]); got != want {
				t.Fatalf("peer %d model = %q, want %q", index, got, want)
			}
			if got, want := result.Peers[index].Dir, stringOf(record["dir"]); got != want {
				t.Fatalf("peer %d dir = %q, want %q", index, got, want)
			}
		}
		if provider.peered.Generation != "gen-7" {
			t.Fatalf("peers generation = %q, want gen-7", provider.peered.Generation)
		}
	}
	if renamed := objectOf(c.Provider["rename"]); renamed != nil {
		if result.RunID != stringOf(renamed["run_id"]) || result.Label != stringOf(renamed["label"]) ||
			result.PreviousLabel != stringOf(renamed["previous_label"]) {
			t.Fatalf("rename result = %+v, want %v", result, renamed)
		}
	}
	if request := objectOf(c.Provider["request"]); request != nil {
		if want, ok := request["session_id"]; ok && provider.renameReq.SessionID != stringOf(want) {
			t.Fatalf("session_id = %q, want %q", provider.renameReq.SessionID, stringOf(want))
		}
		if want, ok := request["name"]; ok && provider.renameReq.Name != stringOf(want) {
			t.Fatalf("name = %q, want %q", provider.renameReq.Name, stringOf(want))
		}
		if want, ok := request["run_id"]; ok && provider.lastRunID != stringOf(want) {
			t.Fatalf("last run_id = %q, want %q", provider.lastRunID, stringOf(want))
		}
	}
	if last := objectOf(c.Provider["last"]); last != nil {
		wantLabel := firstNonEmpty(stringOf(last["label"]), provider.resolution.Label, stringOf(inputTarget(c.Input)))
		if got := result.Label; got != wantLabel {
			t.Fatalf("label = %q, want %q", got, wantLabel)
		}
		if got, want := result.FinalOutput, stringOf(last["final_output"]); got != want {
			t.Fatalf("final_output = %q, want %q", got, want)
		}
		if got, want := result.StopReason, stringOf(last["stop_reason"]); got != want {
			t.Fatalf("stop_reason = %q, want %q", got, want)
		}
		if got, want := result.Truncated, boolOf(last["truncated"]); got != want {
			t.Fatalf("truncated = %v, want %v", got, want)
		}
		if got, want := result.RuntimeID, provider.resolution.RunID; got != want {
			t.Fatalf("runtime_id = %q, want %q", got, want)
		}
		if got, want := result.RunID, firstNonEmpty(provider.resolution.PublicRunID, provider.resolution.RunID); got != want {
			t.Fatalf("run_id = %q, want %q", got, want)
		}
		if provider.lastRunID != provider.resolution.RunID {
			t.Fatalf("last read %q, want the resolved %q", provider.lastRunID, provider.resolution.RunID)
		}
	}
}

// TestPeersActionVocabulary pins the peers action union, which is declared three
// times: the schema's action enum, the peers applicability table's keys, and the
// executor's vocabulary. All three must be list, rename, last in that order, and
// no message action may appear in any of them.
func TestPeersActionVocabulary(t *testing.T) {
	schema := messaging.PeersInputSchema()
	variants, _ := schema["oneOf"].([]any)
	var fromSchema []string
	for _, raw := range variants {
		variant := objectOf(raw)
		properties := objectOf(variant["properties"])
		action := objectOf(properties["action"])
		fromSchema = append(fromSchema, stringOf(action["const"]))
	}
	if !reflect.DeepEqual(fromSchema, peersActionVocabulary) {
		t.Fatalf("schema action enum = %v, want %v", fromSchema, peersActionVocabulary)
	}
	table := messaging.PeersApplicabilityTable()
	fromTable := make([]string, 0, len(table))
	for action := range table {
		fromTable = append(fromTable, action)
	}
	sort.Strings(fromTable)
	sortedVocabulary := append([]string(nil), peersActionVocabulary...)
	sort.Strings(sortedVocabulary)
	if !reflect.DeepEqual(fromTable, sortedVocabulary) {
		t.Fatalf("peers applicability keys = %v, want %v", fromTable, sortedVocabulary)
	}

	// Every vocabulary action is exercised by at least one fixture case, so a
	// branch the schema publishes cannot go untested.
	cases, err := messaging.LoadPeersConformanceCases(conformancePath)
	if err != nil {
		t.Fatalf("load peers conformance cases: %v", err)
	}
	exercised := map[string]bool{}
	for _, c := range cases {
		var input struct {
			Action string `json:"action"`
		}
		if err := json.Unmarshal(c.Input, &input); err != nil {
			continue
		}
		exercised[input.Action] = true
	}
	for _, action := range peersActionVocabulary {
		if !exercised[action] {
			t.Fatalf("peers action %q has no conformance case", action)
		}
	}

	// The message tool's action union never lists a peers action.
	for _, action := range peersActionVocabulary {
		if _, ok := messaging.ApplicabilityTable()[action]; ok {
			t.Fatalf("message applicability table lists the peers action %q", action)
		}
	}
}

// inputTarget returns the case's addressed target, the last rung of the peers
// `last` label fallback chain.
func inputTarget(input json.RawMessage) string {
	var decoded struct {
		Target string `json:"target"`
	}
	if err := json.Unmarshal(input, &decoded); err != nil {
		return ""
	}
	return decoded.Target
}

// TestPeersLastEscapesStopReason keeps a producer-controlled stop reason from
// injecting bidi or zero-width characters into the peers `last` text.
func TestPeersLastEscapesStopReason(t *testing.T) {
	provider := &fakeProvider{
		caller:     messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"},
		resolution: messaging.Resolution{RunID: "rt_7", Label: "mesh-a", Match: messaging.MatchUnique},
		last: messaging.PeerLast{
			RunID:      "rt_7",
			Label:      "mesh-a",
			HasRecord:  true,
			Text:       "OK",
			StopReason: "end\u202eturn",
		},
	}
	result, err := ExecutePeers(t.Context(), provider, json.RawMessage(`{"action": "last", "target": "rt_7"}`))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if strings.Contains(result.Text, "\u202e") {
		t.Fatalf("text carries a raw bidi override: %q", result.Text)
	}
	if !strings.Contains(result.Text, `\u202e`) {
		t.Fatalf("text does not carry the escaped stop reason: %q", result.Text)
	}
}
