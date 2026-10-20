package router

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
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
			if got, want := result.Peers[index].RunID, stringOf(record["run_id"]); got != want {
				t.Fatalf("peer %d run_id = %q, want %q", index, got, want)
			}
			if got, want := result.Peers[index].Label, stringOf(record["label"]); got != want {
				t.Fatalf("peer %d label = %q, want %q", index, got, want)
			}
			if got, want := result.Peers[index].Status, stringOf(record["status"]); got != want {
				t.Fatalf("peer %d status = %q, want %q", index, got, want)
			}
			if got, want := result.Peers[index].Kind, stringOf(record["kind"]); got != want {
				t.Fatalf("peer %d kind = %q, want %q", index, got, want)
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
		if got, want := result.Label, stringOf(last["label"]); got != want {
			t.Fatalf("label = %q, want %q", got, want)
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

func TestPeersInvalidRequestMatrix(t *testing.T) {
	inputs := map[string]map[string]string{
		"list": {
			"unknown property \"target\"":          `{"action": "list", "target": "x"}`,
			"missing required property \"action\"": `{}`,
			"property \"action\" must be a string": `{"action": 5}`,
		},
		"rename": {
			"unknown property \"target\"":              `{"action": "rename", "name": "x", "target": "y"}`,
			"missing required property \"name\"":       `{"action": "rename"}`,
			"property \"session_id\" must be a string": `{"action": "rename", "name": "x", "session_id": 5}`,
		},
		"last": {
			"unknown property \"name\"":            `{"action": "last", "target": "x", "name": "y"}`,
			"missing required property \"target\"": `{"action": "last"}`,
			"property \"target\" must be a string": `{"action": "last", "target": 5}`,
		},
	}
	for action, cases := range inputs {
		for detail, input := range cases {
			t.Run(action+"/"+detail, func(t *testing.T) {
				_, err := ExecutePeers(t.Context(), &fakeProvider{caller: messaging.Caller{Kind: messaging.CallerBroker, RunID: "run-caller"}}, json.RawMessage(input))
				var toolErr *ToolError
				if !errors.As(err, &toolErr) || toolErr.Code != "invalid_request" {
					t.Fatalf("error = %v, want invalid_request", err)
				}
				if got, want := toolErr.Facts["detail"], detail; got != want {
					t.Fatalf("detail = %q, want %q", got, want)
				}
			})
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
