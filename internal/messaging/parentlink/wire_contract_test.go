package parentlink

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"slices"
	"sort"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// parentWireDoc is the parent protocol recorded in testdata/parent_wire.json.
// The link and the relay are checked against it, so a path, a request field or
// a response shape cannot drift silently.
type parentWireDoc struct {
	Protocol      int                  `json:"protocol"`
	Paths         map[string]string    `json:"paths"`
	RequestFields map[string][]string  `json:"request_fields"`
	Responses     []parentWireResponse `json:"responses"`
	FenceFields   []string             `json:"fence_fields"`
	Codes         []string             `json:"codes"`
}

// parentWireResponse is one answer the parent may give.
type parentWireResponse struct {
	Name    string         `json:"name"`
	Fields  []string       `json:"fields"`
	Body    map[string]any `json:"body"`
	Outcome string         `json:"outcome"`
	Code    string         `json:"code"`
}

// loadParentWireDoc reads the recorded parent protocol.
func loadParentWireDoc(t *testing.T) parentWireDoc {
	t.Helper()
	raw, err := os.ReadFile("testdata/parent_wire.json")
	if err != nil {
		t.Fatalf("read the parent wire document: %v", err)
	}
	var doc parentWireDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode the parent wire document: %v", err)
	}
	return doc
}

// TestParentWireDocumentPinsThePathsAndTheCodes pins the recorded protocol's
// own half: the paths the link drives and the wire codes the parent may
// answer with.
func TestParentWireDocumentPinsThePathsAndTheCodes(t *testing.T) {
	doc := loadParentWireDoc(t)
	if doc.Protocol != messaging.ProtocolVersion {
		t.Fatalf("documented protocol = %d, want %d", doc.Protocol, messaging.ProtocolVersion)
	}
	paths := map[string]string{
		"register":   pathRegister,
		"deregister": pathDeregister,
		"relay":      pathRelay,
		"send":       pathSend,
		"wait":       pathWait,
		"withdraw":   pathWithdraw,
		"sessions":   pathSessions,
	}
	for name, path := range paths {
		if documented, ok := doc.Paths[name]; !ok {
			t.Fatalf("the parent protocol does not document the %q path", name)
		} else if documented != path {
			t.Fatalf("the %q path = %q, want %q", name, path, documented)
		}
	}
	known := make(map[string]bool)
	for _, class := range messaging.WireErrors() {
		known[class.Code] = true
	}
	for _, code := range doc.Codes {
		if !known[code] {
			t.Fatalf("the documented code %q is not in the shared error table", code)
		}
	}
}

// TestParentWireDocumentPinsTheRequestShapes pins the requests the link and
// the relay put on the wire: every operation carries exactly the fields the
// recorded protocol names, and every operation the protocol names was sent.
func TestParentWireDocumentPinsTheRequestShapes(t *testing.T) {
	doc := loadParentWireDoc(t)
	requests := driveParentRequests(t)
	recorded := make([]string, 0, len(requests))
	for operation := range requests {
		recorded = append(recorded, operation)
	}
	sort.Strings(recorded)
	documented := make([]string, 0, len(doc.RequestFields))
	for operation := range doc.RequestFields {
		documented = append(documented, operation)
	}
	sort.Strings(documented)
	if !slices.Equal(recorded, documented) {
		t.Fatalf("the operations sent = %v, want the documented %v", recorded, documented)
	}
	for operation, documented := range doc.RequestFields {
		body, ok := requests[operation]
		if !ok {
			t.Fatalf("the %q request was never sent", operation)
		}
		got := make([]string, 0, len(body))
		for field := range body {
			got = append(got, field)
		}
		sort.Strings(got)
		want := append([]string(nil), documented...)
		sort.Strings(want)
		if len(got) != len(want) {
			t.Fatalf("the %q request carries %v, want %v", operation, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("the %q request carries %v, want %v", operation, got, want)
			}
		}
	}
}

// TestParentWireDocumentPinsTheResponseShapes pins the answers the parent may
// give: every recorded answer carries exactly the fields the protocol names,
// and the link's envelope reads them.
func TestParentWireDocumentPinsTheResponseShapes(t *testing.T) {
	doc := loadParentWireDoc(t)
	for _, response := range doc.Responses {
		t.Run(response.Name, func(t *testing.T) {
			raw, err := json.Marshal(response.Body)
			if err != nil {
				t.Fatalf("marshal the recorded answer: %v", err)
			}
			got := make([]string, 0, len(response.Body))
			for field := range response.Body {
				got = append(got, field)
			}
			sort.Strings(got)
			want := append([]string(nil), response.Fields...)
			sort.Strings(want)
			if len(got) != len(want) {
				t.Fatalf("the answer carries %v, want %v", got, want)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("the answer carries %v, want %v", got, want)
				}
			}

			body, err := decode(http.StatusOK, raw)
			if response.Code != "" {
				typed := messaging.WireErrorFor(response.Code)
				if typed == nil {
					t.Fatalf("the documented code %q is not in the shared error table", response.Code)
				}
				if !errors.Is(err, typed) {
					t.Fatalf("decode the answer = %v, want %v", err, typed)
				}
			} else if err != nil {
				t.Fatalf("decode the answer: %v", err)
			}
			if response.Outcome != "" && string(body.Outcome) != response.Outcome {
				t.Fatalf("the decoded outcome = %q, want %q", body.Outcome, response.Outcome)
			}
			if body.Fence != nil {
				fence := body.Fence
				carried := map[string]bool{
					"sender":     fence.Sender != "",
					"message_id": fence.MessageID != "",
					"generation": fence.Generation != "",
					"deadline":   !fence.Deadline.IsZero(),
					"terminal":   fence.Terminal,
				}
				for _, field := range doc.FenceFields {
					present, known := carried[field]
					if !known {
						t.Fatalf("the documented fence field %q is not one the fence carries", field)
					}
					if !present {
						t.Fatalf("the decoded fence does not carry %q", field)
					}
				}
			}
		})
	}
}

// driveParentRequests sends one request of every operation the recorded
// protocol names and reports the body each one carried.
func driveParentRequests(t *testing.T) map[string]map[string]any {
	t.Helper()
	parent := newStubBroker(t, func(req stubRequest) (int, string) {
		switch req.Path {
		case pathRegister:
			return http.StatusOK, `{"generation":"gen-a"}`
		case pathSend:
			return http.StatusOK, `{"outcome":"sent"}`
		case pathWait:
			return http.StatusOK, `{"outcome":"timed-out"}`
		case pathWithdraw:
			return http.StatusOK, `{"outcome":"withdrawn"}`
		}
		return http.StatusOK, `{}`
	})
	child := newStubBroker(t, func(req stubRequest) (int, string) {
		return http.StatusOK, `{"outcome":"sent"}`
	})
	ctx := testContext(t)
	link := newTestLink(t, parent, "gen-a")

	if err := link.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := link.Send(ctx, askRequest("msg-1")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := link.Wait(ctx, waitRequest("msg-1")); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if _, err := link.Withdraw(ctx, withdrawRequest("msg-1")); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if err := link.RecordRelay(messaging.RelayRoute{
		MessageID:      "msg-1",
		AskerRunID:     "run-x",
		EdgeGeneration: "gen-a",
	}); err != nil {
		t.Fatalf("RecordRelay: %v", err)
	}
	_, _ = link.Reply(ctx, messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "run-a", Sender: "alice"},
		ToRunID:   "run-x",
		MessageID: "msg-1",
	})

	registry := NewRegistry()
	relay := newTestRelay(t, registry, "child-one", child.URL(), "gen-a", "run-b")
	relayAsk := askRequest("msg-1")
	relayAsk.To = messaging.Identity{RunID: ParentRunKey("child-one", "run-b"), Sender: "bob"}
	if err := relay.Ask(ctx, relayAsk); err != nil {
		t.Fatalf("relay Ask: %v", err)
	}

	if err := link.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	requests := map[string]map[string]any{}
	for _, req := range parent.recorded() {
		switch {
		case req.Path == pathRegister:
			requests["register"] = req.Body
		case req.Path == pathDeregister:
			requests["deregister"] = req.Body
		case req.Path == pathWait:
			requests["wait"] = req.Body
		case req.Path == pathWithdraw:
			requests["withdraw"] = req.Body
		case req.Path == pathSend && req.Body["reply_to"] != nil:
			requests["reply"] = req.Body
		case req.Path == pathSend:
			requests["send"] = req.Body
		}
	}
	for _, req := range child.recorded() {
		if req.Path == pathRelay {
			requests["relay"] = req.Body
		}
	}
	return requests
}
