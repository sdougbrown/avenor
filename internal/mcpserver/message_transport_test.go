package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/messaging"
)

func TestControlTransportMapsWireErrors(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantTyped     error
		wantUnchanged bool
	}{
		{
			name:          "no data comes back unchanged",
			err:           &client.RPCError{Code: -32000, Message: "authority refused"},
			wantUnchanged: true,
		},
		{
			name:          "unparsable data comes back unchanged",
			err:           &client.RPCError{Code: -32000, Message: "bad envelope", Data: json.RawMessage(`{"wire_code":`)},
			wantUnchanged: true,
		},
		{
			name:          "empty wire code comes back unchanged",
			err:           &client.RPCError{Code: -32000, Message: "authority refused", Data: json.RawMessage(`{"code":"invalid_request"}`)},
			wantUnchanged: true,
		},
		{
			name:      "known wire code becomes the typed error",
			err:       &client.RPCError{Code: -32000, Message: "stale", Data: json.RawMessage(`{"wire_code":"generation-mismatch"}`)},
			wantTyped: messaging.ErrGenerationMismatch,
		},
		{
			name:          "unknown wire code comes back unchanged",
			err:           &client.RPCError{Code: -32000, Message: "unknown", Data: json.RawMessage(`{"wire_code":"no-such-class"}`)},
			wantUnchanged: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cl := &fakeMessageControlClient{
				fakeClient: &fakeClient{},
				waitFunc: func(context.Context, messaging.WaitRequest) (messaging.WaitResult, error) {
					return messaging.WaitResult{}, tc.err
				},
			}
			_, got := newControlTransport(cl).Wait(context.Background(), messaging.WaitRequest{})
			switch {
			case tc.wantTyped != nil:
				if !errors.Is(got, tc.wantTyped) {
					t.Fatalf("transport error = %v, want %v", got, tc.wantTyped)
				}
				if errors.Is(got, tc.err) {
					t.Fatalf("transport error = %v, want the typed error instead of the original", got)
				}
			case tc.wantUnchanged:
				if got != tc.err {
					t.Fatalf("transport error = %v, want the original error unchanged", got)
				}
			}
		})
	}
}
