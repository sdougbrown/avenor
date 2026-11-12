package mcpserver

import (
	"encoding/json"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
)

// TestStatusReportsNoRuntime pins how the hub reads the supervisor's absence
// answer. The code decides when it is present, because the sentence is free to
// change; the sentence still decides against a supervisor that sends no code.
func TestStatusReportsNoRuntime(t *testing.T) {
	coded := func(code, runtimeID string) error {
		data, err := json.Marshal(map[string]string{"code": code, "runtime_id": runtimeID})
		if err != nil {
			t.Fatalf("marshal data: %v", err)
		}
		return &client.RPCError{Code: -32000, Message: "rewritten absence answer", Data: data}
	}

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "code for this runtime wins over the sentence",
			err:  coded(control.CodeRuntimeNotFound, "rt_1"),
			want: true,
		},
		{
			name: "code for another runtime is not this runtime's absence",
			err:  coded(control.CodeRuntimeNotFound, "rt_2"),
			want: false,
		},
		{
			name: "code with no runtime id can only mean this one",
			err: func() error {
				data, err := json.Marshal(map[string]string{"code": control.CodeRuntimeNotFound})
				if err != nil {
					t.Fatalf("marshal data: %v", err)
				}
				return &client.RPCError{Code: -32000, Message: "rewritten absence answer", Data: data}
			}(),
			want: true,
		},
		{
			name: "another code keeps the caller's error",
			err:  coded("generation-mismatch", "rt_1"),
			want: false,
		},
		{
			name: "sentence alone, from a supervisor that sends no code",
			err:  &client.RPCError{Code: -32000, Message: `runtime "rt_1" not found`},
			want: true,
		},
		{
			name: "rewritten sentence with no code stays the caller's error",
			err:  &client.RPCError{Code: -32000, Message: "no such runtime"},
			want: false,
		},
		{
			name: "another rpc code is a refusal, not an absence",
			err:  &client.RPCError{Code: -32601, Message: `runtime "rt_1" not found`},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := statusReportsNoRuntime(tc.err, "rt_1"); got != tc.want {
				t.Fatalf("statusReportsNoRuntime = %v, want %v", got, tc.want)
			}
		})
	}
}
