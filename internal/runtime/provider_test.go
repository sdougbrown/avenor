package runtime

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// fakeTransport satisfies messaging.Transport for option plumbing tests that
// only compare the identity of the value they pass around.
type fakeTransport struct{ messaging.Transport }

func TestValidatePermissionMessageEmpty(t *testing.T) {
	if err := ValidatePermissionMessage(""); err != nil {
		t.Errorf("empty message should be valid, got %v", err)
	}
}

func TestValidatePermissionMessageNormal(t *testing.T) {
	if err := ValidatePermissionMessage("approve with note: looks safe"); err != nil {
		t.Errorf("normal message should be valid, got %v", err)
	}
}

func TestValidatePermissionMessageUTF8(t *testing.T) {
	if err := ValidatePermissionMessage("café — naïve"); err != nil {
		t.Errorf("UTF-8 message should be valid, got %v", err)
	}
}

func TestValidatePermissionMessageOversized(t *testing.T) {
	long := strings.Repeat("x", MaxPermissionMessageBytes+1)
	err := ValidatePermissionMessage(long)
	if err == nil {
		t.Fatal("oversized message should be rejected")
	}
}

func TestValidatePermissionMessageAtBound(t *testing.T) {
	exact := strings.Repeat("x", MaxPermissionMessageBytes)
	if err := ValidatePermissionMessage(exact); err != nil {
		t.Errorf("message at exact bound should be valid, got %v", err)
	}
}

func TestValidatePermissionMessageInvalidUTF8(t *testing.T) {
	invalid := string([]byte{0xff, 0xfe, 0xfd})
	err := ValidatePermissionMessage(invalid)
	if err == nil {
		t.Fatal("invalid UTF-8 should be rejected")
	}
}

func TestValidatePermissionMessagePreservesText(t *testing.T) {
	// The validator must not trim or normalize — just validate.
	msg := "  leading and trailing spaces  \n\t"
	if err := ValidatePermissionMessage(msg); err != nil {
		t.Errorf("message with whitespace should be valid, got %v", err)
	}
	// Verify the constant is 64 KiB
	if MaxPermissionMessageBytes != 64<<10 {
		t.Errorf("MaxPermissionMessageBytes = %d, want %d", MaxPermissionMessageBytes, 64<<10)
	}
}

func TestDecodePermissionMessageJSONRejectsMalformedText(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte{'"', 0xff, '"'},
		[]byte(`"\uD800"`),
		[]byte(`"\uDC00"`),
		[]byte(`"\uD800x"`),
	} {
		if _, err := DecodePermissionMessageJSON(raw); err == nil {
			t.Fatalf("malformed JSON message accepted: %q", raw)
		}
	}
	message, err := DecodePermissionMessageJSON([]byte(`"\uD83D\uDE00"`))
	if err != nil || message != "😀" {
		t.Fatalf("surrogate pair = %q, %v", message, err)
	}
	message, err = DecodePermissionMessageJSON([]byte(`"\\uD800"`))
	if err != nil || message != `\uD800` {
		t.Fatalf("escaped literal = %q, %v", message, err)
	}
}

func TestMergeStartOptionsBrokerFields(t *testing.T) {
	b := broker.New("merge-test-token")
	transport := &fakeTransport{}
	base := StartOptions{RuntimeID: "rt_base", BrokerToken: "tok_base", Messaging: transport}

	// Empty overrides keep the base broker identity.
	merged := MergeStartOptions(base, StartOptions{})
	if merged.RuntimeID != "rt_base" || merged.BrokerToken != "tok_base" || merged.Messaging != transport {
		t.Fatalf("base broker fields lost: %+v", merged)
	}

	// A start override carrying broker identity must survive the merge;
	// dropping BrokerToken silently disables sub-agent broker mode.
	override := StartOptions{RuntimeID: "rt_child", Broker: b, BrokerToken: "tok_child"}
	merged = MergeStartOptions(StartOptions{}, override)
	if merged.RuntimeID != "rt_child" || merged.Broker != b || merged.BrokerToken != "tok_child" {
		t.Fatalf("override broker fields lost: %+v", merged)
	}

	// A provider-scoped transport is inherited by every start, and an explicit
	// start transport replaces it.
	if got := MergeStartOptions(base, override).Messaging; got != transport {
		t.Fatalf("inherited transport = %v, want the provider-scoped one", got)
	}
	other := &fakeTransport{}
	if got := MergeStartOptions(base, StartOptions{Messaging: other}).Messaging; got != other {
		t.Fatalf("override transport = %v, want the start-scoped one", got)
	}
}

func TestMergeStartOptionsProvenance(t *testing.T) {
	iteration := func(v int) *int { return &v }
	tests := []struct {
		name     string
		override StartOptions
		// mutate runs after the merge on the caller's value, to prove the
		// merged identity is a snapshot.
		mutate        func(*LaunchProvenance)
		want          LaunchProvenance
		wantIteration *int
	}{
		{
			name:          "nil override keeps the base identity",
			override:      StartOptions{},
			want:          LaunchProvenance{RuntimeID: "rt_old", ExecutionID: "run_old", Kind: "loop", Phase: "body", Label: "old-label", Attempt: 9},
			wantIteration: iteration(4),
		},
		{
			name:     "non-nil override replaces the whole identity",
			override: StartOptions{Provenance: &LaunchProvenance{RuntimeID: "rt_new", ExecutionID: "run_new", Kind: "run", Attempt: 1}},
			want:     LaunchProvenance{RuntimeID: "rt_new", ExecutionID: "run_new", Kind: "run", Attempt: 1},
		},
		{
			name:     "empty override clears every field",
			override: StartOptions{Provenance: &LaunchProvenance{}},
			want:     LaunchProvenance{},
		},
		{
			name:     "merged identity is a snapshot",
			override: StartOptions{Provenance: &LaunchProvenance{RuntimeID: "rt_new", Kind: "run", Iteration: iteration(1), Attempt: 1}},
			mutate: func(p *LaunchProvenance) {
				p.RuntimeID, p.Kind, p.Attempt, *p.Iteration = "rt_mutated", "mutated", 7, 9
			},
			want:          LaunchProvenance{RuntimeID: "rt_new", Kind: "run", Attempt: 1},
			wantIteration: iteration(1),
		},
		{
			name: "broker identity merges independently of provenance",
			override: StartOptions{
				RuntimeID:   "rt_broker",
				BrokerToken: "tok_broker",
				Provenance:  &LaunchProvenance{RuntimeID: "rt_informational", ExecutionID: "run_informational"},
			},
			want: LaunchProvenance{RuntimeID: "rt_informational", ExecutionID: "run_informational"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := MergeStartOptions(StartOptions{Provenance: &LaunchProvenance{
				RuntimeID: "rt_old", ExecutionID: "run_old", Kind: "loop", Phase: "body",
				Iteration: iteration(4), Label: "old-label", Attempt: 9,
			}}, tt.override)
			if tt.mutate != nil {
				tt.mutate(tt.override.Provenance)
			}
			if merged.Provenance == nil {
				t.Fatal("merge dropped the identity")
			}
			if (merged.Provenance.Iteration == nil) != (tt.wantIteration == nil) {
				t.Fatalf("iteration = %v, want %v", merged.Provenance.Iteration, tt.wantIteration)
			}
			got := *merged.Provenance
			got.Iteration = nil
			if tt.wantIteration != nil && *merged.Provenance.Iteration != *tt.wantIteration {
				t.Fatalf("iteration = %d, want %d", *merged.Provenance.Iteration, *tt.wantIteration)
			}
			if got != tt.want {
				t.Fatalf("provenance = %+v, want %+v", got, tt.want)
			}
			if merged.RuntimeID != tt.override.RuntimeID || merged.BrokerToken != tt.override.BrokerToken {
				t.Fatalf("broker identity = (%q, %q), want (%q, %q)",
					merged.RuntimeID, merged.BrokerToken, tt.override.RuntimeID, tt.override.BrokerToken)
			}
		})
	}
}
