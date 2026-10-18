package runtime

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

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
	base := StartOptions{RuntimeID: "rt_base", BrokerToken: "tok_base"}

	// Empty overrides keep the base broker identity.
	merged := MergeStartOptions(base, StartOptions{})
	if merged.RuntimeID != "rt_base" || merged.BrokerToken != "tok_base" {
		t.Fatalf("base broker fields lost: %+v", merged)
	}

	// A start override carrying broker identity must survive the merge;
	// dropping BrokerToken silently disables sub-agent broker mode.
	override := StartOptions{RuntimeID: "rt_child", Broker: b, BrokerToken: "tok_child"}
	merged = MergeStartOptions(StartOptions{}, override)
	if merged.RuntimeID != "rt_child" || merged.Broker != b || merged.BrokerToken != "tok_child" {
		t.Fatalf("override broker fields lost: %+v", merged)
	}
}

// sameProvenance compares identity values, not the identity's pointer fields.
func sameProvenance(a, b LaunchProvenance) bool {
	if (a.Iteration == nil) != (b.Iteration == nil) {
		return false
	}
	if a.Iteration != nil && *a.Iteration != *b.Iteration {
		return false
	}
	a.Iteration, b.Iteration = nil, nil
	return a == b
}

func TestMergeStartOptionsProvenanceKeepsDefaultsWhenNil(t *testing.T) {
	iteration := 2
	base := StartOptions{Provenance: &LaunchProvenance{
		RuntimeID:   "rt_member",
		ExecutionID: "run_team",
		Kind:        "team",
		Phase:       "reviewer",
		Iteration:   &iteration,
		Label:       "verify",
		Attempt:     1,
	}}

	merged := MergeStartOptions(base, StartOptions{})
	if merged.Provenance == nil {
		t.Fatal("nil override dropped the base provenance")
	}
	if !sameProvenance(*merged.Provenance, *base.Provenance) {
		t.Fatalf("merged provenance = %+v, want %+v", *merged.Provenance, *base.Provenance)
	}
	if merged.Provenance.Iteration == nil || *merged.Provenance.Iteration != 2 {
		t.Fatalf("merged iteration = %v, want 2", merged.Provenance.Iteration)
	}
}

func TestMergeStartOptionsProvenanceReplacesWholeIdentity(t *testing.T) {
	iteration := 4
	base := StartOptions{Provenance: &LaunchProvenance{
		RuntimeID:   "rt_old",
		ExecutionID: "run_old",
		Kind:        "loop",
		Phase:       "body",
		Iteration:   &iteration,
		Label:       "old-label",
		Attempt:     9,
	}}
	override := StartOptions{Provenance: &LaunchProvenance{
		RuntimeID:   "rt_new",
		ExecutionID: "run_new",
		Kind:        "run",
		Attempt:     1,
	}}

	merged := MergeStartOptions(base, override)
	want := LaunchProvenance{RuntimeID: "rt_new", ExecutionID: "run_new", Kind: "run", Attempt: 1}
	if merged.Provenance == nil {
		t.Fatal("explicit override lost the provenance")
	}
	if !sameProvenance(*merged.Provenance, want) {
		t.Fatalf("merged provenance = %+v, want %+v (stale fields must be cleared)", *merged.Provenance, want)
	}
	if merged.Provenance.Iteration != nil {
		t.Fatalf("stale iteration survived the merge: %v", *merged.Provenance.Iteration)
	}
	if merged.Provenance.Phase != "" || merged.Provenance.Label != "" {
		t.Fatalf("stale phase/label survived the merge: %+v", *merged.Provenance)
	}
}

func TestMergeStartOptionsProvenanceEmptyOverrideClearsDefaults(t *testing.T) {
	base := StartOptions{Provenance: &LaunchProvenance{RuntimeID: "rt_old", Kind: "team", Phase: "member", Attempt: 2}}

	merged := MergeStartOptions(base, StartOptions{Provenance: &LaunchProvenance{}})
	if merged.Provenance == nil {
		t.Fatal("explicit empty provenance was treated as absent")
	}
	if !sameProvenance(*merged.Provenance, LaunchProvenance{}) {
		t.Fatalf("merged provenance = %+v, want the empty identity", *merged.Provenance)
	}
}

func TestMergeStartOptionsProvenanceSnapshotsOverride(t *testing.T) {
	iteration := 1
	override := StartOptions{Provenance: &LaunchProvenance{
		RuntimeID:   "rt_a",
		ExecutionID: "run_a",
		Kind:        "loop",
		Phase:       "body",
		Iteration:   &iteration,
		Label:       "label-a",
		Attempt:     1,
	}}

	merged := MergeStartOptions(StartOptions{}, override)
	if merged.Provenance == override.Provenance {
		t.Fatal("merged provenance aliases the caller's value")
	}

	override.Provenance.RuntimeID = "rt_mutated"
	override.Provenance.Phase = "mutated"
	override.Provenance.Label = "mutated"
	override.Provenance.Attempt = 5
	iteration = 99

	snapshotIteration := 1
	want := LaunchProvenance{RuntimeID: "rt_a", ExecutionID: "run_a", Kind: "loop", Phase: "body", Iteration: &snapshotIteration, Label: "label-a", Attempt: 1}
	if !sameProvenance(*merged.Provenance, want) {
		t.Fatalf("merged provenance changed with the caller's value: %+v", *merged.Provenance)
	}
	if merged.Provenance.Iteration == nil || *merged.Provenance.Iteration != 1 {
		t.Fatalf("merged iteration aliases the caller's value: %v", merged.Provenance.Iteration)
	}
}

func TestMergeStartOptionsBrokerIdentityWinsAlongsideProvenance(t *testing.T) {
	merged := MergeStartOptions(StartOptions{RuntimeID: "rt_base", BrokerToken: "tok_base"}, StartOptions{
		RuntimeID:   "rt_broker",
		BrokerToken: "tok_broker",
		Provenance:  &LaunchProvenance{RuntimeID: "rt_informational", ExecutionID: "run_informational"},
	})
	if merged.RuntimeID != "rt_broker" || merged.BrokerToken != "tok_broker" {
		t.Fatalf("broker identity = (%q, %q), want (rt_broker, tok_broker)", merged.RuntimeID, merged.BrokerToken)
	}
	if merged.Provenance == nil || merged.Provenance.RuntimeID != "rt_informational" {
		t.Fatalf("provenance = %+v, want rt_informational", merged.Provenance)
	}
}

func TestMergeStartOptionsProvenanceSnapshotsBaseWhenOverrideIsNil(t *testing.T) {
	iteration := 2
	base := StartOptions{Provenance: &LaunchProvenance{Phase: "verify", Iteration: &iteration}}
	merged := MergeStartOptions(base, StartOptions{})
	base.Provenance.Phase = "changed"
	iteration = 9
	if merged.Provenance == nil || merged.Provenance.Phase != "verify" || *merged.Provenance.Iteration != 2 {
		t.Fatalf("merged provenance followed base mutation: %+v", merged.Provenance)
	}
}

func TestMergeStartOptionsProvenanceStaysOutOfBrokerIdentity(t *testing.T) {
	merged := MergeStartOptions(StartOptions{}, StartOptions{Provenance: &LaunchProvenance{
		RuntimeID:   "rt_informational",
		ExecutionID: "run_informational",
		Kind:        "team",
		Attempt:     1,
	}})
	if merged.RuntimeID != "" {
		t.Fatalf("provenance RuntimeID leaked into broker RuntimeID: %q", merged.RuntimeID)
	}
	if merged.BrokerToken != "" {
		t.Fatalf("provenance affected BrokerToken: %q", merged.BrokerToken)
	}
}
