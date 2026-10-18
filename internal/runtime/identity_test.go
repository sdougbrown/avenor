package runtime

import (
	"slices"
	"testing"
)

func intPtr(v int) *int { return &v }

func TestIdentityEnvPresentValues(t *testing.T) {
	provenance := &LaunchProvenance{
		RuntimeID:   "rt_member",
		ExecutionID: "run_team",
		Kind:        "team",
		Phase:       "reviewer",
		Iteration:   intPtr(3),
		Label:       "verify",
		Attempt:     1,
	}
	want := []string{
		"AVENOR_RUNTIME_ID=rt_member",
		"AVENOR_EXECUTION_ID=run_team",
		"AVENOR_KIND=team",
		"AVENOR_PHASE=reviewer",
		"AVENOR_ITERATION=3",
		"AVENOR_LABEL=verify",
		"AVENOR_ATTEMPT=1",
	}
	if got := provenance.IdentityEnv(); !slices.Equal(got, want) {
		t.Fatalf("IdentityEnv() = %q, want %q", got, want)
	}
}

func TestIdentityEnvOmitsAbsentValues(t *testing.T) {
	provenance := &LaunchProvenance{RuntimeID: "run_direct", ExecutionID: "run_direct", Kind: "run", Attempt: 2}
	want := []string{
		"AVENOR_RUNTIME_ID=run_direct",
		"AVENOR_EXECUTION_ID=run_direct",
		"AVENOR_KIND=run",
		"AVENOR_ATTEMPT=2",
	}
	if got := provenance.IdentityEnv(); !slices.Equal(got, want) {
		t.Fatalf("IdentityEnv() = %q, want %q", got, want)
	}
}

func TestIdentityEnvFormatsNumbers(t *testing.T) {
	tests := []struct {
		name      string
		iteration *int
		attempt   int
		want      []string
	}{
		{
			name:      "zero iteration is present for loop pre and post",
			iteration: intPtr(0),
			attempt:   1,
			want:      []string{"AVENOR_ITERATION=0", "AVENOR_ATTEMPT=1"},
		},
		{
			name:      "two digit values",
			iteration: intPtr(12),
			attempt:   10,
			want:      []string{"AVENOR_ITERATION=12", "AVENOR_ATTEMPT=10"},
		},
		{
			name:    "no iteration and no attempt",
			attempt: 0,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provenance := &LaunchProvenance{Iteration: tt.iteration, Attempt: tt.attempt}
			if got := provenance.IdentityEnv(); !slices.Equal(got, tt.want) {
				t.Fatalf("IdentityEnv() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIdentityEnvNilProvenanceEmitsNothing(t *testing.T) {
	var provenance *LaunchProvenance
	if got := provenance.IdentityEnv(); got != nil {
		t.Fatalf("nil provenance emitted %q", got)
	}
}

func TestIdentityEnvKeepsLabelsVerbatim(t *testing.T) {
	label := `review "$(id -u)" 'quoted'; rm -rf /` + "\n\tnaïve—テスト"
	provenance := &LaunchProvenance{Label: label}
	want := []string{"AVENOR_LABEL=" + label}
	if got := provenance.IdentityEnv(); !slices.Equal(got, want) {
		t.Fatalf("IdentityEnv() = %q, want %q", got, want)
	}
}

func TestScrubIdentityEnvRemovesExactKeysOnly(t *testing.T) {
	environ := []string{
		"AVENOR_RUNTIME_ID=rt_parent",
		"AVENOR_EXECUTION_ID=run_parent",
		"AVENOR_KIND=loop",
		"AVENOR_PHASE=body",
		"AVENOR_ITERATION=2",
		"AVENOR_LABEL=stale",
		"AVENOR_ATTEMPT=7",
		"AVENOR_PARENT_RUN_ID=run_grandparent",
		"AVENOR_RUN_ID=run_broker",
		"AVENOR_BROKER_TOKEN=token",
		"AVENOR_BROKER_URL=http://127.0.0.1:1",
		"AVENOR_TREE_BUDGET=100",
		"AVENOR_AGY_TRANSPORT=rpc",
		"AVENOR_CLAUDE_TERMINAL=tmux",
		"AVENOR_CONTROL_IDENTITY_TOKEN=control",
		"AVENOR_OPENCODE_URL=http://127.0.0.1:2",
		"AVENOR_PHASE_NAME=kept",
		"AVENOR_PHASEX=kept",
		"XAVENOR_KIND=kept",
		"X=AVENOR_KIND=kept",
		"PATH=/usr/bin",
	}
	want := []string{
		"AVENOR_RUN_ID=run_broker",
		"AVENOR_BROKER_TOKEN=token",
		"AVENOR_BROKER_URL=http://127.0.0.1:1",
		"AVENOR_TREE_BUDGET=100",
		"AVENOR_AGY_TRANSPORT=rpc",
		"AVENOR_CLAUDE_TERMINAL=tmux",
		"AVENOR_CONTROL_IDENTITY_TOKEN=control",
		"AVENOR_OPENCODE_URL=http://127.0.0.1:2",
		"AVENOR_PHASE_NAME=kept",
		"AVENOR_PHASEX=kept",
		"XAVENOR_KIND=kept",
		"X=AVENOR_KIND=kept",
		"PATH=/usr/bin",
	}
	if got := ScrubIdentityEnv(environ); !slices.Equal(got, want) {
		t.Fatalf("ScrubIdentityEnv() = %q, want %q", got, want)
	}
}

func TestScrubIdentityEnvRemovesDuplicatesAndBareKeys(t *testing.T) {
	environ := []string{
		"AVENOR_RUNTIME_ID=first",
		"AVENOR_RUNTIME_ID=second",
		"AVENOR_RUNTIME_ID=",
		"AVENOR_KIND",
		"AVENOR_PHASE=",
		"AVENOR_ITERATION=1",
		"AVENOR_ITERATION=2",
		"AVENOR_ATTEMPT=1",
		"AVENOR_ATTEMPT",
		"HOME=/home/user",
	}
	want := []string{"HOME=/home/user"}
	if got := ScrubIdentityEnv(environ); !slices.Equal(got, want) {
		t.Fatalf("ScrubIdentityEnv() = %q, want %q", got, want)
	}
}

func TestScrubIdentityEnvEmptyInput(t *testing.T) {
	if got := ScrubIdentityEnv(nil); len(got) != 0 {
		t.Fatalf("ScrubIdentityEnv(nil) = %q, want nothing", got)
	}
	if got := ScrubIdentityEnv([]string{"AVENOR_LABEL=x"}); len(got) != 0 {
		t.Fatalf("ScrubIdentityEnv(identity only) = %q, want nothing", got)
	}
}
