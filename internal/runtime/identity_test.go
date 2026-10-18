package runtime

import (
	"slices"
	"testing"
)

func intPtr(v int) *int { return &v }

func TestIdentityEnv(t *testing.T) {
	full := &LaunchProvenance{
		RuntimeID:   "rt_member",
		ExecutionID: "run_team",
		Kind:        "team",
		Phase:       "reviewer",
		Iteration:   intPtr(3),
		Label:       "review $(id);done",
		Attempt:     2,
	}
	tests := []struct {
		name       string
		provenance *LaunchProvenance
		want       []string
	}{
		{
			name:       "nil provenance emits nothing",
			provenance: nil,
			want:       nil,
		},
		{
			name:       "every present field in a stable order",
			provenance: full,
			want: []string{
				"AVENOR_RUNTIME_ID=rt_member",
				"AVENOR_EXECUTION_ID=run_team",
				"AVENOR_KIND=team",
				"AVENOR_PHASE=reviewer",
				"AVENOR_ITERATION=3",
				"AVENOR_LABEL=review $(id);done",
				"AVENOR_ATTEMPT=2",
			},
		},
		{
			name:       "absent fields are omitted",
			provenance: &LaunchProvenance{RuntimeID: "run_direct", ExecutionID: "run_direct", Kind: "run", Attempt: 1},
			want:       []string{"AVENOR_RUNTIME_ID=run_direct", "AVENOR_EXECUTION_ID=run_direct", "AVENOR_KIND=run", "AVENOR_ATTEMPT=1"},
		},
		{
			name:       "zero iteration is present for loop pre and post",
			provenance: &LaunchProvenance{Iteration: intPtr(0), Attempt: 1},
			want:       []string{"AVENOR_ITERATION=0", "AVENOR_ATTEMPT=1"},
		},
		{
			name:       "negative iteration and attempt are omitted",
			provenance: &LaunchProvenance{Iteration: intPtr(-1), Attempt: -1},
			want:       nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.provenance.IdentityEnv(); !slices.Equal(got, tt.want) {
				t.Fatalf("IdentityEnv() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestScrubIdentityEnv(t *testing.T) {
	tests := []struct {
		name    string
		environ []string
		want    []string
	}{
		{
			name:    "nil environ",
			environ: nil,
			want:    nil,
		},
		{
			name:    "identity only",
			environ: []string{"AVENOR_LABEL=x", "AVENOR_PARENT_RUN_ID=run_grandparent"},
			want:    nil,
		},
		{
			name: "exact keys removed, near misses and broker credentials preserved",
			environ: []string{
				"AVENOR_RUNTIME_ID=rt_parent",
				"AVENOR_EXECUTION_ID=run_parent",
				"AVENOR_KIND",
				"AVENOR_PHASE=",
				"AVENOR_ITERATION=2",
				"AVENOR_ITERATION=3",
				"AVENOR_LABEL=stale",
				"AVENOR_ATTEMPT=7",
				"AVENOR_PARENT_RUN_ID=run_grandparent",
				"AVENOR_RUN_ID=run_broker",
				"AVENOR_BROKER_TOKEN=token",
				"AVENOR_BROKER_URL=http://127.0.0.1:1",
				"AVENOR_TREE_BUDGET=100",
				"AVENOR_PHASE_NAME=kept",
				"AVENOR_PHASEX=kept",
				"X=AVENOR_KIND=kept",
				"PATH=/usr/bin",
			},
			want: []string{
				"AVENOR_RUN_ID=run_broker",
				"AVENOR_BROKER_TOKEN=token",
				"AVENOR_BROKER_URL=http://127.0.0.1:1",
				"AVENOR_TREE_BUDGET=100",
				"AVENOR_PHASE_NAME=kept",
				"AVENOR_PHASEX=kept",
				"X=AVENOR_KIND=kept",
				"PATH=/usr/bin",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ScrubIdentityEnv(tt.environ); !slices.Equal(got, tt.want) {
				t.Fatalf("ScrubIdentityEnv() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLaunchEnvKeepsLaunchIdentityOverInherited(t *testing.T) {
	env := LaunchEnv(
		[]string{"PATH=/bin", EnvPhase + "=inherited", EnvExecutionID + "=outer"},
		&LaunchProvenance{ExecutionID: "run_1", Phase: "reviewer"},
	)
	want := []string{"PATH=/bin", EnvExecutionID + "=run_1", EnvPhase + "=reviewer"}
	if !slices.Equal(env, want) {
		t.Fatalf("LaunchEnv = %v, want %v", env, want)
	}
}
