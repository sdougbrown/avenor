package runtime

import (
	"strconv"
	"strings"

	"github.com/sdougbrown/avenor/internal/token"
)

// Environment variables that describe the launch a harness child belongs to.
// They are informational: a child may forge them and nothing authorizes on
// them.
const (
	EnvRuntimeID   = "AVENOR_RUNTIME_ID"
	EnvExecutionID = "AVENOR_EXECUTION_ID"
	EnvKind        = "AVENOR_KIND"
	EnvPhase       = "AVENOR_PHASE"
	EnvIteration   = "AVENOR_ITERATION"
	EnvLabel       = "AVENOR_LABEL"
	EnvAttempt     = "AVENOR_ATTEMPT"

	// envParentRunID is superseded by EnvExecutionID. It is never emitted and
	// only scrubbed so inherited values from older harness versions go away.
	envParentRunID = "AVENOR_PARENT_RUN_ID"

	// Test-only switches. They are never emitted and are scrubbed at every
	// strip point so a child cannot inherit them.
	EnvTestHooks             = "AVENOR_TEST_HOOKS"
	EnvTestOwnerPublishDelay = "AVENOR_TEST_OWNER_PUBLISH_DELAY"
)

var identityEnvNames = []string{
	EnvRuntimeID,
	EnvExecutionID,
	EnvKind,
	EnvPhase,
	EnvIteration,
	EnvLabel,
	EnvAttempt,
	envParentRunID,
}

// scrubbedEnvNames is everything a child must not inherit: launch identity,
// the test-only switches, and the parent-link credential — a spawned agent is
// not the parent broker, so carrying the credential would let it authenticate
// against parent routes it was never meant to serve.
var scrubbedEnvNames = append(append([]string(nil), identityEnvNames...), EnvTestHooks, EnvTestOwnerPublishDelay, token.EnvBrokerParentToken)

var identityEnvKeys = func() map[string]struct{} {
	keys := make(map[string]struct{}, len(scrubbedEnvNames))
	for _, name := range scrubbedEnvNames {
		keys[name] = struct{}{}
	}
	return keys
}()

// IdentityEnvNames returns every environment name that describes launch
// identity, including the superseded name that is scrubbed but never emitted.
// A shell launcher that cannot set its child's environment needs the names to
// remove inherited values itself.
func IdentityEnvNames() []string {
	return append([]string(nil), identityEnvNames...)
}

// ScrubbedEnvNames returns every environment name a child must not inherit:
// launch identity, the test-only switches, and the parent-link credential.
// A shell launcher that assembles its child's environment by hand unsets
// these instead of calling ScrubIdentityEnv.
func ScrubbedEnvNames() []string {
	return append([]string(nil), scrubbedEnvNames...)
}

// Launch kinds reported in LaunchProvenance.Kind.
const (
	LaunchKindRun  = "run"
	LaunchKindTeam = "team"
	LaunchKindLoop = "loop"
)

// LaunchProvenance is the informational identity of one harness launch. It is
// separate from the broker RuntimeID in StartOptions, which keeps its routing
// meaning.
type LaunchProvenance struct {
	RuntimeID   string // supervisor child runtime ID, or the CLI run ID
	ExecutionID string // containing execution: CLI run ID or supervisor runtime ID
	Kind        string // containing phase runner: run, team or loop
	Phase       string // member or phase name; empty for a direct run
	Iteration   *int   // loop iteration, including zero for pre/post; nil when not a loop
	Label       string
	Attempt     int // 1-based launch-attempt ordinal; 0 means unset
}

// DirectLaunchProvenance reports the identity of a direct launch attempt. The
// attempt is already counted; a direct launch has no phase or iteration.
func DirectLaunchProvenance(runID, label string, attempt int) *LaunchProvenance {
	return &LaunchProvenance{
		RuntimeID:   runID,
		ExecutionID: runID,
		Kind:        LaunchKindRun,
		Label:       label,
		Attempt:     attempt,
	}
}

// PhaseLaunchProvenance reports the identity of one phase launch. ordinal is
// the zero-based launch ordinal a runner hands to its attempt callback, so the
// reported attempt is always 1-based.
func PhaseLaunchProvenance(runID, kind, phase, label string, iteration *int, ordinal int) *LaunchProvenance {
	return &LaunchProvenance{
		RuntimeID:   runID,
		ExecutionID: runID,
		Kind:        kind,
		Phase:       phase,
		Iteration:   iteration,
		Label:       label,
		Attempt:     ordinal + 1,
	}
}

// IdentityEnv returns the KEY=VALUE entries for the present identity fields in
// a stable order. A nil provenance yields no entries.
func (p *LaunchProvenance) IdentityEnv() []string {
	if p == nil {
		return nil
	}
	env := make([]string, 0, 7)
	if p.RuntimeID != "" {
		env = append(env, EnvRuntimeID+"="+p.RuntimeID)
	}
	if p.ExecutionID != "" {
		env = append(env, EnvExecutionID+"="+p.ExecutionID)
	}
	if p.Kind != "" {
		env = append(env, EnvKind+"="+p.Kind)
	}
	if p.Phase != "" {
		env = append(env, EnvPhase+"="+p.Phase)
	}
	if p.Iteration != nil && *p.Iteration >= 0 {
		env = append(env, EnvIteration+"="+strconv.Itoa(*p.Iteration))
	}
	if p.Label != "" {
		env = append(env, EnvLabel+"="+p.Label)
	}
	if p.Attempt > 0 {
		env = append(env, EnvAttempt+"="+strconv.Itoa(p.Attempt))
	}
	return env
}

// LaunchEnv returns environ without inherited launch identity, followed by p's
// identity. Scrubbing first keeps the launch's own values; a nil p yields the
// scrubbed environment alone.
func LaunchEnv(environ []string, p *LaunchProvenance) []string {
	return append(ScrubIdentityEnv(environ), p.IdentityEnv()...)
}

// ScrubIdentityEnv returns environ without inherited launch identity. Matching
// is on the exact key, so duplicates and bare-key entries are removed too,
// while near-miss names and every other variable keep their order.
func ScrubIdentityEnv(environ []string) []string {
	if len(environ) == 0 {
		return nil
	}
	kept := make([]string, 0, len(environ))
	for _, entry := range environ {
		key, _, _ := strings.Cut(entry, "=")
		if _, ok := identityEnvKeys[key]; ok {
			continue
		}
		kept = append(kept, entry)
	}
	return kept
}
