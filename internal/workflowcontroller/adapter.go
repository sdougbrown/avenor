//go:build unix

package workflowcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/sdougbrown/avenor/internal/commandrunner"
)

// Adapter verdict values emitted by an adapter result.
const (
	AdapterResultPending          = "pending"
	AdapterResultPassed           = "passed"
	AdapterResultFailed           = "failed"
	AdapterResultActionRequired   = "action_required"
	AdapterResultChangesRequested = "changes_requested"
)

// Retry delay clamping bounds for adapter results.
const (
	AdapterMinRetryDelay = 30 * time.Second
	AdapterMaxRetryDelay = 5 * time.Minute
)

// Sentinel errors returned by Invoke.
var (
	// ErrAdapterChanged is returned when the manifest or executable differs
	// from the state recorded at load time.
	ErrAdapterChanged = errors.New("adapter changed since load")
	// ErrAdapterOutputTooLarge is returned when stdout or stderr exceeds the
	// manifest's declared byte limit.
	ErrAdapterOutputTooLarge = errors.New("adapter output exceeded limit")
	// ErrAdapterTimeout is returned when the adapter does not finish within
	// its manifest timeout.
	ErrAdapterTimeout = errors.New("adapter timed out")
	// ErrAdapterInvalidInput is returned when the poll request input does not
	// match the manifest's declared input schema.
	ErrAdapterInvalidInput = errors.New("adapter input failed validation")
	// ErrAdapterInvalidResult is returned when the adapter's stdout is not a
	// valid adapter result.
	ErrAdapterInvalidResult = errors.New("adapter returned an invalid result")
	// ErrAdapterStreamsHeld is returned when the adapter exited while a
	// descendant which left its process group still held the adapter's output.
	// The captured stdout may be cut short, so it is never parsed as a verdict.
	ErrAdapterStreamsHeld = errors.New("adapter output was not closed by the adapter")
)

// PollRequest is the versioned object written to an adapter's stdin.
type PollRequest struct {
	Version      int             `json:"version"`
	PollID       string          `json:"poll_id"`
	WorkflowID   string          `json:"workflow_id"`
	NodeID       string          `json:"node_id"`
	ActivationID string          `json:"activation_id"`
	GateID       string          `json:"gate_id"`
	Input        json.RawMessage `json:"input"`
}

// AdapterSubject is the exact external subject an adapter observed.
type AdapterSubject struct {
	Type        string `json:"type"`
	Repository  string `json:"repository"`
	PullRequest int    `json:"pull_request"`
	Revision    string `json:"revision"`
}

// AdapterResult is a parsed adapter response. RawStdout preserves the exact
// bounded bytes the adapter wrote; ResponseHash is computed over those bytes
// by the controller and never trusted from the adapter. Stderr is redacted,
// diagnostic-only output.
type AdapterResult struct {
	Version      int
	Result       string
	Subject      AdapterSubject
	ObservedAt   time.Time
	Summary      string
	RetryAfterMS *int64

	RawStdout    []byte
	ResponseHash string
	Stderr       string
}

// ClampRetryDelay clamps an adapter-requested retry delay into the
// [floor, 5m] range. Callers choose the floor: adapter retry handling uses
// AdapterMinRetryDelay, poll backoff uses the configurable base interval.
func ClampRetryDelay(requested, floor time.Duration) time.Duration {
	if requested < floor {
		return floor
	}
	if requested > AdapterMaxRetryDelay {
		return AdapterMaxRetryDelay
	}
	return requested
}

// Invoke runs the adapter executable directly (no shell), sends the poll
// request as one JSON object on stdin, and parses its single-object JSON
// result from stdout. The request input is validated against the manifest
// before exec, and the executable and manifest are rechecked against their
// load-time identity; any drift aborts before the process starts. The child
// runs in its own process group, which is killed on timeout, context
// cancellation, or output-limit violation.
func Invoke(ctx context.Context, m *AdapterManifest, req PollRequest) (AdapterResult, error) {
	return invoke(ctx, m, req, nil)
}

// invoke is Invoke with an injectable timeout channel: when timeoutC is
// non-nil it replaces the manifest timeout's real timer so a test can fire
// the timeout explicitly instead of racing a wall-clock timer against process
// startup.
func invoke(ctx context.Context, m *AdapterManifest, req PollRequest, timeoutC <-chan time.Time) (AdapterResult, error) {
	if req.Version != 1 {
		return AdapterResult{}, fmt.Errorf("poll request version must be 1, got %d", req.Version)
	}
	if err := validateAdapterInput(m.Inputs, req.Input); err != nil {
		return AdapterResult{}, err
	}
	if err := m.recheckTrust(); err != nil {
		return AdapterResult{}, err
	}

	stdinJSON, err := json.Marshal(req)
	if err != nil {
		return AdapterResult{}, err
	}

	timeout := time.Duration(m.TimeoutMS) * time.Millisecond
	res, err := commandrunner.Run(ctx, commandrunner.Spec{
		Path:  m.ResolvedPath,
		Args:  append([]string{m.ResolvedPath}, m.Args...),
		Dir:   m.manifestDir,
		Env:   allowlistedEnv(m.InheritEnv),
		Stdin: stdinJSON,
		Limits: commandrunner.Limits{
			Stdout: m.MaxStdoutBytes,
			Stderr: m.MaxStderrBytes,
		},
		Timeout:  timeout,
		TimeoutC: timeoutC,
		Redact:   inheritEnvValues(m.InheritEnv),
	})
	switch {
	case errors.Is(err, commandrunner.ErrTimeout):
		return AdapterResult{}, fmt.Errorf("%w: adapter %s exceeded %s", ErrAdapterTimeout, m.ID, timeout)
	case errors.Is(err, commandrunner.ErrOutputTooLarge):
		return AdapterResult{}, fmt.Errorf("%w: adapter %s: %v", ErrAdapterOutputTooLarge, m.ID, err)
	case errors.Is(err, commandrunner.ErrCanceled):
		return AdapterResult{}, fmt.Errorf("adapter %s canceled: %w", m.ID, ctx.Err())
	case err != nil:
		return AdapterResult{}, fmt.Errorf("adapter %s %w", m.ID, err)
	}
	if res.ExitErr != nil {
		return AdapterResult{}, fmt.Errorf("adapter %s exited: %w", m.ID, res.ExitErr)
	}
	if res.StreamsHeld {
		// A descendant the adapter left behind kept its output open, so the
		// result it wrote cannot be trusted to be complete.
		return AdapterResult{}, fmt.Errorf("%w: adapter %s", ErrAdapterStreamsHeld, m.ID)
	}
	return parseAdapterResult(m, res.Stdout, res.Stderr)
}

// allowlistedEnv builds a child environment containing only the allowlisted
// names copied from the current environment. Nothing else is inherited.
func allowlistedEnv(names []string) []string {
	env := make([]string, 0, len(names))
	for _, name := range names {
		if v, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+v)
		}
	}
	return env
}

// inheritEnvValues returns the current values of the allowlisted names, so the
// runner can redact them from the captured stderr.
func inheritEnvValues(names []string) []string {
	values := make([]string, 0, len(names))
	for _, name := range names {
		if v := os.Getenv(name); v != "" {
			values = append(values, v)
		}
	}
	return values
}

// validateAdapterInput checks req input against the manifest's declared input
// schema: every declared input present, no extras, and JSON types matching.
func validateAdapterInput(inputs map[string]string, raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("%w: input must be a JSON object: %v", ErrAdapterInvalidInput, err)
	}
	for name := range fields {
		if _, ok := inputs[name]; !ok {
			return fmt.Errorf("%w: unexpected input %q", ErrAdapterInvalidInput, name)
		}
	}
	for name, typ := range inputs {
		v, ok := fields[name]
		if !ok {
			return fmt.Errorf("%w: missing input %q", ErrAdapterInvalidInput, name)
		}
		if err := checkInputType(name, typ, v); err != nil {
			return err
		}
	}
	return nil
}

// maxSafeInteger is the largest magnitude integer that round-trips through a
// float64 without loss.
const maxSafeInteger = float64(int64(1) << 53)

func checkInputType(name, typ string, v json.RawMessage) error {
	switch typ {
	case "string":
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return fmt.Errorf("%w: input %q must be a string", ErrAdapterInvalidInput, name)
		}
	case "integer", "number":
		article := "a"
		if typ == "integer" {
			article = "an"
		}
		dec := json.NewDecoder(bytes.NewReader(v))
		dec.UseNumber()
		var val any
		if err := dec.Decode(&val); err != nil {
			return fmt.Errorf("%w: input %q must be %s %s", ErrAdapterInvalidInput, name, article, typ)
		}
		n, ok := val.(json.Number)
		if !ok {
			return fmt.Errorf("%w: input %q must be %s %s", ErrAdapterInvalidInput, name, article, typ)
		}
		if typ == "integer" {
			i, err := n.Int64()
			if err != nil {
				return fmt.Errorf("%w: input %q must be an integral number", ErrAdapterInvalidInput, name)
			}
			if math.Abs(float64(i)) > maxSafeInteger {
				return fmt.Errorf("%w: input %q exceeds the safe integer range", ErrAdapterInvalidInput, name)
			}
		} else if _, err := n.Float64(); err != nil {
			return fmt.Errorf("%w: input %q must be a number", ErrAdapterInvalidInput, name)
		}
	case "boolean":
		var b bool
		if err := json.Unmarshal(v, &b); err != nil {
			return fmt.Errorf("%w: input %q must be a boolean", ErrAdapterInvalidInput, name)
		}
	}
	return nil
}

// adapterResultWire is the strict stdout contract of an adapter.
type adapterResultWire struct {
	Version      int             `json:"version"`
	Result       string          `json:"result"`
	Subject      *AdapterSubject `json:"subject"`
	ObservedAt   *time.Time      `json:"observed_at"`
	Summary      string          `json:"summary"`
	RetryAfterMS *int64          `json:"retry_after_ms"`
}

// parseAdapterResult strictly parses exactly one JSON result object from the
// adapter's captured stdout.
func parseAdapterResult(m *AdapterManifest, stdout []byte, stderr string) (AdapterResult, error) {
	dec := json.NewDecoder(bytes.NewReader(stdout))
	dec.DisallowUnknownFields()
	var wire adapterResultWire
	if err := dec.Decode(&wire); err != nil {
		return AdapterResult{}, fmt.Errorf("%w: adapter %s: %v", ErrAdapterInvalidResult, m.ID, err)
	}
	if dec.More() {
		return AdapterResult{}, fmt.Errorf("%w: adapter %s: trailing data after result object", ErrAdapterInvalidResult, m.ID)
	}
	if wire.Version != 1 {
		return AdapterResult{}, fmt.Errorf("%w: adapter %s: version must be 1, got %d", ErrAdapterInvalidResult, m.ID, wire.Version)
	}
	switch wire.Result {
	case AdapterResultPending, AdapterResultPassed, AdapterResultFailed,
		AdapterResultActionRequired, AdapterResultChangesRequested:
	default:
		return AdapterResult{}, fmt.Errorf("%w: adapter %s: unknown result %q", ErrAdapterInvalidResult, m.ID, wire.Result)
	}
	if wire.Subject == nil {
		return AdapterResult{}, fmt.Errorf("%w: adapter %s: subject is required", ErrAdapterInvalidResult, m.ID)
	}
	if wire.ObservedAt == nil {
		return AdapterResult{}, fmt.Errorf("%w: adapter %s: observed_at is required", ErrAdapterInvalidResult, m.ID)
	}
	if wire.RetryAfterMS != nil && *wire.RetryAfterMS < 0 {
		return AdapterResult{}, fmt.Errorf("%w: adapter %s: retry_after_ms must be non-negative", ErrAdapterInvalidResult, m.ID)
	}
	result := AdapterResult{
		Version:      wire.Version,
		Result:       wire.Result,
		Subject:      *wire.Subject,
		ObservedAt:   *wire.ObservedAt,
		Summary:      wire.Summary,
		RetryAfterMS: wire.RetryAfterMS,
		RawStdout:    stdout,
		Stderr:       stderr,
	}
	sum := sha256.Sum256(stdout)
	result.ResponseHash = hex.EncodeToString(sum[:])
	return result, nil
}
