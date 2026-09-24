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
	"os/exec"
	"strings"
	"syscall"
	"time"
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
// controller's [30s, 5m] range.
func ClampRetryDelay(requested time.Duration) time.Duration {
	if requested < AdapterMinRetryDelay {
		return AdapterMinRetryDelay
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

	cmd := &exec.Cmd{
		Path: m.ResolvedPath,
		Args: append([]string{m.ResolvedPath}, m.Args...),
		Dir:  m.manifestDir,
		Env:  allowlistedEnv(m.InheritEnv),
		// Start the adapter in its own process group so the whole group can
		// be killed on timeout or cancellation.
		SysProcAttr: &syscall.SysProcAttr{Setpgid: true},
	}
	stdoutBuf := &boundedBuffer{limit: m.MaxStdoutBytes}
	stderrBuf := &boundedBuffer{limit: m.MaxStderrBytes}
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return AdapterResult{}, err
	}
	if err := cmd.Start(); err != nil {
		return AdapterResult{}, fmt.Errorf("adapter %s start: %w", m.ID, err)
	}
	go func() {
		// A child that exits early makes this write fail with EPIPE; that is
		// the child's outcome, not a controller error.
		_, _ = stdin.Write(stdinJSON)
		_ = stdin.Close()
	}()

	pgid := cmd.Process.Pid
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	timer := time.NewTimer(time.Duration(m.TimeoutMS) * time.Millisecond)
	defer timer.Stop()

	var waitErr error
	select {
	case waitErr = <-waitCh:
	case <-timer.C:
		killProcessGroup(pgid)
		waitErr = <-waitCh
		if stdoutBuf.exceeded || stderrBuf.exceeded {
			return AdapterResult{}, adapterTooLargeError(m, stdoutBuf, stderrBuf)
		}
		return AdapterResult{}, fmt.Errorf("%w: adapter %s exceeded %dms", ErrAdapterTimeout, m.ID, m.TimeoutMS)
	case <-ctx.Done():
		killProcessGroup(pgid)
		waitErr = <-waitCh
		return AdapterResult{}, fmt.Errorf("adapter %s canceled: %w", m.ID, ctx.Err())
	}

	// Reap any orphaned group members left behind by the adapter.
	killProcessGroup(pgid)

	if stdoutBuf.exceeded || stderrBuf.exceeded {
		return AdapterResult{}, adapterTooLargeError(m, stdoutBuf, stderrBuf)
	}
	if waitErr != nil {
		return AdapterResult{}, fmt.Errorf("adapter %s exited: %w", m.ID, waitErr)
	}
	return parseAdapterResult(m, stdoutBuf.buf.Bytes(), redactStderr(m.InheritEnv, stderrBuf.buf.String()))
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

// killProcessGroup sends SIGKILL to the whole process group. A vanished group
// is not an error.
func killProcessGroup(pgid int) {
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
}

// boundedBuffer captures a stream up to limit bytes; a write that would
// exceed the limit fails the stream and flags the buffer.
type boundedBuffer struct {
	buf      bytes.Buffer
	limit    int64
	exceeded bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if int64(b.buf.Len())+int64(len(p)) > b.limit {
		b.exceeded = true
		return 0, fmt.Errorf("%w: %d bytes exceeds the %d byte limit", ErrAdapterOutputTooLarge, int64(b.buf.Len())+int64(len(p)), b.limit)
	}
	return b.buf.Write(p)
}

func adapterTooLargeError(m *AdapterManifest, stdout, stderr *boundedBuffer) error {
	if stdout.exceeded {
		return fmt.Errorf("%w: adapter %s stdout exceeded %d bytes", ErrAdapterOutputTooLarge, m.ID, m.MaxStdoutBytes)
	}
	return fmt.Errorf("%w: adapter %s stderr exceeded %d bytes", ErrAdapterOutputTooLarge, m.ID, m.MaxStderrBytes)
}

// redactStderr replaces every non-empty allowlisted environment value found in
// the captured stderr with [REDACTED].
func redactStderr(names []string, stderr string) string {
	for _, name := range names {
		if v := os.Getenv(name); v != "" {
			stderr = strings.ReplaceAll(stderr, v, "[REDACTED]")
		}
	}
	return stderr
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
