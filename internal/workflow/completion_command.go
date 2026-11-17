package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/commandrunner"
)

// completion_command.go implements the supervisor-side external-state check a
// node declares with a command completion contract. The check runs after the
// worker exits successfully, in the attempt's working directory. It runs with
// no shell, a fixed literal executable, an empty environment, a declared
// deadline, and bounded streams. Exit 0 meets the contract. Anything else
// leaves it unmet, so the attempt is recorded as contract_unmet and the node's
// retry policy applies.
//
// Either way the check's bounded stdout and stderr are captured as evidence,
// together with one inert summary of what ran and how it ended. The summary
// carries the argv, the exit code when the check exited on its own, the
// duration, and the captured streams' sizes and digests. A failed check is
// therefore as inspectable as a passing one.
//
// The check is an observer: it may run again for a replacement attempt after a
// recovery, so it must be read-only and idempotent. It runs as the supervisor's
// user, the same user the worker runs as, and is not a sandbox. It contains a
// process group and bounds its output, nothing more.

const (
	// maxCompletionCommandTimeoutSeconds bounds one completion command's
	// declared deadline.
	maxCompletionCommandTimeoutSeconds = 300
	// completionCommandStdoutBytes bounds the captured stdout, which is staged
	// as durable evidence and parsed for declared output pointers.
	completionCommandStdoutBytes = 64 << 10
	// completionCommandStderrBytes bounds the captured stderr, which is staged
	// as durable evidence.
	completionCommandStderrBytes = 16 << 10
	// completionCommandDiagnosticBytes bounds how much of a failed check's
	// output reaches both the recorded summary and the contract_unmet
	// rejection text.
	completionCommandDiagnosticBytes = 2048
	// completionCommandStdoutStoredPath and completionCommandStderrStoredPath
	// are the evidence names the captured streams are staged under, so an
	// operator can inspect exactly what the check printed.
	completionCommandStdoutStoredPath = "completion-command.stdout"
	completionCommandStderrStoredPath = "completion-command.stderr"
)

// completionCommandTermination names how a completion check ended. It is
// recorded in the check's evidence summary.
type completionCommandTermination string

const (
	// completionCommandExited is a check that ran and exited on its own; its
	// exit code decides the contract.
	completionCommandExited completionCommandTermination = "exited"
	// completionCommandTimedOut is a check killed at its declared deadline.
	completionCommandTimedOut completionCommandTermination = "timed_out"
	// completionCommandCanceled is a check killed by its attempt's
	// cancellation.
	completionCommandCanceled completionCommandTermination = "canceled"
	// completionCommandTooLarge is a check killed for passing a stream limit.
	completionCommandTooLarge completionCommandTermination = "output_too_large"
	// completionCommandStartFailed is a check that never started.
	completionCommandStartFailed completionCommandTermination = "start_failed"
	// completionCommandSignaled is a check that ended on a signal without an
	// exit status of its own.
	completionCommandSignaled completionCommandTermination = "signaled"
)

// completionCommandSummary is the inert provenance recorded on a completion
// check's staged stdout evidence entry. It names what ran, how it ended, how
// long it took, and the digests of the captured streams.
type completionCommandSummary struct {
	Argv         []string                     `json:"argv,omitempty"`
	ArgvDropped  bool                         `json:"argv_dropped,omitempty"`
	Termination  completionCommandTermination `json:"termination"`
	ExitCode     *int                         `json:"exit_code,omitempty"`
	DurationMS   int64                        `json:"duration_ms"`
	StdoutBytes  int                          `json:"stdout_bytes"`
	StdoutSHA256 string                       `json:"stdout_sha256"`
	StderrBytes  int                          `json:"stderr_bytes"`
	StderrSHA256 string                       `json:"stderr_sha256,omitempty"`
	// StreamsTruncated reports that a descendant which escaped the check's
	// process group kept its output pipes open. The captured streams then stop
	// where the runner's drain bound fell.
	StreamsTruncated bool   `json:"streams_truncated,omitempty"`
	Diagnostic       string `json:"diagnostic,omitempty"`
}

// commandCheck is one completion command's outcome: the captures holding its
// bounded streams and run summary, and the stdout bytes for declared output
// pointers. A contract-unmet check returns both a populated check and an error,
// so the caller can record the evidence with the failed attempt.
type commandCheck struct {
	captures []CommandCapture
	stdout   []byte
	// argv is the resolved argv, for rejection text.
	argv []string
	// streamsHeld reports that the check exited while a descendant which left
	// its process group still held its output pipes. The captured stdout may
	// then stop short of what the check wrote.
	streamsHeld bool
}

// ContractUnmetError names why the supervisor's evaluation rejected a declared
// contract. A rejection that ran a completion check carries the evidence it
// captured. The failed check is therefore recorded with the attempt's terminal
// fact instead of surviving only in the supervisor's log.
type ContractUnmetError struct {
	// Reason names the unmet requirement, in the same words the supervisor
	// logs.
	Reason string
	// Evidence is the inspection evidence the check captured before the
	// rejection: its bounded streams and the run summary.
	Evidence []CommandCapture
	// Canceled reports that the check was killed by its attempt's cancellation
	// rather than judged by its own result. The caller then records the attempt
	// as canceled instead of contract-unmet.
	Canceled bool
}

func (e *ContractUnmetError) Error() string { return e.Reason }

// evalCommandContract runs the node's completion command in the attempt's
// working directory. A non-zero exit, deadline, cancellation, output overflow,
// or start failure leaves the contract unmet with a bounded diagnostic. It
// always captures what the check printed, so the failure is inspectable.
func evalCommandContract(ctx context.Context, command *CommandRequirement, params map[string]string, workingDir string, timeoutC <-chan time.Time) (commandCheck, error) {
	if command == nil {
		return commandCheck{}, errors.New("completion command is not declared")
	}
	argv, err := ResolveArgv("completion command", command.Argv, params)
	if err != nil {
		return commandCheck{}, err
	}
	timeout := time.Duration(command.TimeoutSeconds) * time.Second
	start := time.Now()
	result, runErr := commandrunner.Run(ctx, commandrunner.Spec{
		Path: argv[0],
		Args: argv,
		Dir:  workingDir,
		// An empty environment: the check observes external state without the
		// supervisor's or the worker's credentials.
		Env: []string{},
		Limits: commandrunner.Limits{
			Stdout: completionCommandStdoutBytes,
			Stderr: completionCommandStderrBytes,
		},
		Timeout:  timeout,
		TimeoutC: timeoutC,
	})
	duration := time.Since(start)

	check := commandCheck{argv: argv, stdout: result.Stdout, streamsHeld: result.StreamsHeld}
	reason, termination := commandRejection(argv, timeout, result, runErr)
	// A rejected run's report is captured for the evidence summary; a met
	// contract keeps no diagnostic, because its streams are the record.
	var diagnostic string
	if reason != "" {
		diagnostic = commandDiagnostic(result.Stderr, result.Stdout)
	}
	check.captures = commandEvidence(argv, termination, duration, result, diagnostic)
	if reason != "" {
		return check, &ContractUnmetError{
			Reason:   reason,
			Evidence: check.captures,
			Canceled: termination == completionCommandCanceled,
		}
	}
	return check, nil
}

// commandRejection classifies a finished run: an empty reason is a met contract,
// and any other value is the rejection text. The text names what happened
// without quoting the check's streams. A check's output can contain sensitive
// data, so it is persisted as bounded evidence and never written into the
// supervisor's log.
func commandRejection(argv []string, timeout time.Duration, result commandrunner.Result, runErr error) (string, completionCommandTermination) {
	switch {
	case errors.Is(runErr, commandrunner.ErrTimeout):
		return fmt.Sprintf("completion command %s timed out after %s", argv[0], timeout), completionCommandTimedOut
	case errors.Is(runErr, commandrunner.ErrCanceled):
		return fmt.Sprintf("completion command %s was canceled", argv[0]), completionCommandCanceled
	case errors.Is(runErr, commandrunner.ErrOutputTooLarge):
		return fmt.Sprintf("completion command %s: %v", argv[0], runErr), completionCommandTooLarge
	case runErr != nil:
		return fmt.Sprintf("completion command %s: %v", argv[0], runErr), completionCommandStartFailed
	case result.ExitCode < 0:
		// The runner did not end it, but the check died without an exit
		// status, so its report says so instead of inventing a code.
		return fmt.Sprintf("completion command %s was terminated by a signal", argv[0]), completionCommandSignaled
	case result.ExitCode != 0:
		return fmt.Sprintf("completion command %s exited with status %d", argv[0], result.ExitCode), completionCommandExited
	}
	return "", completionCommandExited
}

// commandEvidence builds the captures for one check: the captured stdout
// (always, carrying the run summary) and the captured stderr when the check
// wrote any.
func commandEvidence(argv []string, termination completionCommandTermination, duration time.Duration, result commandrunner.Result, diagnostic string) []CommandCapture {
	summary := completionCommandSummary{
		Argv:             argv,
		Termination:      termination,
		DurationMS:       duration.Milliseconds(),
		StdoutBytes:      len(result.Stdout),
		StdoutSHA256:     sha256Bytes(result.Stdout),
		StderrBytes:      len(result.Stderr),
		StderrSHA256:     sha256Bytes([]byte(result.Stderr)),
		StreamsTruncated: result.StreamsHeld,
		Diagnostic:       boundedDiagnostic(diagnostic),
	}
	if termination == completionCommandExited && result.ExitCode >= 0 {
		exitCode := result.ExitCode
		summary.ExitCode = &exitCode
	}
	resultJSON := marshalSummary(summary)
	captures := []CommandCapture{{
		StoredPath: completionCommandStdoutStoredPath,
		Data:       result.Stdout,
		Result:     resultJSON,
	}}
	if len(result.Stderr) > 0 {
		captures = append(captures, CommandCapture{
			StoredPath: completionCommandStderrStoredPath,
			Data:       []byte(result.Stderr),
		})
	}
	return captures
}

// marshalSummary encodes the run summary for the evidence entry, within the
// bound one evidence entry may carry. The summary is provenance, never a
// workflow decision. An encoding failure, or a run whose argv alone would pass
// the bound, drops provenance rather than failing the check.
func marshalSummary(summary completionCommandSummary) json.RawMessage {
	encoded, err := json.Marshal(summary)
	if err != nil {
		return nil
	}
	if len(encoded) <= maxEvidenceResultBytes {
		return encoded
	}
	summary.Argv = nil
	summary.ArgvDropped = true
	encoded, err = json.Marshal(summary)
	if err != nil || len(encoded) > maxEvidenceResultBytes {
		return nil
	}
	return encoded
}

// commandDiagnostic renders a check's report as a bounded, single-line excerpt
// for the persisted evidence summary. It uses the check's stderr, or its stdout
// when the check explained nothing on stderr. It never reaches the supervisor's
// log.
func commandDiagnostic(stderr string, stdout []byte) string {
	text := strings.TrimSpace(stderr)
	if text == "" {
		text = strings.TrimSpace(string(stdout))
	}
	if text == "" {
		return ""
	}
	text = strings.ReplaceAll(text, "\n", " | ")
	if len(text) > completionCommandDiagnosticBytes {
		text = strings.ToValidUTF8(text[:completionCommandDiagnosticBytes], "") + "…"
	}
	return ": " + text
}

// boundedDiagnostic keeps the summary's diagnostic within its bound, so the
// evidence adds no unbounded text to a snapshot.
func boundedDiagnostic(diagnostic string) string {
	if len(diagnostic) > completionCommandDiagnosticBytes {
		return strings.ToValidUTF8(diagnostic[:completionCommandDiagnosticBytes], "") + "…"
	}
	return diagnostic
}
