package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/runstate"
)

const (
	awaitExitFailed            = 10
	awaitExitPhaseTimeout      = 11
	awaitExitKilled            = 12
	awaitExitPendingPermission = 20
	awaitExitWallTimeout       = 124
)

type awaitOptions struct {
	socket      string
	until       string
	timeout     time.Duration
	printOutput bool
	format      string
	target      string
}

// runAwait implements the socket-backed `avenor await` command.
func runAwait(args []string) int {
	return runAwaitTo(args, os.Stdout, os.Stderr)
}

// runAwaitTo is the testable form of runAwait.
func runAwaitTo(args []string, stdout, stderr io.Writer) int {
	opts, code := parseAwaitArgs(args, stderr)
	if code != 0 {
		return code
	}

	var ctx context.Context = context.Background()
	cancel := func() {}
	if opts.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.timeout)
	}
	defer cancel()

	resolved, err := awaitResolveTarget(ctx, stderr, opts.socket, opts.target)
	if err != nil {
		return awaitCallError(stderr, err)
	}
	c := resolved.client
	defer c.Close()
	runtimeID := resolved.runtimeID

	out := newAwaitOutput(stdout, opts.format, runtimeID)
	var machine runstate.Machine
	var previous runstate.State

	// A snapshot is deliberately first. It is authoritative for both terminal
	// and permission-gated runs which existed before this client attached.
	snapshot, err := fetchAwaitStatus(ctx, c, runtimeID)
	if err != nil {
		return awaitCallError(stderr, err)
	}
	decision := machine.ObserveSnapshot(snapshot.state)
	previous = emitAwaitTransition(out, previous, decision.State, snapshot)
	if awaitShouldExit(decision.State, opts.until) {
		return finishAwait(ctx, c, out, stderr, runtimeID, opts.printOutput, decision.State)
	}

	// status has no cursor. Read history only after the snapshot, then subscribe
	// with its cursor on this same connection to close the attachment race.
	var history struct {
		LatestSeq int64 `json:"latest_seq"`
	}
	if err := awaitCall(ctx, c, func() error {
		return c.Call("history", map[string]any{"runtime_id": runtimeID, "limit": 1}, &history)
	}); err != nil {
		return awaitCallError(stderr, err)
	}
	if err := awaitCall(ctx, c, func() error {
		return c.Call("subscribe", map[string]any{
			"runtime_id": runtimeID,
			"replay":     true,
			"after_seq":  history.LatestSeq,
		}, nil)
	}); err != nil {
		return awaitCallError(stderr, err)
	}

	// A second authoritative snapshot immediately after subscribing covers a
	// transition that lands between the first snapshot and the subscription.
	snapshot, decision, previous, err = resnapshotAwait(ctx, c, runtimeID, &machine, out, previous)
	if err != nil {
		return awaitCallError(stderr, err)
	}
	snapshot, decision, previous, err = settleAwaitCleanup(ctx, c, runtimeID, &machine, out, previous, snapshot, decision)
	if err != nil {
		return awaitCallError(stderr, err)
	}
	if awaitShouldExit(decision.State, opts.until) {
		return finishAwait(ctx, c, out, stderr, runtimeID, opts.printOutput, decision.State)
	}

	events := c.Events()
	for {
		select {
		case <-ctx.Done():
			_ = c.Close() // unblock an in-flight RPC if one was started below
			fmt.Fprintln(stderr, "avenor await: timeout elapsed")
			return awaitExitWallTimeout
		case event, ok := <-events:
			if !ok {
				fmt.Fprintln(stderr, "avenor await: supervisor connection lost")
				return 2
			}
			// The server normally scopes subscriptions, but do not let a stray
			// notification for another runtime wake this run.
			if event.RuntimeID != "" && event.RuntimeID != runtimeID {
				continue
			}
			decision = machine.ObserveEvent(event.Event)
			if decision.Action != runstate.ActionResnapshot {
				continue
			}
			snapshot, decision, previous, err = resnapshotAwait(ctx, c, runtimeID, &machine, out, previous)
			if err != nil {
				return awaitCallError(stderr, err)
			}
			snapshot, decision, previous, err = settleAwaitCleanup(ctx, c, runtimeID, &machine, out, previous, snapshot, decision)
			if err != nil {
				return awaitCallError(stderr, err)
			}
			if awaitShouldExit(decision.State, opts.until) {
				return finishAwait(ctx, c, out, stderr, runtimeID, opts.printOutput, decision.State)
			}
		}
	}
}

func parseAwaitArgs(args []string, stderr io.Writer) (awaitOptions, int) {
	fs := flag.NewFlagSet("avenor await", flag.ContinueOnError)
	fs.SetOutput(stderr)
	socket := fs.String("socket", "", "explicit control socket path; omitting scans ~/.avenor/sockets")
	until := fs.String("until", "attention", "wait until: attention or done")
	timeout := fs.Duration("timeout", 0, "wall-clock timeout")
	printOutput := fs.Bool("print-output", false, "print complete result output before exiting")
	format := fs.String("format", "plain", "output format: plain or json")

	// flag.FlagSet stops at a positional argument. The documented command puts
	// the run target first, so split the one target from known flag arguments.
	var flagArgs []string
	var target string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--socket" || arg == "--until" || arg == "--timeout" || arg == "--format" {
			if i+1 >= len(args) {
				fmt.Fprintf(stderr, "avenor await: %s requires a value\n", arg)
				return awaitOptions{}, 2
			}
			flagArgs = append(flagArgs, arg, args[i+1])
			i++
			continue
		}
		if strings.HasPrefix(arg, "--socket=") || strings.HasPrefix(arg, "--until=") || strings.HasPrefix(arg, "--timeout=") || strings.HasPrefix(arg, "--format=") || arg == "--print-output" || strings.HasPrefix(arg, "-") {
			flagArgs = append(flagArgs, arg)
			continue
		}
		if target != "" {
			fmt.Fprintln(stderr, "avenor await: expected exactly one <run-id|label>")
			return awaitOptions{}, 2
		}
		target = arg
	}
	if err := fs.Parse(flagArgs); err != nil {
		return awaitOptions{}, 2
	}
	if target == "" {
		fmt.Fprintln(stderr, "avenor await: <run-id|label> is required")
		return awaitOptions{}, 2
	}
	if *until != "attention" && *until != "done" {
		fmt.Fprintf(stderr, "avenor await: unsupported --until %q\n", *until)
		return awaitOptions{}, 2
	}
	if *timeout < 0 {
		fmt.Fprintln(stderr, "avenor await: --timeout must not be negative")
		return awaitOptions{}, 2
	}
	if *format != "plain" && *format != "json" {
		fmt.Fprintf(stderr, "avenor await: unsupported --format %q\n", *format)
		return awaitOptions{}, 2
	}
	return awaitOptions{socket: *socket, until: *until, timeout: *timeout, printOutput: *printOutput, format: *format, target: target}, 0
}

func awaitList(ctx context.Context, c *client.Client) ([]map[string]any, error) {
	var runtimes []map[string]any
	err := awaitCall(ctx, c, func() error {
		var err error
		runtimes, err = c.List()
		return err
	})
	return runtimes, err
}

type awaitRuntimeResolution struct {
	runtimeID string
	runID     string
	startedAt time.Time
	exact     bool
	losers    []string
}

type awaitResolvedTarget struct {
	client      *client.Client
	runtimeID   string
	targetRunID string
}

func awaitResolveTarget(ctx context.Context, stderr io.Writer, socket, target string) (awaitResolvedTarget, error) {
	if socket != "" {
		c, err := client.Dial(socket)
		if err != nil {
			return awaitResolvedTarget{}, err
		}
		runtimes, err := awaitList(ctx, c)
		if err != nil {
			_ = c.Close()
			return awaitResolvedTarget{}, err
		}
		resolution := resolveAwaitRuntime(target, runtimes)
		for _, loser := range resolution.losers {
			fmt.Fprintf(stderr, "avenor await: label collision: losing run_id %s to winner run_id %s\n", loser, resolution.runID)
		}
		if !resolution.exact && resolution.runtimeID == "" {
			_ = c.Close()
			return awaitResolvedTarget{}, fmt.Errorf("run %q not found", target)
		}
		return awaitResolvedTarget{client: c, runtimeID: resolution.runtimeID, targetRunID: resolution.runID}, nil
	}
	return awaitResolveTargetFromSockets(ctx, stderr, target)
}

func awaitResolveTargetFromSockets(ctx context.Context, stderr io.Writer, target string) (awaitResolvedTarget, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return awaitResolvedTarget{}, err
	}
	dir := filepath.Join(home, ".avenor", "sockets")
	entries, err := os.ReadDir(dir)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return awaitResolvedTarget{}, ctxErr
	}
	if err != nil {
		return awaitResolvedTarget{}, err
	}
	var best *awaitResolvedTarget
	var bestStartedAt time.Time
	for _, entry := range entries {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if best != nil {
				_ = best.client.Close()
			}
			return awaitResolvedTarget{}, ctxErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sock") {
			continue
		}
		c, err := client.Dial(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			_ = c.Close()
			if best != nil {
				_ = best.client.Close()
			}
			return awaitResolvedTarget{}, ctxErr
		}
		runtimes, err := awaitList(ctx, c)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				if best != nil {
					_ = best.client.Close()
				}
				_ = c.Close()
				return awaitResolvedTarget{}, ctx.Err()
			}
			_ = c.Close()
			continue
		}
		resolution := resolveAwaitRuntime(target, runtimes)
		for _, loser := range resolution.losers {
			fmt.Fprintf(stderr, "avenor await: label collision: losing run_id %s to winner run_id %s\n", loser, resolution.runID)
		}
		if resolution.exact {
			if best != nil {
				_ = best.client.Close()
			}
			return awaitResolvedTarget{client: c, runtimeID: resolution.runtimeID, targetRunID: resolution.runID}, nil
		}
		if resolution.runtimeID == "" {
			_ = c.Close()
			continue
		}
		if best == nil || resolution.startedAt.After(bestStartedAt) {
			if best != nil {
				fmt.Fprintf(stderr, "avenor await: label collision: losing run_id %s to winner run_id %s\n", best.targetRunID, resolution.runID)
				_ = best.client.Close()
			}
			best = &awaitResolvedTarget{client: c, runtimeID: resolution.runtimeID, targetRunID: resolution.runID}
			bestStartedAt = resolution.startedAt
			continue
		}
		fmt.Fprintf(stderr, "avenor await: label collision: losing run_id %s to winner run_id %s\n", resolution.runID, best.targetRunID)
		_ = c.Close()
	}
	if best != nil {
		return *best, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return awaitResolvedTarget{}, ctxErr
	}
	return awaitResolvedTarget{}, fmt.Errorf("run %q not found", target)
}

func resolveAwaitRuntime(target string, runtimes []map[string]any) awaitRuntimeResolution {
	for _, runtime := range runtimes {
		if runtimeString(runtime, "run_id") == target {
			if id := runtimeString(runtime, "runtime_id"); id != "" {
				return awaitRuntimeResolution{runtimeID: id, runID: runtimeString(runtime, "run_id"), exact: true}
			}
		}
	}
	var best awaitRuntimeResolution
	for _, runtime := range runtimes {
		if runtimeString(runtime, "label") != target {
			continue
		}
		if id := runtimeString(runtime, "runtime_id"); id != "" {
			startedAt := runtimeStartedAt(runtime["started_at"])
			runID := runtimeString(runtime, "run_id")
			if best.runtimeID == "" || startedAt.After(best.startedAt) {
				if best.runtimeID != "" {
					losers := append([]string{}, best.losers...)
					losers = append(losers, best.runID)
					best = awaitRuntimeResolution{runtimeID: id, runID: runID, startedAt: startedAt, losers: losers}
					continue
				}
				best = awaitRuntimeResolution{runtimeID: id, runID: runID, startedAt: startedAt}
				continue
			}
			best.losers = append(best.losers, runID)
		}
	}
	return best
}

const awaitCleanupPollDelay = 15 * time.Millisecond

type awaitSnapshot struct {
	state      runstate.Snapshot
	permission map[string]any
	reason     string
	rawStatus  string
	rawPhase   string
}

func fetchAwaitStatus(ctx context.Context, c *client.Client, runtimeID string) (awaitSnapshot, error) {
	var raw map[string]any
	if err := awaitCall(ctx, c, func() error {
		var err error
		raw, err = c.Status(runtimeID)
		return err
	}); err != nil {
		return awaitSnapshot{}, err
	}
	if responseRuntimeID := runtimeString(raw, "runtime_id"); responseRuntimeID != "" && responseRuntimeID != runtimeID {
		return awaitSnapshot{}, fmt.Errorf("protocol error: status response runtime_id %q does not match requested runtime_id %q", responseRuntimeID, runtimeID)
	}
	rawStatus := runtimeString(raw, "status")
	if rawStatus == "" {
		return awaitSnapshot{}, errors.New("protocol error: status response is missing status")
	}
	rawPhase := runtimeString(raw, "phase")
	status := rawStatus
	// A stable completed runtime uses status=ended regardless of the process
	// outcome. Preserve Machine as the sole lifecycle decision-maker by
	// normalizing this extra status field into its Snapshot input only.
	if rawStatus == "ended" {
		if exitCode, ok := runtimeExitCode(raw["exit_code"]); ok {
			switch exitCode {
			case 0:
				status = "done"
			case 124:
				status = "timeout"
			case 130:
				status = "killed"
			default:
				status = "failed"
			}
		}
	}
	snapshot := awaitSnapshot{
		state: runstate.Snapshot{
			Status:            status,
			Phase:             rawPhase,
			PendingPermission: runtimeBool(raw, "pending_permission"),
		},
		reason:    runtimeString(raw, "stop_reason"),
		rawStatus: rawStatus,
		rawPhase:  rawPhase,
	}
	if permission, ok := raw["permission"].(map[string]any); ok {
		snapshot.permission = permission
	}
	return snapshot, nil
}

func awaitCall(ctx context.Context, c *client.Client, call func() error) error {
	done := make(chan error, 1)
	go func() { done <- call() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = c.Close()
		return ctx.Err()
	}
}

func awaitCallError(stderr io.Writer, err error) int {
	if err == context.DeadlineExceeded {
		fmt.Fprintln(stderr, "avenor await: timeout elapsed")
		return awaitExitWallTimeout
	}
	if errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection closed") || strings.Contains(err.Error(), "broken pipe") {
		fmt.Fprintln(stderr, "avenor await: supervisor connection lost")
		return 2
	}
	fmt.Fprintf(stderr, "avenor await: %v\n", err)
	return 2
}

func runtimeString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func runtimeBool(values map[string]any, key string) bool {
	value, _ := values[key].(bool)
	return value
}

func runtimeStartedAt(value any) time.Time {
	switch startedAt := value.(type) {
	case int:
		return time.Unix(0, int64(startedAt)*int64(time.Millisecond))
	case int64:
		return time.Unix(0, startedAt*int64(time.Millisecond))
	case float64:
		return time.Unix(0, int64(startedAt)*int64(time.Millisecond))
	case json.Number:
		ms, err := startedAt.Int64()
		if err != nil {
			return time.Time{}
		}
		return time.Unix(0, ms*int64(time.Millisecond))
	default:
		return time.Time{}
	}
}

func runtimeExitCode(value any) (int, bool) {
	switch code := value.(type) {
	case int:
		return code, true
	case int64:
		return int(code), true
	case float64:
		return int(code), code == float64(int(code))
	case json.Number:
		value, err := code.Int64()
		return int(value), err == nil
	default:
		return 0, false
	}
}

// resnapshotAwait observes every status through Machine so the await command
// never makes lifecycle decisions from raw status fields directly.
func resnapshotAwait(ctx context.Context, c *client.Client, runtimeID string, machine *runstate.Machine, out *awaitOutput, previous runstate.State) (awaitSnapshot, runstate.Decision, runstate.State, error) {
	snapshot, err := fetchAwaitStatus(ctx, c, runtimeID)
	if err != nil {
		return awaitSnapshot{}, runstate.Decision{}, previous, err
	}
	decision := machine.ObserveSnapshot(snapshot.state)
	previous = emitAwaitTransition(out, previous, decision.State, snapshot)
	return snapshot, decision, previous, nil
}

// settleAwaitCleanup handles the interval where session.end has set a terminal
// phase but runtime cleanup has not yet changed status from running. No later
// event is emitted for this transition, so polling continues until cleanup
// settles or the caller's wall-clock context expires.
func settleAwaitCleanup(ctx context.Context, c *client.Client, runtimeID string, machine *runstate.Machine, out *awaitOutput, previous runstate.State, snapshot awaitSnapshot, decision runstate.Decision) (awaitSnapshot, runstate.Decision, runstate.State, error) {
	for awaitCleanupTransient(snapshot, decision) {
		timer := time.NewTimer(awaitCleanupPollDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return awaitSnapshot{}, runstate.Decision{}, previous, ctx.Err()
		case <-timer.C:
		}

		var err error
		snapshot, decision, previous, err = resnapshotAwait(ctx, c, runtimeID, machine, out, previous)
		if err != nil {
			return awaitSnapshot{}, runstate.Decision{}, previous, err
		}
	}
	return snapshot, decision, previous, nil
}

func awaitCleanupTransient(snapshot awaitSnapshot, decision runstate.Decision) bool {
	return snapshot.rawStatus == "running" &&
		runstate.IsTerminalStatus(snapshot.rawPhase) &&
		decision.State == runstate.StateActive
}

func awaitShouldExit(state runstate.State, until string) bool {
	if state == runstate.StatePendingPermission {
		return until == "attention"
	}
	return state == runstate.StateDone || state == runstate.StateFailed || state == runstate.StateTimeout || state == runstate.StateKilled
}

type awaitOutput struct {
	writer    *bufio.Writer
	format    string
	runtimeID string
}

func newAwaitOutput(writer io.Writer, format, runtimeID string) *awaitOutput {
	return &awaitOutput{writer: bufio.NewWriter(writer), format: format, runtimeID: runtimeID}
}

// emitAwaitTransition produces at most one record for a state change. Active
// is intentionally silent: it matters only as the reset that lets a later
// permission gate be reported again.
func emitAwaitTransition(out *awaitOutput, previous, state runstate.State, snapshot awaitSnapshot) runstate.State {
	if state == previous || state == runstate.StateActive {
		return state
	}
	switch state {
	case runstate.StatePendingPermission:
		out.attention(permissionSummary(snapshot.permission))
	case runstate.StateDone:
		out.turnDone()
	case runstate.StateFailed, runstate.StateTimeout, runstate.StateKilled:
		out.end(string(state), snapshot.reason)
	}
	return state
}

func permissionSummary(permission map[string]any) string {
	for _, key := range []string{"summary", "question", "description", "message", "tool", "command"} {
		if value, ok := permission[key].(string); ok && value != "" {
			return strings.Join(strings.Fields(value), " ")
		}
	}
	return "permission requested"
}

func (out *awaitOutput) attention(summary string) {
	if out.format == "json" {
		out.json(map[string]any{"event": "attention", "kind": "permission", "runtime_id": out.runtimeID, "summary": summary})
		return
	}
	fmt.Fprintf(out.writer, "ATTENTION permission %s %s\n", out.runtimeID, summary)
	_ = out.writer.Flush()
}

func (out *awaitOutput) turnDone() {
	if out.format == "json" {
		out.json(map[string]any{"event": "turn_done", "runtime_id": out.runtimeID})
		return
	}
	fmt.Fprintf(out.writer, "TURN-DONE %s\n", out.runtimeID)
	_ = out.writer.Flush()
}

func (out *awaitOutput) end(phase, reason string) {
	if out.format == "json" {
		record := map[string]any{"event": "end", "phase": phase, "runtime_id": out.runtimeID}
		if reason != "" {
			record["reason"] = reason
		}
		out.json(record)
		return
	}
	fmt.Fprintf(out.writer, "END %s %s", phase, out.runtimeID)
	if reason != "" {
		fmt.Fprintf(out.writer, " %s", strings.Join(strings.Fields(reason), " "))
	}
	fmt.Fprintln(out.writer)
	_ = out.writer.Flush()
}

func (out *awaitOutput) json(record map[string]any) {
	_ = json.NewEncoder(out.writer).Encode(record)
	_ = out.writer.Flush()
}

func finishAwait(ctx context.Context, c *client.Client, out *awaitOutput, stderr io.Writer, runtimeID string, printOutput bool, state runstate.State) int {
	if printOutput {
		var result map[string]any
		if err := awaitCall(ctx, c, func() error {
			var err error
			result, err = c.Result(runtimeID)
			return err
		}); err != nil {
			return awaitCallError(stderr, err)
		}
		finalOutput := runtimeString(result, "final_output")
		if out.format == "json" {
			out.json(map[string]any{"final_output": finalOutput})
		} else {
			fmt.Fprintln(out.writer, "---")
			_, _ = io.WriteString(out.writer, finalOutput)
			_ = out.writer.Flush()
		}
	}
	switch state {
	case runstate.StateDone:
		return 0
	case runstate.StateFailed:
		return awaitExitFailed
	case runstate.StateTimeout:
		return awaitExitPhaseTimeout
	case runstate.StateKilled:
		return awaitExitKilled
	case runstate.StatePendingPermission:
		return awaitExitPendingPermission
	default:
		return 2
	}
}
