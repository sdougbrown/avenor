package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type awaitRPCRequest struct {
	ID     any            `json:"id"`
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

type awaitSocketServer struct {
	listener net.Listener
	statuses []map[string]any
	events   []map[string]any // replayed after subscribe; never sent live later.
	result   string
	closeSub bool
	list     []map[string]any

	mu       sync.Mutex
	requests []awaitRPCRequest
	statusAt int
	invalid  error
}

func newAwaitSocketServer(t *testing.T, statuses []map[string]any, events []map[string]any) *awaitSocketServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	s := &awaitSocketServer{
		listener: listener,
		statuses: statuses,
		events:   events,
		list:     []map[string]any{{"runtime_id": "rt_1", "run_id": "run_1", "label": "label_1"}},
	}
	go s.serve()
	t.Cleanup(func() {
		_ = listener.Close()
		s.assertValid(t)
	})
	return s
}

func (s *awaitSocketServer) socket() string { return s.listener.Addr().String() }

func (s *awaitSocketServer) serve() {
	conn, err := s.listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	for {
		var req awaitRPCRequest
		if err := decoder.Decode(&req); err != nil {
			return
		}
		s.mu.Lock()
		s.requests = append(s.requests, req)
		s.mu.Unlock()

		if err := s.validate(req); err != nil {
			s.recordInvalid(err)
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32602, "message": err.Error()}})
			continue
		}

		var result any = map[string]any{}
		switch req.Method {
		case "list":
			result = s.list
		case "status":
			s.mu.Lock()
			at := s.statusAt
			s.statusAt++
			s.mu.Unlock()
			if len(s.statuses) > 0 {
				if at >= len(s.statuses) {
					at = len(s.statuses) - 1
				}
				result = s.statuses[at]
			}
		case "history":
			result = map[string]any{"runtime_id": "rt_1", "events": []any{}, "latest_seq": 9}
		case "subscribe":
			result = map[string]any{"subscribed": true}
		case "result":
			result = map[string]any{"final_output": s.result}
		}
		if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
			return
		}
		if req.Method == "subscribe" {
			afterSeq, _ := runtimeSequence(req.Params["after_seq"])
			for _, event := range s.events {
				// These are deterministic replay records for the event inserted
				// after history's latest_seq and before this subscription.
				if sequence, ok := runtimeSequence(event["seq"]); !ok || sequence <= afterSeq {
					continue
				}
				if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "event", "params": event}); err != nil {
					return
				}
			}
			if s.closeSub {
				return
			}
		}
	}
}

func runtimeSequence(value any) (int64, bool) {
	switch sequence := value.(type) {
	case int64:
		return sequence, true
	case int:
		return int64(sequence), true
	case float64:
		return int64(sequence), sequence == float64(int64(sequence))
	default:
		return 0, false
	}
}

func (s *awaitSocketServer) validate(req awaitRPCRequest) error {
	switch req.Method {
	case "status", "history", "subscribe", "result":
		if req.Params["runtime_id"] != "rt_1" {
			return fmt.Errorf("%s runtime_id = %#v, want rt_1", req.Method, req.Params["runtime_id"])
		}
	}
	if req.Method == "history" && req.Params["limit"] != float64(1) {
		return fmt.Errorf("history limit = %#v, want 1", req.Params["limit"])
	}
	if req.Method == "subscribe" && req.Params["replay"] != true {
		return fmt.Errorf("subscribe replay = %#v, want true", req.Params["replay"])
	}
	return nil
}

func (s *awaitSocketServer) recordInvalid(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.invalid == nil {
		s.invalid = err
	}
}

func (s *awaitSocketServer) assertValid(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.invalid != nil {
		t.Error(s.invalid)
	}
}

func (s *awaitSocketServer) calls() ([]string, map[string]map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	methods := make([]string, len(s.requests))
	params := make(map[string]map[string]any, len(s.requests))
	for i, request := range s.requests {
		methods[i] = request.Method
		params[request.Method] = request.Params
	}
	return methods, params
}

func awaitStatus(status, phase string, pending bool) map[string]any {
	return map[string]any{
		"runtime_id":         "rt_1",
		"status":             status,
		"phase":              phase,
		"pending_permission": pending,
	}
}

func awaitEvent(event, runtimeID string, seq int64) map[string]any {
	return map[string]any{"event": event, "runtime_id": runtimeID, "seq": seq}
}

func awaitEndedStatus(exitCode int) map[string]any {
	status := awaitStatus("ended", "done", false)
	status["exit_code"] = exitCode
	return status
}

func TestAwaitTerminalBeforeAttachSnapshotExitConditions(t *testing.T) {
	tests := []struct {
		name   string
		status map[string]any
		code   int
		want   string
	}{
		{name: "done", status: awaitStatus("idle", "done", false), code: 0, want: "TURN-DONE rt_1\n"},
		{name: "ended exit 0", status: awaitEndedStatus(0), code: 0, want: "TURN-DONE rt_1\n"},
		{name: "ended exit 1", status: awaitEndedStatus(1), code: 10, want: "END failed rt_1\n"},
		{name: "ended exit 124", status: awaitEndedStatus(124), code: 11, want: "END timeout rt_1\n"},
		{name: "ended exit 130", status: awaitEndedStatus(130), code: 12, want: "END killed rt_1\n"},
		{name: "failed", status: awaitStatus("idle", "failed", false), code: 10, want: "END failed rt_1\n"},
		{name: "phase timeout", status: awaitStatus("idle", "timeout", false), code: 11, want: "END timeout rt_1\n"},
		{name: "killed", status: awaitStatus("idle", "killed", false), code: 12, want: "END killed rt_1\n"},
		{name: "permission", status: func() map[string]any {
			s := awaitStatus("running", "waiting", true)
			s["permission"] = map[string]any{"tool": "bash", "summary": "run command"}
			return s
		}(), code: 20, want: "ATTENTION permission rt_1 run command\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newAwaitSocketServer(t, []map[string]any{tt.status}, nil)
			var stdout, stderr bytes.Buffer
			if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != tt.code {
				t.Fatalf("exit code = %d, want %d (stderr %q)", got, tt.code, stderr.String())
			}
			if got := stdout.String(); got != tt.want {
				t.Fatalf("stdout = %q, want %q", got, tt.want)
			}
			methods, _ := server.calls()
			if want := []string{"list", "status"}; !reflect.DeepEqual(methods, want) {
				t.Fatalf("calls = %v, want immediate snapshot exit %v", methods, want)
			}
		})
	}
}

func TestAwaitDoneWaitsThroughPermissionAndAttachesRaceSafely(t *testing.T) {
	gate := awaitStatus("running", "waiting", true)
	gate["permission"] = map[string]any{"question": "Allow write?"}
	server := newAwaitSocketServer(t,
		[]map[string]any{gate, awaitStatus("running", "working", false), awaitStatus("idle", "done", false)},
		[]map[string]any{awaitEvent("permission.response", "rt_1", 10), awaitEvent("session.end", "rt_1", 11)},
	)
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"label_1", "--socket", server.socket(), "--until", "done"}, &stdout, &stderr); got != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", got, stderr.String())
	}
	if want := "ATTENTION permission rt_1 Allow write?\nTURN-DONE rt_1\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	methods, params := server.calls()
	if want := []string{"list", "status", "history", "subscribe", "status", "status"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("calls = %v, want %v", methods, want)
	}
	subscribe := params["subscribe"]
	if subscribe["runtime_id"] != "rt_1" || subscribe["replay"] != true || subscribe["after_seq"] != float64(9) {
		t.Fatalf("subscribe params = %#v, want runtime replay cursor", subscribe)
	}
}

func TestAwaitGateBeforeAttachExitsFromSnapshot(t *testing.T) {
	gate := awaitStatus("running", "waiting", true)
	gate["permission"] = map[string]any{"tool": "exec"}
	server := newAwaitSocketServer(t, []map[string]any{gate}, nil)
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != 20 {
		t.Fatalf("exit code = %d, want 20 (stderr %q)", got, stderr.String())
	}
	if stdout.String() != "ATTENTION permission rt_1 exec\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if calls, _ := server.calls(); !reflect.DeepEqual(calls, []string{"list", "status"}) {
		t.Fatalf("gate snapshot unexpectedly subscribed: %v", calls)
	}
}

func TestAwaitDefaultUntilAttentionFromReplayedPermission(t *testing.T) {
	gate := awaitStatus("running", "waiting", true)
	gate["permission"] = map[string]any{"description": "Allow deployment to staging?", "tool": "deploy"}
	server := newAwaitSocketServer(t,
		[]map[string]any{
			awaitStatus("running", "working", false),
			awaitStatus("running", "working", false),
			gate,
		},
		[]map[string]any{awaitEvent("permission.request", "rt_1", 10)},
	)
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != awaitExitPendingPermission {
		t.Fatalf("exit code = %d, want %d (stderr %q)", got, awaitExitPendingPermission, stderr.String())
	}
	if want := "ATTENTION permission rt_1 Allow deployment to staging?\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if calls, params := server.calls(); !reflect.DeepEqual(calls, []string{"list", "status", "history", "subscribe", "status", "status"}) {
		t.Fatalf("calls = %v, want replay resnapshot %v", calls, []string{"list", "status", "history", "subscribe", "status", "status"})
	} else if params["subscribe"]["after_seq"] != float64(9) {
		t.Fatalf("subscribe params = %#v, want replay after history latest_seq", params["subscribe"])
	}
}

func TestAwaitEndReasonUsesStopReason(t *testing.T) {
	status := awaitStatus("idle", "failed", false)
	status["phase_label"] = "implementation activity"
	status["stop_reason"] = "process exited"
	server := newAwaitSocketServer(t, []map[string]any{status}, nil)
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != awaitExitFailed {
		t.Fatalf("exit code = %d, want %d (stderr %q)", got, awaitExitFailed, stderr.String())
	}
	if want := "END failed rt_1 process exited\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestAwaitPrintOutputIsLossless(t *testing.T) {
	server := newAwaitSocketServer(t, []map[string]any{awaitStatus("idle", "done", false)}, nil)
	server.result = "first line\nsecond line\nno newline after this"
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"run_1", "--socket", server.socket(), "--print-output"}, &stdout, &stderr); got != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", got, stderr.String())
	}
	want := "TURN-DONE rt_1\n---\n" + server.result
	if stdout.String() != want {
		t.Fatalf("output was not byte-for-byte lossless\n got: %q\nwant: %q", stdout.String(), want)
	}
}

func TestAwaitJSONOutput(t *testing.T) {
	server := newAwaitSocketServer(t, []map[string]any{awaitStatus("idle", "done", false)}, nil)
	server.result = "complete\noutput"
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"--format=json", "run_1", "--socket", server.socket(), "--print-output"}, &stdout, &stderr); got != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", got, stderr.String())
	}
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(stdout.Bytes()), []byte{'\n'}) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("invalid NDJSON %q: %v", line, err)
		}
		records = append(records, record)
	}
	want := []map[string]any{{"event": "turn_done", "runtime_id": "rt_1"}, {"final_output": "complete\noutput"}}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("json records = %#v, want %#v", records, want)
	}
}

func TestAwaitCleanupRaceSettlesFromReplayWithoutAnotherEvent(t *testing.T) {
	// session.end is replayed from the history/subscribe gap. Cleanup remains
	// running/done across more than three polls and emits no later event.
	server := newAwaitSocketServer(t,
		[]map[string]any{
			awaitStatus("running", "working", false),
			awaitStatus("running", "working", false),
			awaitStatus("running", "done", false),
			awaitStatus("running", "done", false),
			awaitStatus("running", "done", false),
			awaitStatus("running", "done", false),
			awaitStatus("running", "done", false),
			awaitStatus("idle", "done", false),
		},
		[]map[string]any{awaitEvent("session.end", "rt_1", 10)},
	)
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", got, stderr.String())
	}
	if stdout.String() != "TURN-DONE rt_1\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if calls, params := server.calls(); !reflect.DeepEqual(calls, []string{"list", "status", "history", "subscribe", "status", "status", "status", "status", "status", "status", "status"}) {
		t.Fatalf("calls = %v, want replay resnapshot and cleanup polls", calls)
	} else if params["subscribe"]["after_seq"] != float64(9) {
		t.Fatalf("subscribe params = %#v, want replay after history latest_seq", params["subscribe"])
	}
}

func TestAwaitWrongRuntimeEventIsIgnored(t *testing.T) {
	// The matching event provides a deterministic completion gate after the
	// wrong-runtime event. Only the matching event may cause the third status
	// request (the first two are the attachment snapshots).
	server := newAwaitSocketServer(t,
		[]map[string]any{awaitStatus("running", "working", false), awaitStatus("running", "working", false), awaitStatus("idle", "done", false)},
		[]map[string]any{awaitEvent("session.end", "rt_other", 10), awaitEvent("session.end", "rt_1", 11)},
	)
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != 0 {
		t.Fatalf("exit code = %d, want completion from matching event (stderr %q, stdout %q)", got, stderr.String(), stdout.String())
	}
	calls, _ := server.calls()
	statusCalls := 0
	for _, call := range calls {
		if call == "status" {
			statusCalls++
		}
	}
	if statusCalls != 3 {
		t.Fatalf("status calls = %d, want 3; wrong-runtime event must not resnapshot (calls %v)", statusCalls, calls)
	}
}

func TestAwaitRejectsMalformedStatusResponse(t *testing.T) {
	tests := []struct {
		name   string
		status map[string]any
	}{
		{
			name:   "mismatched runtime ID",
			status: map[string]any{"runtime_id": "rt_other", "status": "running", "phase": "working"},
		},
		{
			name:   "missing status",
			status: map[string]any{"runtime_id": "rt_1", "phase": "working"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newAwaitSocketServer(t, []map[string]any{tt.status}, nil)
			var stdout, stderr bytes.Buffer
			if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != 2 {
				t.Fatalf("exit code = %d, want 2 (stderr %q)", got, stderr.String())
			}
			if !bytes.Contains(stderr.Bytes(), []byte("protocol error")) {
				t.Fatalf("stderr = %q, want protocol error", stderr.String())
			}
			if calls, _ := server.calls(); !reflect.DeepEqual(calls, []string{"list", "status"}) {
				t.Fatalf("calls = %v, malformed status must stop attachment", calls)
			}
		})
	}
}

func TestAwaitRetryAndLaggedEventsKeepWaitingThenResnapshot(t *testing.T) {
	// A retry can publish session.end before its next attempt starts; active
	// status must not be mistaken for a completed turn. A lag notification then
	// forces the authoritative snapshot that observes the eventual terminal.
	server := newAwaitSocketServer(t,
		[]map[string]any{
			awaitStatus("running", "working", false), // initial snapshot
			awaitStatus("running", "working", false), // post-subscribe snapshot
			awaitStatus("running", "done", false),    // replay session.end
			awaitStatus("running", "working", false), // retry leaves cleanup race
			awaitStatus("idle", "done", false),       // lag resnapshot
		},
		[]map[string]any{awaitEvent("session.end", "rt_1", 10), awaitEvent("subscriber.lagged", "rt_1", 11)},
	)
	var stdout, stderr bytes.Buffer
	if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", got, stderr.String())
	}
	if stdout.String() != "TURN-DONE rt_1\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if calls, _ := server.calls(); !reflect.DeepEqual(calls, []string{"list", "status", "history", "subscribe", "status", "status", "status", "status"}) {
		t.Fatalf("calls = %v, want post-subscribe, retry, cleanup, and lag resnapshots", calls)
	}
}

func TestAwaitTimeoutAndConnectionLoss(t *testing.T) {
	t.Run("wall timeout", func(t *testing.T) {
		server := newAwaitSocketServer(t, []map[string]any{awaitStatus("running", "working", false)}, nil)
		var stdout, stderr bytes.Buffer
		if got := runAwaitTo([]string{"run_1", "--socket", server.socket(), "--timeout", "10ms"}, &stdout, &stderr); got != awaitExitWallTimeout {
			t.Fatalf("exit code = %d, want %d (stderr %q)", got, awaitExitWallTimeout, stderr.String())
		}
	})
	t.Run("supervisor EOF", func(t *testing.T) {
		server := newAwaitSocketServer(t, []map[string]any{awaitStatus("running", "working", false)}, nil)
		server.closeSub = true
		var stdout, stderr bytes.Buffer
		if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != 2 {
			t.Fatalf("exit code = %d, want 2 (stderr %q)", got, stderr.String())
		}
		if !bytes.Contains(stderr.Bytes(), []byte("supervisor connection lost")) {
			t.Fatalf("stderr = %q, want connection-loss message", stderr.String())
		}
	})
}

func TestAwaitUsageNotFoundAndDeadSocket(t *testing.T) {
	t.Run("usage", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		if got := runAwaitTo([]string{"run_1"}, &stdout, &stderr); got != 2 {
			t.Fatalf("exit code = %d, want 2", got)
		}
	})
	t.Run("not found", func(t *testing.T) {
		server := newAwaitSocketServer(t, nil, nil)
		server.list = []map[string]any{{"runtime_id": "other", "run_id": "other"}}
		var stdout, stderr bytes.Buffer
		if got := runAwaitTo([]string{"run_1", "--socket", server.socket()}, &stdout, &stderr); got != 2 {
			t.Fatalf("exit code = %d, want 2", got)
		}
	})
	t.Run("dead socket", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		if got := runAwaitTo([]string{"run_1", "--socket", filepath.Join(t.TempDir(), "missing.sock")}, &stdout, &stderr); got != 2 {
			t.Fatalf("exit code = %d, want 2", got)
		}
	})
}

func TestAwaitTimeoutDoesNotWaitForClientCallDeadline(t *testing.T) {
	// A listener that accepts but never replies verifies --timeout bounds RPC
	// attachment too, rather than inheriting client.Call's 30-second deadline.
	path := filepath.Join(t.TempDir(), "silent.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	stopServer := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		<-stopServer
	}()
	t.Cleanup(func() {
		close(stopServer)
		_ = listener.Close()
		<-serverDone
	})
	var stdout, stderr bytes.Buffer
	start := time.Now()
	if got := runAwaitTo([]string{"run_1", "--socket", path, "--timeout", "20ms"}, &stdout, &stderr); got != awaitExitWallTimeout {
		t.Fatalf("exit code = %d, want %d (stderr %q)", got, awaitExitWallTimeout, stderr.String())
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %v, expected wall-clock bound", elapsed)
	}
}
