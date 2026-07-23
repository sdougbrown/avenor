package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

func fakeClient() (*client, *io.PipeWriter, *io.PipeReader) {
	clientInR, clientInW := io.Pipe()
	clientOutR, clientOutW := io.Pipe()
	errR, errW := io.Pipe()
	_ = errW.Close()

	c := newClient(nil, clientInW, clientOutR, errR)
	return c, clientOutW, clientInR
}

func writeLine(w io.Writer, v any) {
	b, _ := json.Marshal(v)
	b = append(b, '\n')
	_, _ = w.Write(b)
}

func readCommand(r io.Reader) (map[string]any, error) {
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		return nil, io.EOF
	}
	var cmd map[string]any
	if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
		return cmd, err
	}
	return cmd, nil
}

func TestClientCommandResponse(t *testing.T) {
	c, wOut, rIn := fakeClient()
	defer c.Close()

	done := make(chan error, 1)
	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			done <- err
			return
		}
		id, _ := cmd["id"].(string)
		writeLine(wOut, map[string]any{
			"type":    "response",
			"success": true,
			"id":      id,
		})
		done <- nil
	}()

	result, err := c.sendCommand(context.Background(), map[string]any{"type": "get_state"})
	if err != nil {
		t.Fatalf("sendCommand error: %v", err)
	}

	select {
	case goroutineErr := <-done:
		if goroutineErr != nil {
			t.Fatalf("goroutine error: %v", goroutineErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for goroutine to process command")
	}

	var resp map[string]any
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if resp["type"] != "response" {
		t.Errorf("type = %v, want response", resp["type"])
	}
}

func TestClientNotificationRouting(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi_test")
	sub := make(chan events.Event, 4)
	c.subscribe("pi_test", sub)

	go func() {
		writeLine(wOut, map[string]any{
			"type": "turn_start",
		})
	}()

	select {
	case ev := <-sub:
		if ev.Event != "avenor.turn.start" {
			t.Errorf("event = %q, want avenor.turn.start", ev.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for notification event")
	}
}

func TestClientMalformedJSON(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	go func() {
		_, _ = wOut.Write([]byte("not json\n"))
		writeLine(wOut, map[string]any{
			"type": "turn_start",
		})
	}()

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "avenor.turn.start" {
			t.Errorf("expected turn.start after malformed line, got %q", ev.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event after malformed line")
	}
	if !strings.Contains(c.Stderr(), "dropped malformed JSON line") {
		t.Fatalf("stderr = %q, want malformed JSON diagnostic", c.Stderr())
	}
}

func TestClientInvalidJSON(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	go func() {
		_, _ = wOut.Write([]byte(`{"invalid:`))
		_, _ = wOut.Write([]byte("\n"))
		writeLine(wOut, map[string]any{
			"type": "turn_start",
		})
	}()

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "avenor.turn.start" {
			t.Errorf("expected turn.start after truncated JSON, got %q", ev.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event after truncated JSON")
	}
}

func TestClientEmptyLines(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi_empty")
	sub := make(chan events.Event, 4)
	c.subscribe("pi_empty", sub)

	go func() {
		_, _ = wOut.Write([]byte("\n\n"))
		writeLine(wOut, map[string]any{
			"type": "turn_start",
		})
	}()

	select {
	case ev := <-sub:
		if ev.Event != "avenor.turn.start" {
			t.Errorf("event = %q, want avenor.turn.start", ev.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event after empty lines")
	}
}

func TestClientExtensionUIRouting(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi_perm")
	sub := make(chan events.Event, 4)
	c.subscribe("pi_perm", sub)

	go func() {
		writeLine(wOut, map[string]any{
			"type":    "extension_ui_request",
			"id":      "ui-1",
			"method":  "select",
			"title":   "Allow command?",
			"options": []string{"Allow", "Deny"},
		})
	}()

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "permission.request" {
			t.Errorf("event = %q, want permission.request", ev.Event)
		}
		kind, _ := ev.Fields["kind"].(string)
		if kind != "command" {
			t.Errorf("kind = %q, want command", kind)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for extension UI event")
	}
}

func TestClientAnswerExtensionUI(t *testing.T) {
	c, _, rIn := fakeClient()
	defer c.Close()

	c.setSessionID("pi_perm")
	c.mu.Lock()
	c.approvals["req-1"] = pendingApproval{
		id:     "req-1",
		method: "select",
		rawID:  "ui-1",
	}
	c.mu.Unlock()

	done := make(chan map[string]any, 1)
	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		done <- cmd
	}()

	if err := c.answerExtensionUI("req-1", "select", map[string]any{
		"cancelled": false,
		"value":     "Allow",
	}); err != nil {
		t.Fatalf("answerExtensionUI: %v", err)
	}

	select {
	case cmd := <-done:
		if cmd["type"] != "extension_ui_response" {
			t.Errorf("type = %v, want extension_ui_response", cmd["type"])
		}
		if cmd["value"] != "Allow" {
			t.Errorf("value = %v, want Allow", cmd["value"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for answer write")
	}
}

func TestClientAnswerExtensionUICancelled(t *testing.T) {
	c, _, rIn := fakeClient()
	defer c.Close()

	c.setSessionID("pi_perm")
	c.mu.Lock()
	c.approvals["req-cancel"] = pendingApproval{
		id:     "req-cancel",
		method: "select",
		rawID:  "ui-cancel",
	}
	c.mu.Unlock()

	done := make(chan map[string]any, 1)
	go func() {
		cmd, err := readCommand(rIn)
		if err != nil {
			return
		}
		done <- cmd
	}()

	if err := c.answerExtensionUI("req-cancel", "select", map[string]any{
		"cancelled": true,
	}); err != nil {
		t.Fatalf("answerExtensionUI: %v", err)
	}

	select {
	case cmd := <-done:
		if cmd["type"] != "extension_ui_response" {
			t.Errorf("type = %v, want extension_ui_response", cmd["type"])
		}
		if cmd["cancelled"] != true {
			t.Errorf("cancelled = %v, want true", cmd["cancelled"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancelled answer write")
	}
}

func TestClientFanoutRecordsDroppedEvents(t *testing.T) {
	c, _, _ := fakeClient()
	defer c.Close()

	for i := 0; i < cap(c.eventsCh); i++ {
		c.eventsCh <- events.Event{Event: "existing"}
	}

	sub := make(chan events.Event)
	c.subscribe("th_drop", sub)
	c.fanout(&events.Event{Event: "agent.message", SessionID: "th_drop"})

	stderr := c.Stderr()
	if !strings.Contains(stderr, "global event buffer full") {
		t.Fatalf("stderr = %q, want global drop note", stderr)
	}
	if !strings.Contains(stderr, "subscriber buffer full") {
		t.Fatalf("stderr = %q, want subscriber drop note", stderr)
	}
}

func TestStartClientUsesRequestedWorkingDirectory(t *testing.T) {
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })

	var captured *exec.Cmd
	piExecCommandContext = func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		captured = exec.Command("cat")
		return captured
	}

	cwd := t.TempDir()
	client, err := StartClientWithAgentAndDir(context.Background(), "", "", "", "", cwd)
	if err != nil {
		t.Fatalf("StartClientWithAgentAndDir: %v", err)
	}
	defer client.Close()

	if captured == nil {
		t.Fatal("pi command was not created")
	}
	if captured.Dir != cwd {
		t.Errorf("pi command cwd = %q, want %q", captured.Dir, cwd)
	}
}

func TestClientClose(t *testing.T) {
	proc := exec.Command("cat")
	stdin, _ := proc.StdinPipe()
	stdout, _ := proc.StdoutPipe()
	stderr, _ := proc.StderrPipe()
	_ = proc.Start()

	c := newClient(proc, stdin, stdout, stderr)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if proc.ProcessState == nil {
		t.Fatal("process should have exited")
	}
}

func TestClientSubscribeUnsubscribe(t *testing.T) {
	c, _, _ := fakeClient()
	defer c.Close()

	ch := make(chan events.Event, 1)
	c.subscribe("th_sub", ch)
	c.mu.Lock()
	if len(c.subs["th_sub"]) != 1 {
		t.Fatal("sub not registered")
	}
	c.mu.Unlock()

	c.unsubscribe("th_sub", ch)
	c.mu.Lock()
	if len(c.subs["th_sub"]) != 0 {
		t.Fatal("sub not removed")
	}
	c.mu.Unlock()
}

func TestRollingBuffer(t *testing.T) {
	b := newRollingBuffer(3)
	b.Append("line 1")
	b.Append("line 2")
	b.Append("line 3")
	b.Append("line 4")
	b.Append("line 5")

	out := b.String()
	if out != "line 3\nline 4\nline 5\n" {
		t.Errorf("rolling buffer = %q", out)
	}
}

func TestClientAgentEndSessionIdFromLastMessage(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	go func() {
		writeLine(wOut, map[string]any{
			"type": "agent_end",
			"messages": []any{
				map[string]any{
					"sessionId":   "pi-msg-session",
					"stop_reason": "end_of_turn",
				},
			},
		})
	}()

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "session.end" {
			t.Errorf("event = %q, want session.end", ev.Event)
		}
		if ev.SessionID != "pi-msg-session" {
			t.Errorf("sessionID = %q, want pi-msg-session", ev.SessionID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent_end event")
	}
}

func TestClientExtensionUIDialogStoresFields(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi-dialog-ses")

	go func() {
		writeLine(wOut, map[string]any{
			"type":    "extension_ui_request",
			"id":      "ui-dialog-1",
			"method":  "confirm",
			"title":   "Confirm action?",
			"message": "Proceed?",
		})
	}()

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "permission.request" {
			t.Fatalf("event = %q, want permission.request", ev.Event)
		}
		reqID, ok := ev.Fields["request_id"].(string)
		if !ok || reqID != "ui-dialog-1" {
			t.Errorf("request_id = %v, want ui-dialog-1", reqID)
		}
		uiReqID, ok := ev.Fields["ui_request_id"].(string)
		if !ok || uiReqID != "ui-dialog-1" {
			t.Errorf("ui_request_id = %v, want ui-dialog-1", uiReqID)
		}

		c.mu.Lock()
		approval, ok := c.approvals["ui-dialog-1"]
		c.mu.Unlock()
		if !ok {
			t.Fatal("approval not stored for dialog request")
		}
		if approval.sessionID != "pi-dialog-ses" {
			t.Errorf("pendingApproval.sessionID = %q, want pi-dialog-ses", approval.sessionID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for extension UI event")
	}
}

func TestClientCloseUnblocksPending(t *testing.T) {
	c, _, _ := fakeClient()

	done := make(chan error, 1)
	go func() {
		_, err := c.sendCommand(context.Background(), map[string]any{"type": "get_state"})
		done <- err
	}()

	select {
	case <-done:
		// sendCommand may return before Close if response arrives, but we sent nothing.
		// Give it a moment, then close.
	case <-time.After(50 * time.Millisecond):
	}

	go func() {
		_ = c.Close()
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from sendCommand after close")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sendCommand to unblock after close")
	}
}

func TestCRLFStripping(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi_crlf")

	go func() {
		_, _ = wOut.Write([]byte("{\"type\":\"turn_start\"}\r\n"))
	}()

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "avenor.turn.start" {
			t.Errorf("event = %q, want avenor.turn.start", ev.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CRLF-stripped event")
	}
}
