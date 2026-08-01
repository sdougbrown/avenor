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

func TestClientMessageUpdateRoutingCompactsProviderSnapshots(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi_test")
	signature := strings.Repeat("opaque-signature", 1_000)
	updates := []struct {
		delta       string
		partialJSON string
		arguments   string
	}{
		{delta: `{"content":"first`, partialJSON: `{"content":"first`, arguments: "first"},
		{delta: ` second"}`, partialJSON: `{"content":"first second"}`, arguments: "first second"},
	}
	go func() {
		for _, update := range updates {
			toolCall := map[string]any{
				"type":        "toolCall",
				"id":          "call-1",
				"name":        "write",
				"arguments":   map[string]any{"content": update.arguments},
				"partialJson": update.partialJSON,
			}
			partial := map[string]any{"content": []any{
				map[string]any{"type": "thinking", "thinkingSignature": signature},
				toolCall,
			}}
			writeLine(wOut, map[string]any{
				"type":                  "message_update",
				"message":               partial,
				"assistantMessageEvent": map[string]any{"type": "toolcall_delta", "contentIndex": 1, "delta": update.delta, "partial": partial},
			})
		}
	}()

	got := make([]events.Event, 0, len(updates)*2)
	for len(got) < cap(got) {
		select {
		case ev := <-c.eventsCh:
			got = append(got, ev)
		case <-time.After(time.Second):
			t.Fatalf("timed out after %d routed events", len(got))
		}
	}
	for i := range updates {
		canonical, alias := got[i*2], got[i*2+1]
		if canonical.Event != "tool.call_update" || alias.Event != "avenor.message.update" {
			t.Fatalf("update %d events = %+v, want canonical and compact compatibility updates", i, got[i*2:i*2+2])
		}
		if canonical.SessionID != "pi_test" || alias.SessionID != "pi_test" {
			t.Fatalf("update %d session IDs = %q/%q, want pi_test", i, canonical.SessionID, alias.SessionID)
		}
		for _, key := range []string{"message", "partial", "partialJson", "arguments"} {
			if _, ok := canonical.Fields[key]; ok {
				t.Fatalf("update %d canonical event retained %s: %+v", i, key, canonical.Fields)
			}
		}
		if _, ok := alias.Fields["message"]; ok {
			t.Fatalf("update %d alias retained message: %+v", i, alias.Fields)
		}
		assistantEvent, ok := alias.Fields["assistantMessageEvent"].(map[string]any)
		if !ok {
			t.Fatalf("update %d alias missing assistantMessageEvent: %+v", i, alias.Fields)
		}
		for _, key := range []string{"partial", "partialJson", "arguments"} {
			if _, ok := assistantEvent[key]; ok {
				t.Fatalf("update %d compact assistant event retained %s: %+v", i, key, assistantEvent)
			}
		}
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal routed events: %v", err)
	}
	if strings.Contains(string(encoded), signature) || strings.Contains(string(encoded), `"partialJson"`) {
		t.Fatalf("routed events retained cumulative provider snapshot (%d bytes)", len(encoded))
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

func TestStartClientForwardsAgentAndModel(t *testing.T) {
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })

	var captured *exec.Cmd
	piExecCommandContext = func(_ context.Context, _ string, args ...string) *exec.Cmd {
		captured = exec.Command("cat", args...)
		return captured
	}

	client, err := StartClientWithAgentAndDir(
		context.Background(), "anthropic", "sonnet", "", "jockey", "",
	)
	if err != nil {
		t.Fatalf("StartClientWithAgentAndDir: %v", err)
	}
	defer client.Close()

	if captured == nil {
		t.Fatal("pi command was not created")
	}
	want := []string{"cat", "--mode", "rpc", "--no-session", "--provider", "anthropic", "--model", "sonnet"}
	if strings.Join(captured.Args, " ") != strings.Join(want, " ") {
		t.Fatalf("pi args = %#v, want %#v", captured.Args, want)
	}
	gotAgent := ""
	for _, entry := range captured.Env {
		if strings.HasPrefix(entry, "PI_AGENT=") {
			gotAgent = strings.TrimPrefix(entry, "PI_AGENT=")
		}
	}
	if gotAgent != "jockey" {
		t.Fatalf("PI_AGENT = %q, want jockey", gotAgent)
	}
}

func TestStartClientForwardsThinkingFlag(t *testing.T) {
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })

	var captured []string
	piExecCommandContext = func(_ context.Context, _ string, args ...string) *exec.Cmd {
		captured = append([]string(nil), args...)
		return exec.Command("cat")
	}
	client, err := StartClientWithAgentProfileThinkingAndDir(
		context.Background(), "", "", "", "", "", "high", "",
	)
	if err != nil {
		t.Fatalf("StartClientWithAgentProfileThinkingAndDir: %v", err)
	}
	defer client.Close()
	if got := strings.Join(captured, " "); got != "--mode rpc --no-session --thinking high" {
		t.Fatalf("args = %q", got)
	}
}

func TestStartClientOmitsThinkingFlagByDefault(t *testing.T) {
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })

	var captured []string
	piExecCommandContext = func(_ context.Context, _ string, args ...string) *exec.Cmd {
		captured = append([]string(nil), args...)
		return exec.Command("cat")
	}
	client, err := StartClientWithAgentProfileAndDir(context.Background(), "", "", "", "", "", "")
	if err != nil {
		t.Fatalf("StartClientWithAgentProfileAndDir: %v", err)
	}
	defer client.Close()
	if got := strings.Join(captured, " "); strings.Contains(got, "--thinking") {
		t.Fatalf("args = %q", got)
	}
}

func TestStartClientEmptyAgent(t *testing.T) {
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })

	var captured *exec.Cmd
	piExecCommandContext = func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		captured = exec.Command("cat")
		return captured
	}

	client, err := StartClientWithAgentAndDir(
		context.Background(), "anthropic", "sonnet", "", "", "",
	)
	if err != nil {
		t.Fatalf("StartClientWithAgentAndDir: %v", err)
	}
	defer client.Close()

	if captured == nil {
		t.Fatal("pi command was not created")
	}
	for _, entry := range captured.Env {
		if strings.HasPrefix(entry, "PI_AGENT=") {
			t.Fatalf("PI_AGENT should not be set when agent is empty, got %q", entry)
		}
	}
}

func TestStartClientReplacesAgentProfileEnvironment(t *testing.T) {
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })
	t.Setenv("PI_AGENT", "stale-agent")
	t.Setenv("PI_AGENT_PROFILE", "stale-profile")

	var captured *exec.Cmd
	piExecCommandContext = func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		captured = exec.Command("cat")
		return captured
	}

	client, err := StartClientWithAgentProfileAndDir(
		context.Background(), "", "", "", "explore", "cloud", "",
	)
	if err != nil {
		t.Fatalf("StartClientWithAgentProfileAndDir: %v", err)
	}
	defer client.Close()

	if captured == nil {
		t.Fatal("pi command was not created")
	}
	gotAgent, gotProfile := "", ""
	for _, entry := range captured.Env {
		if strings.HasPrefix(entry, "PI_AGENT=") {
			gotAgent = strings.TrimPrefix(entry, "PI_AGENT=")
		}
		if strings.HasPrefix(entry, "PI_AGENT_PROFILE=") {
			gotProfile = strings.TrimPrefix(entry, "PI_AGENT_PROFILE=")
		}
	}
	if gotAgent != "explore" || gotProfile != "cloud" {
		t.Fatalf("pi environment = PI_AGENT=%q PI_AGENT_PROFILE=%q, want explore/cloud", gotAgent, gotProfile)
	}
}

func TestStartClientInjectsProfileWithoutAgent(t *testing.T) {
	original := piExecCommandContext
	t.Cleanup(func() { piExecCommandContext = original })
	t.Setenv("PI_AGENT_PROFILE", "stale-profile")

	var captured *exec.Cmd
	piExecCommandContext = func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		captured = exec.Command("cat")
		return captured
	}

	client, err := StartClientWithAgentProfileAndDir(
		context.Background(), "", "", "", "", "cloud", "",
	)
	if err != nil {
		t.Fatalf("StartClientWithAgentProfileAndDir: %v", err)
	}
	defer client.Close()

	gotAgent, gotProfile := "", ""
	for _, entry := range captured.Env {
		if strings.HasPrefix(entry, "PI_AGENT=") {
			gotAgent = strings.TrimPrefix(entry, "PI_AGENT=")
		}
		if strings.HasPrefix(entry, "PI_AGENT_PROFILE=") {
			gotProfile = strings.TrimPrefix(entry, "PI_AGENT_PROFILE=")
		}
	}
	if gotAgent != "" || gotProfile != "cloud" {
		t.Fatalf("pi environment = PI_AGENT=%q PI_AGENT_PROFILE=%q, want empty/cloud", gotAgent, gotProfile)
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

func TestClientToolCallCorrelationEnrichesPermission(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi-corr")

	go func() {
		writeLine(wOut, map[string]any{
			"type":     "tool_execution_start",
			"toolName": "bash",
			"input":    "ls -la /tmp/agy-stage15-terra/probe-workdir",
		})
		writeLine(wOut, map[string]any{
			"type":    "extension_ui_request",
			"id":      "ui-corr-1",
			"method":  "select",
			"title":   "Allow command?",
			"options": []string{"Allow", "Deny"},
		})
	}()

	// Drain and verify the tool_execution_start events (canonical + alias).
	drainAndAssert(t, c.eventsCh, []string{"tool.call", "avenor.tool.start"})

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "permission.request" {
			t.Fatalf("event = %q, want permission.request", ev.Event)
		}
		if got, _ := ev.Fields["tool_name"].(string); got != "bash" {
			t.Errorf("tool_name = %q, want bash", got)
		}
		if got, _ := ev.Fields["command"].(string); got != "ls -la /tmp/agy-stage15-terra/probe-workdir" {
			t.Errorf("command = %q, want the ls command", got)
		}
		if got, _ := ev.Fields["description"].(string); got != "Allow command?" {
			t.Errorf("description = %q, want Allow command?", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for permission request")
	}
}

func TestClientToolCallCorrelationNoToolCall(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi-nocorr")

	go func() {
		writeLine(wOut, map[string]any{
			"type":    "extension_ui_request",
			"id":      "ui-nocorr-1",
			"method":  "select",
			"title":   "Allow command?",
			"options": []string{"Allow", "Deny"},
		})
	}()

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "permission.request" {
			t.Fatalf("event = %q, want permission.request", ev.Event)
		}
		if _, ok := ev.Fields["tool_name"]; ok {
			t.Error("tool_name should not be set when no tool call preceded")
		}
		if _, ok := ev.Fields["command"]; ok {
			t.Error("command should not be set when no tool call preceded")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for permission request")
	}
}

func TestClientToolCallClearedOnToolExecutionEnd(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi-clear")

	go func() {
		writeLine(wOut, map[string]any{
			"type":     "tool_execution_start",
			"toolName": "bash",
			"input":    "ls /tmp/work",
		})
		writeLine(wOut, map[string]any{
			"type":     "tool_execution_end",
			"toolName": "bash",
		})
		writeLine(wOut, map[string]any{
			"type":    "extension_ui_request",
			"id":      "ui-clear-1",
			"method":  "select",
			"title":   "Allow?",
			"options": []string{"Allow", "Deny"},
		})
	}()

	// Drain and verify the tool_execution_start + end events.
	drainAndAssert(t, c.eventsCh, []string{"tool.call", "avenor.tool.start", "tool.call_update", "avenor.tool.end"})

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "permission.request" {
			t.Fatalf("event = %q, want permission.request", ev.Event)
		}
		if _, ok := ev.Fields["tool_name"]; ok {
			t.Error("tool_name should not be set after tool_execution_end")
		}
		if _, ok := ev.Fields["command"]; ok {
			t.Error("command should not be set after tool_execution_end")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for permission request")
	}
}

// drainAndAssert reads n events from ch, where n = len(want), and asserts
// each event type matches the corresponding entry in want. This locks the
// contract so tests break loudly if translateNotification changes the
// number or order of events.
func drainAndAssert(t *testing.T, ch <-chan events.Event, want []string) {
	t.Helper()
	for i, wantType := range want {
		select {
		case ev := <-ch:
			if ev.Event != wantType {
				t.Fatalf("event[%d] = %q, want %q", i, ev.Event, wantType)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %d (%s)", i, wantType)
		}
	}
}

func TestClientToolCallCorrelationPreservesBackendFields(t *testing.T) {
	c, wOut, _ := fakeClient()
	defer c.Close()

	c.setSessionID("pi-preserve")

	go func() {
		// tool_execution_start with a command.
		writeLine(wOut, map[string]any{
			"type":     "tool_execution_start",
			"toolName": "bash",
			"input":    "ls /tmp/work",
		})
		// extension_ui_request that already carries tool_name and command.
		// enrichWithToolContext must NOT overwrite these.
		writeLine(wOut, map[string]any{
			"type":      "extension_ui_request",
			"id":        "ui-preserve-1",
			"method":    "select",
			"title":     "Allow?",
			"options":   []string{"Allow", "Deny"},
			"tool_name": "custom_tool",
			"command":   "cat /custom/path",
		})
	}()

	drainAndAssert(t, c.eventsCh, []string{"tool.call", "avenor.tool.start"})

	select {
	case ev := <-c.eventsCh:
		if ev.Event != "permission.request" {
			t.Fatalf("event = %q, want permission.request", ev.Event)
		}
		// Backend-provided fields must be preserved, not overwritten.
		if got, _ := ev.Fields["tool_name"].(string); got != "custom_tool" {
			t.Errorf("tool_name = %q, want custom_tool (backend value preserved)", got)
		}
		if got, _ := ev.Fields["command"].(string); got != "cat /custom/path" {
			t.Errorf("command = %q, want cat /custom/path (backend value preserved)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for permission request")
	}
}
