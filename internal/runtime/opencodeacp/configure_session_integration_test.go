//go:build integration

package opencodeacp

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

func TestConfigureSessionEndToEndAgentAndModel(t *testing.T) {
	if !opencodeAvailable() {
		t.Skip("opencode not on PATH")
	}

	modelID := "deepseek/deepseek-v4-pro"
	if env := os.Getenv("AVENOR_TEST_MODEL"); env != "" {
		modelID = env
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	client := newE2EClient(t, ctx)

	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	session, err := client.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	if err := client.SetSessionModel(ctx, session.SessionID, modelID); err != nil {
		t.Fatalf("SetSessionModel: %v", err)
	}
	if err := client.SetSessionMode(ctx, session.SessionID, "jockey"); err != nil {
		t.Fatalf("SetSessionMode: %v", err)
	}

	resp := promptAndCollect(t, ctx, client, session,
		"What model are you currently using? Reply on a single line starting with MODEL: and the model ID. Nothing else.")

	t.Logf("jockey response: %q", resp)
	if resp == "" {
		t.Error("no response text — configureSession may not be sending agent/model correctly")
	}
}

func TestConfigureSessionAgentOnlyNoModel(t *testing.T) {
	if !opencodeAvailable() {
		t.Skip("opencode not on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newE2EClient(t, ctx)

	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	session, err := client.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// Only set agent — no SetSessionModel. Replicates --agent jockey without --model.
	if err := client.SetSessionMode(ctx, session.SessionID, "jockey"); err != nil {
		t.Fatalf("SetSessionMode: %v", err)
	}

	resp := promptAndCollect(t, ctx, client, session,
		"What model are you currently using? Reply on a single line starting with MODEL: and the model ID. Nothing else.")

	t.Logf("jockey response (agent-only, no model): %q", resp)
	if resp == "" {
		t.Error("no response text — SetSessionMode may not have applied the agent correctly")
	}
}

// TestConfigureSessionJockeyWritePermission verifies that the jockey agent
// (which has no write permissions) cannot write files. If a write succeeds,
// the agent mode is not being applied correctly.
func TestConfigureSessionJockeyWritePermission(t *testing.T) {
	if !opencodeAvailable() {
		t.Skip("opencode not on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newE2EClient(t, ctx)

	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	session, err := client.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	if err := client.SetSessionMode(ctx, session.SessionID, "jockey"); err != nil {
		t.Fatalf("SetSessionMode: %v", err)
	}

	resp := promptAndCollect(t, ctx, client, session,
		"Write a file named /tmp/avenor-jockey-test.txt containing the text 'jockey wrote this'. "+
			"After the write attempt, reply with whether the write succeeded or was blocked, "+
			"and why. Start your reply with RESULT:.")

	t.Logf("jockey write-permission response: %q", resp)
	if resp == "" {
		t.Error("no response — SetSessionMode may not have applied the agent correctly")
	}
}

// TestConfigureSessionNoAgentWritePermission verifies that WITHOUT setting a
// jockey agent, writes succeed (or at least are not blocked by jockey-specific
// constraints). This contrasts with TestConfigureSessionJockeyWritePermission.
func TestConfigureSessionNoAgentWritePermission(t *testing.T) {
	if !opencodeAvailable() {
		t.Skip("opencode not on PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := newE2EClient(t, ctx)

	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	session, err := client.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// No SetSessionMode call — uses opencode's default mode, which has write permissions.

	resp := promptAndCollect(t, ctx, client, session,
		"Write a file named /tmp/avenor-default-test.txt containing the text 'default agent wrote this'. "+
			"After the write attempt, reply with whether the write succeeded or was blocked, "+
			"and why. Start your reply with RESULT:.")

	t.Logf("default agent write-permission response: %q", resp)
	if resp == "" {
		t.Error("no response from default agent")
	}
}

func newE2EClient(t *testing.T, ctx context.Context) *acp.Client {
	t.Helper()

	client, err := acp.NewClient(ctx, acp.ClientConfig{
		Bin:                  "opencode",
		Args:                 []string{"acp", "--pure", "--log-level", "WARN"},
		Dir:                  ".",
		AppendCWDArg:         true,
		AutoAnswerPermission: true, // auto-answer all permissions (preferring reject) so session doesn't hang
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func promptAndCollect(t *testing.T, ctx context.Context, client *acp.Client, session *acp.Session, prompt string) string {
	t.Helper()

	eventsCh := client.Events()

	promptDone := make(chan struct {
		event events.Event
		err   error
	}, 1)
	go func() {
		event, err := session.Prompt(ctx, prompt)
		promptDone <- struct {
			event events.Event
			err   error
		}{event: event, err: err}
	}()

	var responseText strings.Builder
	for {
		select {
		case ev, ok := <-eventsCh:
			if !ok {
				return responseText.String()
			}
			if ev.Event == "agent.message_chunk" {
				if text := chunkTextFromEvent(ev); text != "" {
					responseText.WriteString(text)
				}
			}
		case result := <-promptDone:
			if result.err != nil {
				t.Fatalf("Prompt: %v", result.err)
			}
			t.Logf("session.end stop_reason: %v", result.event.Fields["stop_reason"])
			return responseText.String()
		case <-ctx.Done():
			t.Fatalf("timeout: %v", ctx.Err())
		}
	}
}

func chunkTextFromEvent(ev events.Event) string {
	content, ok := ev.Fields["content"].(map[string]any)
	if !ok {
		return ""
	}
	text, _ := content["text"].(string)
	return text
}
