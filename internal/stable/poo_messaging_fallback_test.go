package stable

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime"
)

type fallbackOutcomeProvider struct{ *stage2Provider }

func (p *fallbackOutcomeProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	session, err := p.stage2Provider.Start(ctx, opts)
	if err == nil {
		off := false
		session.MessagingOn = &off
	}
	return session, err
}

func TestFallbackMessagingOutcomeSurvivesColdResume(t *testing.T) {
	stubPooOnPath(t)
	sup, base := newStage2Supervisor(t, 8)
	var launches []runtime.StartOptions
	sup.newProviderFunc = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		launches = append(launches, opts)
		return &fallbackOutcomeProvider{base}, nil
	}
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "prior.env")
	prior, err := sup.spawn(SpawnParams{Prompt: "start", Dir: dir, Label: "prior", Backend: "poo", SentinelFile: sentinel})
	if err != nil {
		t.Fatal(err)
	}
	if len(launches) != 1 || !launches[0].MessagingOn || launches[0].Messaging != nil {
		t.Fatalf("initial launch options = %+v, want default-on", launches)
	}
	finishResumeFixture(t, sup, prior, sentinel, false)
	result, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue",
		SentinelFile: filepath.Join(dir, "next.env"), EventLog: filepath.Join(dir, "next.log"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(launches) != 2 || launches[1].MessagingOn || launches[1].Messaging != nil {
		t.Fatalf("resume launch options = %+v, want effective off with default request", launches)
	}
	child := sup.runtimeByID(result.RuntimeID)
	child.mu.Lock()
	on := child.messagingOn
	child.mu.Unlock()
	if on {
		t.Fatal("resumed child recorded messaging on")
	}
}

func TestExplicitMessagingUnavailableIsNotRetried(t *testing.T) {
	provider := newItestProvider(
		itestAttempt("ses_initial", itestSessionEnd("ses_initial", "error", "")),
		stableScriptedAttempt{startErr: &runtime.MessagingUnavailableError{Reason: "test"}},
		itestAttempt("ses_unwanted", itestSessionEnd("ses_unwanted", "end_turn", "")),
	)
	h := newItestHarness(t, Config{}, itestSingleProvider(provider))
	sentinel := filepath.Join(t.TempDir(), "done.env")
	rt := h.spawn(map[string]any{"messaging": true, "max_retries": 3, "sentinel_file": sentinel})
	child := h.sup.runtimeByID(rt)
	select {
	case <-child.runDone:
	case <-time.After(30 * time.Second):
		t.Fatal("runtime did not finish after messaging refusal")
	}
	content := readSentinel(t, sentinel)
	if !strings.Contains(content, "STOP_REASON="+runtime.MessagingUnavailableStopReason+"\n") {
		t.Fatalf("sentinel = %q, want stop reason %q", content, runtime.MessagingUnavailableStopReason)
	}
	provider.mu.Lock()
	attempt := provider.attempt
	provider.mu.Unlock()
	if attempt != 1 {
		t.Fatalf("attempt index = %d, want 1 (initial session and one refused relaunch)", attempt)
	}
}
