package stable

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// boundParentProvider records what each spawned child was launched with and
// told, then finishes its turn.
type boundParentProvider struct {
	*stableFakeProvider
	mu      sync.Mutex
	starts  []runtime.StartOptions
	prompts []string
}

func newBoundParentProvider() *boundParentProvider {
	return &boundParentProvider{stableFakeProvider: &stableFakeProvider{events: make(chan events.Event, 1)}}
}

func (p *boundParentProvider) Start(ctx context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	p.starts = append(p.starts, opts)
	p.mu.Unlock()
	return p.stableFakeProvider.Start(ctx, opts)
}

func (p *boundParentProvider) Prompt(ctx context.Context, sessionID, prompt string) error {
	p.mu.Lock()
	p.prompts = append(p.prompts, prompt)
	p.mu.Unlock()
	return p.stableFakeProvider.Prompt(ctx, sessionID, prompt)
}

func (p *boundParentProvider) launches() ([]runtime.StartOptions, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]runtime.StartOptions(nil), p.starts...), append([]string(nil), p.prompts...)
}

// boundParentSupervisor builds a supervisor whose one provider records the
// children a spawn starts, so a test can tell a refused spawn from a started
// child.
type boundParentSupervisor struct {
	sup      *Supervisor
	provider *boundParentProvider
	mu       sync.Mutex
	created  int
}

func newBoundParentSupervisor(t *testing.T) *boundParentSupervisor {
	t.Helper()
	sup := newTestSupervisor(t, Config{ControlSocket: newStableSocketPath(t, "bound-parent"), MaxRuntimes: 2})
	bp := &boundParentSupervisor{sup: sup, provider: newBoundParentProvider()}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		bp.mu.Lock()
		bp.created++
		bp.mu.Unlock()
		return bp.provider, nil
	}
	t.Cleanup(func() { _ = sup.Shutdown("kill") })
	return bp
}

func (bp *boundParentSupervisor) children() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.created
}

// bindOne mints one conversation's binding the way the host's owner connection
// does.
func (bp *boundParentSupervisor) bindOne(t *testing.T, session string) messaging.MessageBindResult {
	t.Helper()
	res, err := bp.sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: session})
	if err != nil {
		t.Fatalf("message_bind %s: %v", session, err)
	}
	return res
}

// spawnMCP drives the gated spawn verb with a parent the MCP surface declares.
func (bp *boundParentSupervisor) spawnMCP(parentRunID string) (SpawnResult, error) {
	raw, err := json.Marshal(SpawnParams{Prompt: "work", Dir: "/tmp", ParentRunID: parentRunID})
	if err != nil {
		return SpawnResult{}, err
	}
	out, err := bp.sup.SpawnMCP(raw)
	if err != nil {
		return SpawnResult{}, err
	}
	return out.(SpawnResult), nil
}

// awaitPrompt waits for the child's prompt, which reaches the provider after the
// spawn returns.
func awaitPrompt(t *testing.T, provider *boundParentProvider) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, prompts := provider.launches()
		if len(prompts) > 0 {
			return prompts[0]
		}
		if time.Now().After(deadline) {
			t.Fatal("the child never received its prompt")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSpawnMCPNamesALiveBoundParent pins the accepted case: a spawn_mcp whose
// parent is the conversation's live binding starts a child told to report to
// that run, and neither the spawn result nor the child's launch carries the
// conversation's credential.
func TestSpawnMCPNamesALiveBoundParent(t *testing.T) {
	t.Parallel()
	bp := newBoundParentSupervisor(t)
	bound := bp.bindOne(t, "ses_live")

	result, err := bp.spawnMCP(bound.RunID)
	if err != nil {
		t.Fatalf("spawn_mcp with a live bound parent: %v", err)
	}
	if bp.children() != 1 {
		t.Fatalf("children created = %d, want 1", bp.children())
	}
	if result.ParentToken != "" {
		t.Fatal("the spawn result carries the conversation's credential")
	}
	starts, _ := bp.provider.launches()
	if len(starts) != 1 {
		t.Fatalf("child launches = %d, want 1", len(starts))
	}
	if starts[0].BrokerToken == bound.RunToken {
		t.Fatal("the child was launched with the bound conversation's credential")
	}
	note := awaitPrompt(t, bp.provider)
	if !strings.Contains(note, `"`+bound.RunID+`"`) {
		t.Fatalf("child note = %q, want it to name the bound run %q", note, bound.RunID)
	}
}

// TestSpawnMCPRefusesAStaleBoundParent pins the refusal: a parent this
// supervisor no longer holds live — a retired run, a removed record, or an id an
// earlier supervisor's binding minted — fails the spawn with the reconnect
// instruction. No child starts, and the spawn does not adopt the id into a fresh
// record nothing reads.
func TestSpawnMCPRefusesAStaleBoundParent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		kill func(t *testing.T, bp *boundParentSupervisor, bound messaging.MessageBindResult)
	}{
		{
			name: "retired binding",
			kill: func(t *testing.T, bp *boundParentSupervisor, bound messaging.MessageBindResult) {
				retireBoundPeer(t, bp.sup, bound)
			},
		},
		{
			name: "removed record",
			kill: func(t *testing.T, bp *boundParentSupervisor, bound messaging.MessageBindResult) {
				bp.sup.broker.DeleteRunIfToken(bound.RunID, bound.RunToken)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bp := newBoundParentSupervisor(t)
			bound := bp.bindOne(t, "ses_stale")
			retiredToken := bound.RunToken
			tc.kill(t, bp, bound)

			_, err := bp.spawnMCP(bound.RunID)
			if err == nil || !strings.Contains(err.Error(), "reconnect the host conversation") {
				t.Fatalf("spawn_mcp error = %v, want the reconnect refusal", err)
			}
			if bp.children() != 0 {
				t.Fatalf("children created = %d, want the stale spawn refused before any child", bp.children())
			}
			if token, ok := bp.sup.broker.OwnedReachableToken(bound.RunID); ok {
				t.Fatalf("the refused spawn minted a live record for %s (token %q)", bound.RunID, token)
			}
			if record := bp.sup.broker.GetRun(bound.RunID); record != nil && record.Token != "" && record.Token != retiredToken {
				t.Fatalf("the refused spawn replaced the stale record's credential")
			}
		})
	}
}

// TestSpawnMCPRefusesAnEarlierSupervisorsBinding pins the replacement case: a
// binding minted by a supervisor that is gone is not this supervisor's to serve,
// even though the native session id is the same. The stale spawn is refused and
// the replacement holds no record it could hand a child.
func TestSpawnMCPRefusesAnEarlierSupervisorsBinding(t *testing.T) {
	t.Parallel()
	earlier := newBoundParentSupervisor(t)
	bound := earlier.bindOne(t, "ses_restarted")

	replacement := newBoundParentSupervisor(t)
	if _, err := replacement.sup.MessageBind(bindCaller(), messaging.MessageBindRequest{Host: "poo", SessionID: "ses_restarted"}); err != nil {
		t.Fatalf("message_bind on the replacement: %v", err)
	}

	_, err := replacement.spawnMCP(bound.RunID)
	if err == nil || !strings.Contains(err.Error(), "reconnect the host conversation") {
		t.Fatalf("spawn_mcp error = %v, want the reconnect refusal", err)
	}
	if replacement.children() != 0 {
		t.Fatalf("children created = %d, want none on the replacement", replacement.children())
	}
	if record := replacement.sup.broker.GetRun(bound.RunID); record != nil {
		t.Fatalf("the replacement minted a record for the earlier supervisor's run %s", bound.RunID)
	}
}
