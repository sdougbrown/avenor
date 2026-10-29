package stable

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/pi"
)

const resumeUUID1 = "6f9619ff-8b86-d011-b42d-00cf4fc964ff"
const resumeUUID2 = "0b6b5d3e-6f5f-4e4a-9d8c-2b8a8f8f8f8f"

func writeSentinel(t *testing.T, path, status, sessionID string) {
	t.Helper()
	content := status + "\n"
	if sessionID != "" {
		content += "SESSION=" + sessionID + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
}

func TestColdResumeRefusesUntilPriorTeardownFinishes(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 4)
	for _, state := range []lifecycleState{
		{completion: completionAdmissionClosed},
		{shutdown: shutdownRequested},
		{writer: writerClosing},
	} {
		child := &childRuntime{id: "prior", runDone: make(chan struct{}), lc: state}
		_, err := sup.resumeOnce(child, child.id, SpawnParams{Backend: "opencode", Label: "next", SessionID: "same-session"})
		if err == nil || !strings.Contains(err.Error(), "teardown") {
			t.Fatalf("resume during teardown (%+v): %v", state, err)
		}
		if got := provider.resumesRecorded(); len(got) != 0 {
			t.Fatalf("provider resumed before teardown: %v", got)
		}
	}
}

func TestResumeRefusesNonOwner(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	_, err := sup.Resume(messaging.Caller{Kind: messaging.CallerBroker, RunID: "rt_a"}, messaging.ResumeRequest{Target: "rt_a", Message: "again"})
	assertToolCode(t, err, "not_permitted")
}

func TestResumeResolution(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	owner := messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}

	if _, err := sup.broker.CreateRun("rt_b", "shared"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := sup.broker.CreateRun("rt_a", "shared"); err != nil {
		t.Fatalf("create run: %v", err)
	}
	// A broker-known but not locally-owned run is not resumable here.
	_, err := sup.Resume(owner, messaging.ResumeRequest{Target: "shared", Message: "again"})
	assertToolCode(t, err, "ambiguous_name")
	if !strings.Contains(err.Error(), `"rt_a", "rt_b"`) {
		t.Fatalf("ambiguous text = %q", err)
	}
	_, err = sup.Resume(owner, messaging.ResumeRequest{Target: "ghost", Message: "again"})
	assertToolCode(t, err, "unknown_target")
}

func TestResumePiDenialStatuses(t *testing.T) {
	// The gate reads the live environment; pin the off state so the denial
	// under test is the pi gate, not the ladder behind it.
	t.Setenv(pi.SessionsEnv, "")
	sup, _ := newStage2Supervisor(t, 4)
	cases := []struct {
		name   string
		lc     lifecycleState
		status string
	}{
		// Park timeout: the session ended, so the denial names expiry.
		{name: "expired", lc: lifecycleState{completion: completionPublished}, status: "expired"},
		// Runtime shutdown requested: the denial names the shutdown, not expiry.
		{name: "shutdown", lc: lifecycleState{shutdown: shutdownRequested}, status: "shutdown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			child := &childRuntime{id: "prior", lc: tc.lc}
			_, err := sup.resumeOnce(child, child.id, SpawnParams{Backend: "pi", Label: "next"})
			assertToolCode(t, err, "not_resumable")
			if !strings.Contains(err.Error(), "(status: "+tc.status+")") {
				t.Fatalf("not-resumable text = %q, want status %q", err, tc.status)
			}
		})
	}
}

func TestResumeRefusesNonResumableSentinel(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "sentinel.env")
	eventLog := filepath.Join(dir, "events.log")
	prior, err := sup.spawn(SpawnParams{
		Prompt: "start", Dir: dir, Label: "prior",
		SentinelFile: sentinel, OnEvent: eventLog,
	})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	// The session ladder runs only on the cold arm, so drive the prior runtime
	// to a terminal state first: a live runtime is refused as busy before the
	// sentinel is ever read.
	if err := sup.RuntimeCancel(prior.RuntimeID); err != nil {
		t.Fatalf("cancel prior: %v", err)
	}
	child := sup.runtimeByID(prior.RuntimeID)
	select {
	case <-child.runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("prior runtime did not stop")
	}
	for _, status := range []string{"TIMEOUT", "KILLED"} {
		t.Run(status, func(t *testing.T) {
			writeSentinel(t, sentinel, status, "ses_old")
			_, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
				RunID: resumeUUID1, Target: prior.RuntimeID, Message: "again",
				SentinelFile: filepath.Join(dir, "avenor-run-"+resumeUUID1+".done"),
				EventLog:     filepath.Join(dir, "avenor-run-"+resumeUUID1+".log"),
			})
			assertToolCode(t, err, "not_resumable")
			if !strings.Contains(err.Error(), "(status: "+strings.ToLower(status)+")") {
				t.Fatalf("not-resumable text = %q", err)
			}
		})
	}
}

// TestResumeRefusesWithoutResolvableSession pins the ladder's last rung: with
// no sentinel SESSION and no live session there is nothing to continue, so the
// resume is a typed not_resumable denial instead of a fresh session behind a
// run the caller was told was resumed.
func TestResumeRefusesWithoutResolvableSession(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 4)
	dir := t.TempDir()
	prior, err := sup.spawn(SpawnParams{
		Prompt: "start", Dir: dir, Label: "prior",
		SentinelFile: filepath.Join(dir, "never-written.env"),
		OnEvent:      filepath.Join(dir, "events.log"),
	})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	// A registered runtime whose live session is gone and whose sentinel was
	// never written: neither rung of the ladder can name a session. The spawn
	// installs a bootstrap session before the run goroutine claims it, so a
	// clear before that claim makes the attempt start a second one. Wait for
	// the attempt to be running: it has then taken the session, and only a
	// session.start event could replace it again, which this provider never
	// emits.
	child := sup.runtimeByID(prior.RuntimeID)
	if child == nil {
		t.Fatalf("runtime %q is not registered", prior.RuntimeID)
	}
	waitForStableChild(t, child, func(active, _ bool, _, _ string) bool { return active })
	child.mu.Lock()
	child.session = runtime.Session{}
	child.mu.Unlock()
	// The ladder runs only on the cold arm, so drive the runtime terminal
	// before resuming; cancel writes its own error sentinel, and this ladder
	// rung needs the never-written sentinel to stay absent.
	if err := sup.RuntimeCancel(prior.RuntimeID); err != nil {
		t.Fatalf("cancel prior: %v", err)
	}
	select {
	case <-child.runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("prior runtime did not stop")
	}
	if err := os.Remove(filepath.Join(dir, "never-written.env")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove sentinel: %v", err)
	}

	_, err = sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "again",
	})
	assertToolCode(t, err, "not_resumable")
	if !strings.Contains(err.Error(), "(status: unknown)") {
		t.Fatalf("not-resumable text = %q", err)
	}
	if starts := provider.startsRecorded(); len(starts) != 1 {
		t.Fatalf("provider starts = %d, want only the prior spawn", len(starts))
	}
	if resumes := provider.resumesRecorded(); len(resumes) != 0 {
		t.Fatalf("provider resumes = %v, want none", resumes)
	}
}

func finishResumeFixture(t *testing.T, sup *Supervisor, prior SpawnResult, sentinel string) {
	t.Helper()
	if err := sup.RuntimeCancel(prior.RuntimeID); err != nil {
		t.Fatalf("cancel prior: %v", err)
	}
	child := sup.runtimeByID(prior.RuntimeID)
	select {
	case <-child.runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("prior runtime did not stop")
	}
	writeSentinel(t, sentinel, "DONE", "ses_resumed")
}

func TestResumeCarriesIdentityAndSession(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 8)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "sentinel.env")
	eventLog := filepath.Join(dir, "events.log")
	prior, err := sup.spawn(SpawnParams{
		Prompt: "start", Dir: dir, Label: "prior",
		Agent: "claude", AutoApprove: true,
		SentinelFile: sentinel, OnEvent: eventLog,
	})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	finishResumeFixture(t, sup, prior, sentinel)
	// The default follow-up label comes from the current display name, so a
	// rename before the resume is honoured.
	if _, err := sup.SessionRename(messaging.Caller{Kind: messaging.CallerBroker, RunID: prior.RuntimeID}, "", "renamed-prior"); err != nil {
		t.Fatalf("rename prior: %v", err)
	}

	newSentinel := filepath.Join(dir, "avenor-run-"+resumeUUID1+".done")
	newEventLog := filepath.Join(dir, "avenor-run-"+resumeUUID1+".log")
	result, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue",
		SentinelFile: newSentinel, EventLog: newEventLog,
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if result.RunID != resumeUUID1 || result.Label != "renamed-prior-followup" || result.SentinelFile != newSentinel || result.OnEvent != newEventLog {
		t.Fatalf("resume result = %+v", result)
	}
	if result.SessionID != "ses_resumed" || result.RuntimeID == prior.RuntimeID || result.RuntimeID == "" {
		t.Fatalf("resume identity = %+v", result)
	}

	if resumes := provider.resumesRecorded(); len(resumes) != 1 || resumes[0] != "ses_resumed" {
		t.Fatalf("provider resumes = %v, want [ses_resumed]", resumes)
	}
	if starts := provider.startsRecorded(); len(starts) != 1 {
		t.Fatalf("provider starts = %d, want only the prior spawn", len(starts))
	}
	// The resumed runtime carries the prior run's identity.
	priorChild := sup.runtimeByID(prior.RuntimeID)
	resumedChild := sup.runtimeByID(result.RuntimeID)
	if resumedChild == nil {
		t.Fatalf("resumed runtime %q is not registered", result.RuntimeID)
	}
	priorChild.mu.Lock()
	wantAgent, wantModel, wantBackend := priorChild.effectiveAgent, priorChild.effectiveModel, priorChild.effectiveBackend
	priorChild.mu.Unlock()
	resumedChild.mu.Lock()
	got := struct {
		agent, model, backend, thinking, dir, label string
		autoApprove                                 bool
	}{
		resumedChild.effectiveAgent, resumedChild.effectiveModel, resumedChild.effectiveBackend,
		resumedChild.thinking, resumedChild.dir, resumedChild.label, resumedChild.autoApprove,
	}
	resumedChild.mu.Unlock()
	if got.agent != wantAgent || got.model != wantModel || got.backend != wantBackend ||
		got.dir != dir || got.label != "renamed-prior-followup" || !got.autoApprove {
		t.Fatalf("resumed identity = %+v (prior %s/%s/%s)", got, wantAgent, wantModel, wantBackend)
	}
}

func TestResumeColdArmKeepsHarnessLabelsUnambiguous(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 8)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "sentinel.env")
	prior, err := sup.spawn(SpawnParams{
		Prompt: "start", Dir: dir, Label: "prior",
		Agent: "claude", AutoApprove: true,
		SentinelFile: sentinel,
	})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	finishResumeFixture(t, sup, prior, sentinel)
	if err := sup.broker.SetHarnessName(prior.RuntimeID, "harness-name"); err != nil {
		t.Fatalf("set harness name: %v", err)
	}

	newSentinel := filepath.Join(dir, "avenor-run-"+resumeUUID1+".done")
	newEventLog := filepath.Join(dir, "avenor-run-"+resumeUUID1+".log")
	result, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue",
		SentinelFile: newSentinel, EventLog: newEventLog,
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if result.Label != "harness-name-followup" {
		t.Fatalf("resume label = %q, want the follow-up label", result.Label)
	}
	for label, want := range map[string]string{"harness-name": prior.RuntimeID, result.Label: result.RuntimeID} {
		got, err := sup.resolveRunReference(label)
		if err != nil || got != want {
			t.Fatalf("resolve %q = %q, %v; want %q", label, got, err, want)
		}
	}
	resumed := sup.runtimeByID(result.RuntimeID)
	if resumed == nil {
		t.Fatalf("resumed runtime %q is not registered", result.RuntimeID)
	}
	resumed.mu.Lock()
	registered := resumed.label
	resumed.mu.Unlock()
	if registered != result.Label {
		t.Fatalf("runtime registered under %q but resume reported %q", registered, result.Label)
	}
	if got := sup.broker.DisplayName(result.RuntimeID); got != result.Label {
		t.Fatalf("broker display name = %q, resume reported %q", got, result.Label)
	}
	// The cold arm ran: the provider resumed the prior session.
	if resumes := provider.resumesRecorded(); len(resumes) != 1 || resumes[0] != "ses_resumed" {
		t.Fatalf("provider resumes = %v, want [ses_resumed]", resumes)
	}
}

func TestResumeKeyedReplayAdoptsFirstRun(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 8)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "sentinel.env")
	eventLog := filepath.Join(dir, "events.log")
	prior, err := sup.spawn(SpawnParams{
		Prompt: "start", Dir: dir, Label: "prior",
		SentinelFile: sentinel, OnEvent: eventLog,
	})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	finishResumeFixture(t, sup, prior, sentinel)

	// The TS host mints <home>/runs/<run_id>/sentinel.done, which no sentinel
	// basename parser can recover the run id from: the run id must round-trip
	// through the spawn result instead.
	first := filepath.Join(dir, "runs", resumeUUID1, "sentinel.done")
	firstLog := filepath.Join(dir, "runs", resumeUUID1, "events.ndjson")
	if err := os.MkdirAll(filepath.Dir(first), 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}
	second := filepath.Join(dir, "avenor-run-"+resumeUUID2+".done")
	secondLog := filepath.Join(dir, "avenor-run-"+resumeUUID2+".log")

	firstResult, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue", IdempotencyKey: "k",
		SentinelFile: first, EventLog: firstLog,
	})
	if err != nil {
		t.Fatalf("first resume: %v", err)
	}
	secondResult, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID2, Target: prior.RuntimeID, Message: "continue", IdempotencyKey: "k",
		SentinelFile: second, EventLog: secondLog,
	})
	if err != nil {
		t.Fatalf("keyed replay: %v", err)
	}
	if secondResult.RunID != resumeUUID1 || secondResult.SentinelFile != first || secondResult.OnEvent != firstLog {
		t.Fatalf("keyed replay = %+v, want the first run's ids", secondResult)
	}
	if secondResult.RuntimeID != firstResult.RuntimeID {
		t.Fatalf("keyed replay spawned a second runtime: %q vs %q", secondResult.RuntimeID, firstResult.RuntimeID)
	}
	if starts := provider.startsRecorded(); len(starts) != 1 {
		t.Fatalf("provider starts = %d, want only the prior spawn", len(starts))
	}
	if resumes := provider.resumesRecorded(); len(resumes) != 1 {
		t.Fatalf("provider resumes = %d, want one for the keyed first use", len(resumes))
	}
}

func TestResumeRejectsLiveLabel(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 8)
	dir := t.TempDir()
	prior, err := sup.spawn(SpawnParams{Prompt: "start", Dir: dir, Label: "prior"})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	if _, err := sup.spawn(SpawnParams{Prompt: "other", Dir: dir, Label: "taken"}); err != nil {
		t.Fatalf("spawn other: %v", err)
	}
	_, err = sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue", Label: "taken",
	})
	assertToolCode(t, err, "refused")
	if !strings.Contains(err.Error(), "label already in use: taken") {
		t.Fatalf("label text = %q", err)
	}
}

func TestResumeRefusesActiveRuntimeEvenWithDoneSentinel(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 4)
	prior, err := sup.spawn(SpawnParams{Prompt: "start", Dir: t.TempDir(), Label: "prior"})
	if err != nil {
		t.Fatal(err)
	}
	writeSentinel(t, prior.SentinelFile, "DONE", "ses_resumed")
	_, err = sup.Resume(messaging.Caller{Kind: messaging.CallerOwner}, messaging.ResumeRequest{Target: prior.RuntimeID, Message: "continue"})
	assertToolCode(t, err, "refused")
	if !strings.Contains(err.Error(), "busy") {
		t.Fatalf("error = %v, want busy", err)
	}
	if got := provider.resumesRecorded(); len(got) != 0 {
		t.Fatalf("unexpected concurrent resume: %v", got)
	}
}

// TestResumeKeyedBusyRefusalIsToolError pins the keyed path's error mapping:
// a plain refusal from the resume work — a busy target, the way a teardown-in-
// progress one refuses — must surface as a refused ToolError carrying the
// original message, not as the raw error.
func TestResumeKeyedBusyRefusalIsToolError(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 4)
	prior, err := sup.spawn(SpawnParams{Prompt: "start", Dir: t.TempDir(), Label: "prior"})
	if err != nil {
		t.Fatal(err)
	}
	writeSentinel(t, prior.SentinelFile, "DONE", "ses_resumed")
	_, err = sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue", IdempotencyKey: "k",
	})
	assertToolCode(t, err, "refused")
	if !strings.Contains(err.Error(), "busy") {
		t.Fatalf("error = %v, want the busy message preserved", err)
	}
	if got := provider.resumesRecorded(); len(got) != 0 {
		t.Fatalf("unexpected concurrent resume: %v", got)
	}
}

func TestResumeReportsSpawnedPaths(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	prior, err := sup.spawn(SpawnParams{Prompt: "start", Dir: t.TempDir(), Label: "prior"})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	finishResumeFixture(t, sup, prior, prior.SentinelFile)

	result, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue",
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	resumed := sup.runtimeByID(result.RuntimeID)
	if resumed == nil {
		t.Fatalf("resumed runtime %q is not registered", result.RuntimeID)
	}
	resumed.mu.Lock()
	wantSentinel, wantEvent := resumed.sentinelFile, resumed.onEvent
	resumed.mu.Unlock()
	if result.SentinelFile != wantSentinel || result.OnEvent != wantEvent {
		t.Fatalf("result paths = %q/%q, want the spawned paths %q/%q", result.SentinelFile, result.OnEvent, wantSentinel, wantEvent)
	}
	if result.SentinelFile == "" || result.OnEvent == "" {
		t.Fatalf("result paths are empty: %+v", result)
	}
}

func TestResumeRefusesSupervisorTarget(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	owner := messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}
	if _, err := sup.SessionRename(owner, "", "main"); err != nil {
		t.Fatalf("rename supervisor: %v", err)
	}
	for _, target := range []string{"supervisor", "main"} {
		_, err := sup.Resume(owner, messaging.ResumeRequest{Target: target, Message: "again"})
		assertToolCode(t, err, "refused")
		if !strings.Contains(err.Error(), "not a resume target") {
			t.Fatalf("target %q refusal = %q", target, err)
		}
	}
}

func TestResumeRefusesUnownedRuntime(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	if _, err := sup.broker.CreateRun("rt_orphan", "orphan-label"); err != nil {
		t.Fatalf("create broker-only run: %v", err)
	}
	_, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		Target: "orphan-label", Message: "again",
	})
	assertToolCode(t, err, "refused")
	if !strings.Contains(err.Error(), `runtime "rt_orphan" is not owned by this supervisor`) {
		t.Fatalf("unowned refusal = %q", err)
	}
}

func TestResumeConcurrentKeyedSpawnsOneRuntime(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 8)
	dir := t.TempDir()
	prior, err := sup.spawn(SpawnParams{Prompt: "start", Dir: dir, Label: "prior"})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	finishResumeFixture(t, sup, prior, prior.SentinelFile)

	const n = 8
	results := make([]messaging.ResumeResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
				RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue", IdempotencyKey: "k",
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("resume %d: %v", i, err)
		}
		if results[i].RuntimeID != results[0].RuntimeID || results[i].RunID != results[0].RunID {
			t.Fatalf("resume %d got a different run: %+v vs %+v", i, results[i], results[0])
		}
	}
	if got := countIdempotencyRuntimes(sup); got != 2 {
		t.Fatalf("runtimes = %d, want the prior run plus one resumed run", got)
	}
	if resumes := provider.resumesRecorded(); len(resumes) != 1 {
		t.Fatalf("provider resumes = %d, want one for the keyed first use", len(resumes))
	}
}

// TestResumeKeySpaceIsolatedFromSpawn pins the follow_up: key prefix: a spawn
// and a resume may both use key "k" without the resume being rejected as the
// same key reused with different parameters.
func TestResumeKeySpaceIsolatedFromSpawn(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 8)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "sentinel.env")
	eventLog := filepath.Join(dir, "events.log")
	prior, err := sup.spawn(SpawnParams{
		Prompt: "start", Dir: dir, Label: "prior",
		SentinelFile: sentinel, OnEvent: eventLog,
	})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	finishResumeFixture(t, sup, prior, sentinel)

	raw, err := json.Marshal(SpawnParams{Prompt: "keyed", Dir: dir, Agent: "claude", IdempotencyKey: "k"})
	if err != nil {
		t.Fatalf("marshal spawn params: %v", err)
	}
	if _, err := sup.Spawn(raw); err != nil {
		t.Fatalf("keyed spawn: %v", err)
	}

	resumed, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID:          resumeUUID1,
		Target:         prior.RuntimeID,
		Message:        "continue",
		IdempotencyKey: "k",
		SentinelFile:   filepath.Join(dir, "avenor-run-"+resumeUUID1+".done"),
		EventLog:       filepath.Join(dir, "avenor-run-"+resumeUUID1+".log"),
	})
	if err != nil {
		t.Fatalf("resume reused the spawn's key space: %v", err)
	}
	if resumed.RunID != resumeUUID1 {
		t.Fatalf("resumed run_id = %q, want the freshly minted %q (a keyed replay returns the first run's id)", resumed.RunID, resumeUUID1)
	}
}

func TestResumeDropsStartOnlyOptionsForClaudeResume(t *testing.T) {
	sup, provider := newStage2Supervisor(t, 8)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "sentinel.env")
	eventLog := filepath.Join(dir, "events.log")
	prior, err := sup.spawn(SpawnParams{
		Prompt: "start", Dir: dir, Label: "prior",
		Backend: "claude", Thinking: "high", AutoApprove: true,
		SentinelFile: sentinel, OnEvent: eventLog,
	})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	finishResumeFixture(t, sup, prior, sentinel)

	newSentinel := filepath.Join(dir, "avenor-run-"+resumeUUID1+".done")
	newEventLog := filepath.Join(dir, "avenor-run-"+resumeUUID1+".log")
	if _, err := sup.Resume(messaging.Caller{Kind: messaging.CallerOwner, RunID: "supervisor"}, messaging.ResumeRequest{
		RunID: resumeUUID1, Target: prior.RuntimeID, Message: "continue",
		SentinelFile: newSentinel, EventLog: newEventLog,
	}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumes := provider.resumesRecorded(); len(resumes) != 1 || resumes[0] != "ses_resumed" {
		t.Fatalf("provider resumes = %v, want [ses_resumed]", resumes)
	}
}

// With persisted pi sessions opted in, a pi runtime takes the cold resume
// path like every other backend: the denial moves from the pi gate to the
// session ladder, which here finds no sentinel and no live session.
func TestResumePiWithPersistedSessionsRunsTheLadder(t *testing.T) {
	t.Setenv(pi.SessionsEnv, "1")
	sup, _ := newStage2Supervisor(t, 4)
	child := &childRuntime{id: "prior", lc: lifecycleState{completion: completionPublished}, runDone: make(chan struct{})}
	child.closeRunDone()
	_, err := sup.resumeOnce(child, child.id, SpawnParams{Backend: "pi", Label: "next"})
	assertToolCode(t, err, "not_resumable")
	if !strings.Contains(err.Error(), "(status: unknown)") {
		t.Fatalf("not-resumable text = %q, want the ladder's unknown status", err)
	}
}
