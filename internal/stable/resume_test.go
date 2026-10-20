package stable

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
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
	writeSentinel(t, sentinel, "DONE", "ses_resumed")
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
	writeSentinel(t, sentinel, "DONE", "ses_resumed")

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

func TestResumeReportsSpawnedPaths(t *testing.T) {
	sup, _ := newStage2Supervisor(t, 4)
	prior, err := sup.spawn(SpawnParams{Prompt: "start", Dir: t.TempDir(), Label: "prior"})
	if err != nil {
		t.Fatalf("spawn prior: %v", err)
	}
	writeSentinel(t, prior.SentinelFile, "DONE", "ses_resumed")

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
