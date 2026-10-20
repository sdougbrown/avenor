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

	first := filepath.Join(dir, "avenor-run-"+resumeUUID1+".done")
	firstLog := filepath.Join(dir, "avenor-run-"+resumeUUID1+".log")
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
