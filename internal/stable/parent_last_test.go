package stable

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// TestSupervisorLastReadsARunItsParentHolds pins the read through the parent
// seam: a run registered at the parent broker is not in the branch's own broker,
// yet its last turn reads through the supervisor and through the branch's
// /last_message route, while a run nobody holds is still an unknown target.
func TestSupervisorLastReadsARunItsParentHolds(t *testing.T) {
	t.Parallel()
	parentBroker, _ := startTestParentBroker(t)
	if _, err := parentBroker.CreateRun("hub-run", "hub"); err != nil {
		t.Fatalf("create the parent's run: %v", err)
	}
	if err := parentBroker.SetLastAgentMessage("hub-run", "reviewed the diff", "end_turn", false); err != nil {
		t.Fatalf("record the parent run's turn: %v", err)
	}
	sup := newTestSupervisor(t, Config{
		ControlSocket:     newStableSocketPath(t, "last"),
		MaxRuntimes:       2,
		BrokerParentURL:   "http://" + parentBroker.Addr(),
		BrokerParentToken: testLinkToken,
	})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })

	record, ok, err := sup.SessionLast("hub-run")
	if err != nil || !ok {
		t.Fatalf("SessionLast(parent run) = (%+v, %v, %v), want the parent's turn", record, ok, err)
	}
	if record.RunID != "hub-run" || record.Text != "reviewed the diff" || record.StopReason != "end_turn" || record.At == 0 {
		t.Fatalf("SessionLast(parent run) = %+v, want the recorded turn with its time", record)
	}

	status, body := getLastMessage(t, sup, "hub-run")
	if status != http.StatusOK || body.RunID != "hub-run" || !body.HasRecord || body.FinalOutput != "reviewed the diff" {
		t.Fatalf("/last_message = %d %+v, want the parent's turn", status, body)
	}

	_, _, err = sup.SessionLast("nobody")
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) || toolErr.Code != "unknown_target" {
		t.Fatalf("SessionLast(unknown run) error = %v, want unknown_target", err)
	}
}

// lastMessageBody is the /last_message fields these tests read.
type lastMessageBody struct {
	RunID       string `json:"run_id"`
	HasRecord   bool   `json:"has_record"`
	FinalOutput string `json:"final_output"`
}

// getLastMessage reads /last_message from the supervisor's broker as a run it
// holds, the way a branch's own client does.
func getLastMessage(t *testing.T, sup *Supervisor, target string) (int, lastMessageBody) {
	t.Helper()
	token, err := sup.broker.CreateRun("rt_local", "local")
	if err != nil {
		t.Fatalf("create a local run: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+sup.broker.Addr()+"/last_message?target="+target, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(messaging.HeaderRunID, "rt_local")
	req.Header.Set(messaging.HeaderRunToken, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /last_message: %v", err)
	}
	defer resp.Body.Close()
	var body lastMessageBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /last_message: %v", err)
	}
	return resp.StatusCode, body
}

// TestSupervisorLastReportsAParentRunWithNoTurnAsHavingNone pins the no-record
// answer through the parent: a run the parent holds that has produced no turn is
// ok=false with no error, and its route answer says has_record=false, so a run
// with nothing recorded is never reported as having an empty turn.
func TestSupervisorLastReportsAParentRunWithNoTurnAsHavingNone(t *testing.T) {
	t.Parallel()
	parentBroker, _ := startTestParentBroker(t)
	if _, err := parentBroker.CreateRun("quiet-run", "quiet"); err != nil {
		t.Fatalf("create the parent's run: %v", err)
	}
	sup := newTestSupervisor(t, Config{
		ControlSocket:     newStableSocketPath(t, "quiet"),
		MaxRuntimes:       2,
		BrokerParentURL:   "http://" + parentBroker.Addr(),
		BrokerParentToken: testLinkToken,
	})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })

	record, ok, err := sup.SessionLast("quiet-run")
	if err != nil || ok {
		t.Fatalf("SessionLast(parent run with no turn) = (%+v, %v, %v), want no error and ok=false", record, ok, err)
	}
	if record.RunID != "quiet-run" {
		t.Fatalf("SessionLast(parent run with no turn) identity = %+v, want the run's identity", record)
	}

	status, body := getLastMessage(t, sup, "quiet-run")
	if status != http.StatusOK || body.RunID != "quiet-run" || body.HasRecord {
		t.Fatalf("/last_message = %d %+v, want 200 with has_record=false", status, body)
	}
}

// TestSupervisorLastRefusesARunAnotherBranchOwns pins the limit of the parent
// read: a sibling branch's run is not visible to a branch, so the hub refuses
// the read it forwards for this branch's run as an unknown target.
func TestSupervisorLastRefusesARunAnotherBranchOwns(t *testing.T) {
	t.Parallel()
	parentBroker, _ := startTestParentBroker(t)
	siblingBroker, _, _ := startTestBranch(t, parentBroker, "rt_remote")
	if err := siblingBroker.SetLastAgentMessage("rt_remote", "built on the sibling", "end_turn", false); err != nil {
		t.Fatalf("record the sibling run's turn: %v", err)
	}
	sup := newTestSupervisor(t, Config{
		ControlSocket:     newStableSocketPath(t, "sib"),
		MaxRuntimes:       2,
		BrokerParentURL:   "http://" + parentBroker.Addr(),
		BrokerParentToken: testLinkToken,
	})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })

	record, ok, err := sup.SessionLast(branchKey(siblingBroker, "rt_remote"))
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) || toolErr.Code != "unknown_target" {
		t.Fatalf("SessionLast(sibling branch's run) = (%+v, %v, %v), want unknown_target", record, ok, err)
	}
}

// TestSupervisorLastReportsAnUnreachableParentApartFromAMissingRun pins the
// failure class end to end: a parent that has gone away is a refused read, not
// an unknown target, whose code is not the invalid-request class a malformed
// call gets and whose reason names no parent address.
func TestSupervisorLastReportsAnUnreachableParentApartFromAMissingRun(t *testing.T) {
	t.Parallel()
	parentBroker, _ := startTestParentBroker(t)
	if _, err := parentBroker.CreateRun("hub-run", "hub"); err != nil {
		t.Fatalf("create the parent's run: %v", err)
	}
	sup := newTestSupervisor(t, Config{
		ControlSocket:     newStableSocketPath(t, "gone"),
		MaxRuntimes:       2,
		BrokerParentURL:   "http://" + parentBroker.Addr(),
		BrokerParentToken: testLinkToken,
	})
	t.Cleanup(func() { _ = sup.Shutdown("kill") })
	parentURL := parentBroker.Addr()
	if err := parentBroker.Stop(); err != nil {
		t.Fatalf("stop the parent: %v", err)
	}

	_, _, err := sup.SessionLast("hub-run")
	if err == nil {
		t.Fatal("SessionLast with the parent gone succeeded")
	}
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) || toolErr.Code != "refused" || toolErr.Message != messaging.ErrRemoteReadFailed.Error() {
		t.Fatalf("SessionLast with the parent gone = %v, want a refused failed read carrying %q", err, messaging.ErrRemoteReadFailed)
	}
	if strings.Contains(err.Error(), parentURL) {
		t.Fatalf("the failure %q names the parent's address", err)
	}
}
