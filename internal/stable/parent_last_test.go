package stable

import (
	"encoding/json"
	"errors"
	"net/http"
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

	token, err := sup.broker.CreateRun("rt_local", "local")
	if err != nil {
		t.Fatalf("create a local run: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+sup.broker.Addr()+"/last_message?target=hub-run", nil)
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
	var body struct {
		RunID       string `json:"run_id"`
		HasRecord   bool   `json:"has_record"`
		FinalOutput string `json:"final_output"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /last_message: %v", err)
	}
	if resp.StatusCode != http.StatusOK || body.RunID != "hub-run" || !body.HasRecord || body.FinalOutput != "reviewed the diff" {
		t.Fatalf("/last_message = %d %+v, want the parent's turn", resp.StatusCode, body)
	}

	_, _, err = sup.SessionLast("nobody")
	var toolErr *router.ToolError
	if !errors.As(err, &toolErr) || toolErr.Code != "unknown_target" {
		t.Fatalf("SessionLast(unknown run) error = %v, want unknown_target", err)
	}
}
