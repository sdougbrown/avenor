package control

import (
	"encoding/json"
	"errors"
	"net"
	"testing"
)

// errRefusedForTest stands in for a status failure that is not an absence.
var errRefusedForTest = errors.New("status unavailable")

// TestStatusCarriesRuntimeNotFoundCode pins the control plane's absence answer
// on the status verb: the sentence older clients match, plus the structured
// code that lets a newer one stop matching prose. Both dispatch paths answer,
// so both are driven.
func TestStatusCarriesRuntimeNotFoundCode(t *testing.T) {
	t.Run("one-shot snapshot", func(t *testing.T) {
		s, cap := NewTestServer(NewState("run_1", "", 0))
		if err := s.Start(testSocketPath(t)); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer s.Stop()

		owner := dialOwner(t, s, cap)
		defer owner.Close()
		statusFor(t, owner, "rt_ghost")
		assertRuntimeNotFound(t, readResp(t, owner), "rt_ghost")
	})

	t.Run("stable supervisor", func(t *testing.T) {
		s, cap := NewTestServer(NewState("run_1", "", 0))
		s.SetStableHandler(&mockStableHandler{statusErr: &RuntimeNotFoundError{RuntimeID: "rt_ghost"}})
		if err := s.Start(testSocketPath(t)); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer s.Stop()

		owner := dialOwner(t, s, cap)
		defer owner.Close()
		statusFor(t, owner, "rt_ghost")
		assertRuntimeNotFound(t, readResp(t, owner), "rt_ghost")
	})
}

// TestStatusKeepsOtherFailuresUngoded stops the code from spreading to every
// status failure: a supervisor that refuses for another reason answers with no
// data, exactly as before.
func TestStatusKeepsOtherFailuresUngoded(t *testing.T) {
	s, cap := NewTestServer(NewState("run_1", "", 0))
	s.SetStableHandler(&mockStableHandler{statusErr: errRefusedForTest})
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	owner := dialOwner(t, s, cap)
	defer owner.Close()
	statusFor(t, owner, "rt_1")
	resp := readResp(t, owner)
	if resp.Error == nil {
		t.Fatalf("status answered success, want the supervisor's failure")
	}
	if resp.Error.Data != nil {
		t.Errorf("error data = %#v, want none for a failure that is not an absence", resp.Error.Data)
	}
	if want := errRefusedForTest.Error(); resp.Error.Message != want {
		t.Errorf("message = %q, want %q", resp.Error.Message, want)
	}
}

func statusFor(t *testing.T, conn net.Conn, runtimeID string) {
	t.Helper()
	params, err := json.Marshal(map[string]string{"runtime_id": runtimeID})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if err := writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "status", Params: params}); err != nil {
		t.Fatalf("write status: %v", err)
	}
}

func assertRuntimeNotFound(t *testing.T, resp Response, wantID string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("status answered success, want a refusal: %+v", resp)
	}
	data, ok := resp.Error.Data.(map[string]any)
	if !ok {
		t.Fatalf("error data = %#v, want an object carrying the code", resp.Error.Data)
	}
	if got := data["code"]; got != CodeRuntimeNotFound {
		t.Errorf("data.code = %v, want %q", got, CodeRuntimeNotFound)
	}
	if got := data["runtime_id"]; got != wantID {
		t.Errorf("data.runtime_id = %v, want %q", got, wantID)
	}
	// The sentence stays byte-identical: a client released before the code
	// exists still matches it.
	if want := (&RuntimeNotFoundError{RuntimeID: wantID}).Error(); resp.Error.Message != want {
		t.Errorf("message = %q, want the sentence older clients match (%q)", resp.Error.Message, want)
	}
}
