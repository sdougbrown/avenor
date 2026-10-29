package broker

import (
	"encoding/json"
	"net/http"
)

func (b *Broker) routeLastMessage(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		b.withAuth(b.handleLastMessage)(w, r)
	case http.MethodPost:
		// A publish admits a turn into the peer's record, so the peer may not be
		// tombstoned underneath it; the read side admits nothing and takes no lock.
		b.withAuth(b.withPeerAdmission(b.handlePublishLastMessage))(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (b *Broker) handlePublishLastMessage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RunID       string  `json:"run_id"`
		Token       string  `json:"token"`
		FinalOutput *string `json:"final_output"`
		StopReason  string  `json:"stop_reason"`
		Truncated   bool    `json:"truncated"`
		Sequence    *uint64 `json:"sequence"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || body.FinalOutput == nil {
		writeCodedError(w, http.StatusBadRequest, "final_output and valid record fields are required", "invalid-request")
		return
	}
	st := b.GetRun(authenticatedCaller(r))
	if st == nil {
		writeCodedError(w, http.StatusNotFound, "run is no longer available", "target-unavailable")
		return
	}
	st.Mu.Lock()
	// This route publishes a peer's turn across a machine boundary, and only a
	// peer has a sequence to order those publishes by. A provisioned run records
	// its turns in process, through SetLastAgentMessage, where the writer is the
	// recorder itself; letting it in here would give a delayed or retried publish
	// a second, unordered way to overwrite a newer record.
	if !st.peer {
		st.Mu.Unlock()
		writeCodedError(w, http.StatusForbidden, "only a peer publishes its own last turn over the wire", "not-permitted")
		return
	}
	if body.Sequence == nil || *body.Sequence == 0 || *body.Sequence > 9007199254740991 {
		st.Mu.Unlock()
		writeCodedError(w, http.StatusBadRequest, "peer records require a positive safe-integer sequence", "invalid-request")
		return
	}
	if *body.Sequence > st.peerLastTurnSequence {
		setLastAgentMessageLocked(st, *body.FinalOutput, body.StopReason, body.Truncated)
		st.peerLastTurnSequence = *body.Sequence
	}
	st.Mu.Unlock()
	writeJSONBody(w, http.StatusOK, map[string]any{"ok": true})
}
