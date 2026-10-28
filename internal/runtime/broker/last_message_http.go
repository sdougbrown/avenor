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
		b.withAuth(b.handlePublishLastMessage)(w, r)
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
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || body.FinalOutput == nil {
		writeCodedError(w, http.StatusBadRequest, "final_output and valid record fields are required", "invalid-request")
		return
	}
	if err := b.SetLastAgentMessage(authenticatedCaller(r), *body.FinalOutput, body.StopReason, body.Truncated); err != nil {
		writeCodedError(w, http.StatusNotFound, "run is no longer available", "target-unavailable")
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{"ok": true})
}
