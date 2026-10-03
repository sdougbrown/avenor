package room

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// JevGovernor is the System-One turn governor: after each activation it asks
// TypeSafe for typed judgments over bounded room state and chooses the next
// activation inside the deterministic envelope the coordinator enforces.
// It is control-plane only: it never edits files and never produces prose for
// participants. On any transport or decode error it falls back to the
// deterministic marker governor so the room keeps working without it.
type JevGovernor struct {
	APIKey    string
	Endpoint  string // default https://api.typesafe.ai/v1/systemone
	Model     string // default jev-latest
	Fallback  Governor
	Excerpt   int     // per-output bound in governor state
	Threshold float64 // confidence deadband; below this, return to the operator

	client *http.Client
}

func NewJevGovernor(apiKey string, fallback Governor) *JevGovernor {
	return &JevGovernor{
		APIKey:    apiKey,
		Endpoint:  "https://api.typesafe.ai/v1/systemone",
		Model:     "jev-latest",
		Fallback:  fallback,
		Excerpt:   800,
		Threshold: 0.35,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

type jevRequest struct {
	State     any                      `json:"state"`
	Model     string                   `json:"model"`
	Questions map[string]jevQuestionIn `json:"questions"`
}

type jevQuestionIn struct {
	Type         string `json:"type"`
	Instructions any    `json:"instructions"`
	Criteria     any    `json:"criteria,omitempty"`
}

type jevResponse struct {
	Answers map[string]struct {
		Type          string             `json:"type"`
		Noul          float64            `json:"noul,omitempty"`
		Choice        string             `json:"choice,omitempty"`
		Confidence    float64            `json:"confidence,omitempty"`
		Probabilities map[string]float64 `json:"probabilities,omitempty"`
	} `json:"answers"`
}

// Decide asks the model who needs a turn next. The deterministic envelope is
// enforced here and again by the coordinator: budget, depth cap, and the
// human-required short-circuit never depend on the model.
func (g *JevGovernor) Decide(s State) Decision {
	if s.BudgetRemaining <= 0 || s.Depth >= s.MaxDepth {
		return Decision{Reason: "envelope: budget/depth"}
	}
	state := g.state(s)
	choiceOpts := map[string]string{
		"human":     "the operator must speak next; the agents should stop and wait",
		"stop":      "the work for this request is done; return control to the operator",
		"all_heads": "every head other than the last speaker should react in parallel",
	}
	for _, p := range s.Participants {
		choiceOpts[p] = fmt.Sprintf("head %s should get the next turn", p)
	}
	req := jevRequest{
		Model: g.model(),
		State: state,
		Questions: map[string]jevQuestionIn{
			"next": {
				Type: "choice",
				Instructions: "One head just finished a turn in a shared room with an operator. " +
					"Who should speak next?",
				Criteria: choiceOpts,
			},
			"mode": {
				Type:         "choice",
				Instructions: "If a head is activated next, what kind of turn should it be?",
				Criteria: map[string]string{
					"continue":        "keep working on the operator's request",
					"review":          "examine the other head's work and critique it",
					"challenge":       "stress-test the other head's conclusion",
					"answer_peer":     "respond to a question or request from the other head",
					"handoff":         "take over the task from the other head",
					"recover_blocked": "help the other head past a blocker",
				},
			},
			"needs_human": {
				Type:         "noul",
				Instructions: "Does this situation require an operator decision before any head continues?",
				Criteria: map[string]string{
					"true":  "the agents are blocked, asked the operator a question, or the request is ambiguous",
					"false": "the agents can proceed or stop without operator input",
				},
			},
			"requests_peer": {
				Type:         "noul",
				Instructions: "Did the most recent speaker explicitly ask for the peer's input or review?",
				Criteria: map[string]string{
					"true":  "the last output explicitly requests peer review, help, or reaction",
					"false": "no explicit request for peer input",
				},
			},
		},
	}
	ans, err := g.ask(req)
	if err != nil {
		if g.Fallback != nil {
			d := g.Fallback.Decide(s)
			d.Reason = "jev unavailable (" + err.Error() + "); marker fallback: " + d.Reason
			return d
		}
		return Decision{Reason: "jev unavailable: " + err.Error()}
	}

	if ans["needs_human"].Noul >= 0.8 {
		return Decision{Reason: fmt.Sprintf("needs_human=%.2f → operator", ans["needs_human"].Noul)}
	}
	// Envelope rule: an explicit peer request is honored unless the operator
	// intervenes — the orientation header promises heads that the marker works,
	// so the governor may choose who reacts and in what mode, but not whether
	// the request is silently denied.
	reqPeer := ans["requests_peer"].Noul
	if reqPeer >= 0.8 && s.BudgetRemaining > 0 && s.Depth < s.MaxDepth {
		speaker := g.latestOther(s, "")
		if speaker != "" {
			lastAuthor := authorOf(s, speaker)
			targets := others(s, lastAuthor)
			if len(targets) > 0 {
				return Decision{
					Activate:       targets,
					SpeakerEventID: speaker,
					Mode:           ans["mode"].Choice,
					Reason: fmt.Sprintf("jev: peer request honored (requests_peer=%.2f) targets=%v mode=%s (model said next=%s p=%.2f)",
						reqPeer, targets, ans["mode"].Choice, ans["next"].Choice, ans["next"].Probabilities[ans["next"].Choice]),
				}
			}
		}
	}
	next := ans["next"].Choice
	conf := ans["next"].Confidence
	if conf < g.Threshold {
		return Decision{Reason: fmt.Sprintf("next=%s confidence=%.2f below deadband → operator", next, conf)}
	}
	if next == "human" || next == "stop" {
		return Decision{Reason: fmt.Sprintf("next=%s (p=%.2f)", next, ans["next"].Probabilities[next])}
	}
	if next == "all_heads" {
		// Fan the react round to every participant that did not produce the
		// chosen speaker event; the speaker is the latest other output.
		// all_heads targets everyone except the last speaker.
		speaker := g.latestOther(s, "")
		var lastAuthor string
		if speaker != "" {
			lastAuthor = authorOf(s, speaker)
		}
		targets := make([]string, 0, len(s.Participants))
		for _, p := range s.Participants {
			if p != lastAuthor {
				targets = append(targets, p)
			}
		}
		return Decision{
			Activate:       targets,
			SpeakerEventID: speaker,
			Mode:           ans["mode"].Choice,
			Reason:         fmt.Sprintf("jev: all_heads p=%.2f mode=%s needs_human=%.2f requests_peer=%.2f", conf, ans["mode"].Choice, ans["needs_human"].Noul, ans["requests_peer"].Noul),
		}
	}
	// next names a participant.
	if !contains(s.Participants, next) {
		return Decision{Reason: fmt.Sprintf("jev chose unknown participant %q → operator", next)}
	}
	speaker := g.latestOther(s, next)
	if speaker == "" {
		return Decision{Reason: "no peer output to react to → operator"}
	}
	return Decision{
		Activate:       []string{next},
		SpeakerEventID: speaker,
		Mode:           ans["mode"].Choice,
		Reason: fmt.Sprintf("jev: next=%s p=%.2f mode=%s needs_human=%.2f requests_peer=%.2f",
			next, ans["next"].Probabilities[next], ans["mode"].Choice, ans["needs_human"].Noul, ans["requests_peer"].Noul),
	}
}

// state assembles the bounded judgment state. Named fields only; every field
// is something the questions can reference.
func (g *JevGovernor) state(s State) map[string]any {
	excerpt := g.Excerpt
	if excerpt <= 0 {
		excerpt = 800
	}
	turns := make([]map[string]string, 0, len(s.Settled))
	var lastSpeaker string
	for _, a := range s.Settled {
		turns = append(turns, map[string]string{
			"id":      a.EventID,
			"head":    a.Participant,
			"depth":   fmt.Sprint(a.Depth),
			"mode":    a.Mode,
			"output":  Bound(a.FinalOutput, excerpt),
			"blocked": fmt.Sprint(a.Blocked),
		})
		lastSpeaker = a.Participant
	}
	return map[string]any{
		"human_request":      Bound(s.HumanInput, excerpt),
		"participants":       s.Participants,
		"peer_depth":         s.Depth,
		"max_peer_depth":     s.MaxDepth,
		"budget_remaining":   s.BudgetRemaining,
		"last_speaker":       lastSpeaker,
		"turns_this_request": turns,
	}
}

// latestOther returns the event ID of the most recent settled output not
// authored by exclude (empty exclude = latest output overall).
func (g *JevGovernor) latestOther(s State, exclude string) string {
	for i := len(s.Settled) - 1; i >= 0; i-- {
		if s.Settled[i].Participant != exclude {
			return s.Settled[i].EventID
		}
	}
	return ""
}

func authorOf(s State, eventID string) string {
	for _, a := range s.Settled {
		if a.EventID == eventID {
			return a.Participant
		}
	}
	return ""
}

func (g *JevGovernor) model() string {
	if g.Model == "" {
		return "jev-latest"
	}
	return g.Model
}

func (g *JevGovernor) ask(req jevRequest) (map[string]jevAnswer, error) {
	endpoint := g.Endpoint
	if endpoint == "" {
		endpoint = "https://api.typesafe.ai/v1/systemone"
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+g.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := g.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("typesafe api %d: %s", resp.StatusCode, Bound(string(raw), 300))
	}
	var parsed jevResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	out := make(map[string]jevAnswer, len(parsed.Answers))
	for k, v := range parsed.Answers {
		out[k] = jevAnswer{Noul: v.Noul, Choice: v.Choice, Confidence: v.Confidence, Probabilities: v.Probabilities}
	}
	return out, nil
}

type jevAnswer struct {
	Noul          float64
	Choice        string
	Confidence    float64
	Probabilities map[string]float64
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
