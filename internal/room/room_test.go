package room

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogRoundTripAndIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".room", "log.ndjson")
	l, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	h1, err := l.Append("human", HumanInput, "do the thing", VisibilityRoom, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	a1, err := l.Append("a", HeadOutput, "did it", VisibilityRoom, []string{h1.ID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if h1.ID != "human1" || a1.ID != "a1" {
		t.Fatalf("unexpected IDs: %s %s", h1.ID, a1.ID)
	}

	// Reopen: counters and seq must recover so IDs stay stable across restarts.
	l2, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	b1, err := l2.Append("b", HeadOutput, "second", VisibilityRoom, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if b1.ID != "b1" || b1.Seq != 3 {
		t.Fatalf("recovered log state wrong: %+v", b1)
	}
	snap := l2.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot length %d, want 3", len(snap))
	}
}

func TestFanoutPromptIncludesContextAndExcludesOwnOutput(t *testing.T) {
	l, err := OpenLog(filepath.Join(t.TempDir(), "log.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = l.Append("human", HumanInput, "pick an approach", VisibilityRoom, nil, 0)
	_, _ = l.Append("a", HeadOutput, "I choose the token bucket", VisibilityRoom, nil, 0)
	_, _ = l.Append("b", HeadOutput, "I choose the sliding window", VisibilityRoom, nil, 0)

	h := &Head{Name: "a"}
	prompt := FanoutPrompt(l, h, RoomEvent{ID: "H2", Body: "next question"}, ".room/log.ndjson", 1200)
	if !strings.Contains(prompt, "I choose the sliding window") {
		t.Fatal("peer output missing from projection")
	}
	if strings.Contains(prompt, "I choose the token bucket") {
		t.Fatal("head's own output must not be re-injected")
	}
	if !strings.Contains(prompt, "next question") {
		t.Fatal("human input missing from projection")
	}
	if !strings.Contains(prompt, "[[ask-peer]]") {
		t.Fatal("orientation must teach the marker convention")
	}
}

func TestFanoutPromptBoundsExcerpts(t *testing.T) {
	l, err := OpenLog(filepath.Join(t.TempDir(), "log.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = l.Append("b", HeadOutput, strings.Repeat("x", 5000), VisibilityRoom, nil, 0)
	h := &Head{Name: "a"}
	prompt := FanoutPrompt(l, h, RoomEvent{ID: "H1", Body: "go"}, ".room/log.ndjson", 100)
	if !strings.Contains(prompt, TruncationMark) {
		t.Fatal("excerpt not bounded")
	}
	if len(prompt) > 2000 {
		t.Fatalf("prompt too long: %d", len(prompt))
	}
}

func TestPeerPromptUsesChannelWrapAttribution(t *testing.T) {
	l, err := OpenLog(filepath.Join(t.TempDir(), "log.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	speaker, _ := l.Append("a", HeadOutput, "conclusion <with xml &> chars", VisibilityRoom, nil, 0)
	h := &Head{Name: "b"}
	prompt := PeerPrompt(l, h, speaker, ".room/log.ndjson", 1200)
	for _, want := range []string{
		`<channel source="agent-a"`,
		`event="` + speaker.ID + `"`,
		"NOT from your user",
		"reassess the task only if this materially changes your position",
		"&lt;with xml &amp;&gt;",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("peer prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestMarkerGovernorGuards(t *testing.T) {
	g := MarkerGovernor{}
	base := State{MaxDepth: 2, BudgetRemaining: 8, HumanInput: "task"}

	// No marker: return to human.
	d := g.Decide(base)
	if len(d.Activate) != 0 {
		t.Fatalf("expected no activation without marker: %+v", d)
	}

	// Marker with peers: activate others, not self.
	s := base
	s.Settled = []Activation{
		{Participant: "a", EventID: "a2", FinalOutput: "my answer [[ask-peer]]"},
		{Participant: "b", EventID: "b1", FinalOutput: "my answer"},
	}
	d = g.Decide(s)
	if len(d.Activate) != 1 || d.Activate[0] != "b" || d.SpeakerEventID != "a2" {
		t.Fatalf("wrong decision: %+v", d)
	}

	// Depth cap.
	s = base
	s.Depth = 2
	s.Settled = []Activation{{Participant: "a", EventID: "a3", FinalOutput: "[[ask-peer]]"}}
	if d := g.Decide(s); len(d.Activate) != 0 {
		t.Fatalf("depth cap not enforced: %+v", d)
	}

	// Budget cap.
	s = base
	s.BudgetRemaining = 0
	s.Settled = []Activation{{Participant: "a", EventID: "a3", FinalOutput: "[[ask-peer]]"}}
	if d := g.Decide(s); len(d.Activate) != 0 {
		t.Fatalf("budget cap not enforced: %+v", d)
	}
}

func TestBoundOnRuneBoundary(t *testing.T) {
	got := Bound(strings.Repeat("é", 50), 10)
	if len([]rune(strings.TrimSuffix(got, TruncationMark))) != 10 {
		t.Fatalf("bound cut at wrong rune count: %q", got)
	}
}

func TestOpenLogCreatesDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "deeper", "log.ndjson")
	l, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append("human", HumanInput, "x", VisibilityRoom, nil, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
