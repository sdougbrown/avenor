package claudecore

import (
	"context"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const (
	testInterval = 5 * time.Millisecond
	testWindow   = 2 * time.Second
)

func unsentPane(box string) string {
	return testRule + "\n❯ " + box + "\n" + testRule + "\n"
}

const workingPane = "❯ do the thing\n✻ Cogitating for 3s\n" + testRule + "\n❯ \n" + testRule + "\n"

func TestResendEnterStopsOnceTurnIsActive(t *testing.T) {
	idle := unsentPane("do the thing")
	term := terminal.NewFakeSessionQueue("resend", 1, []string{idle, idle, idle, workingPane})
	resendEnterUntilSubmitted(context.Background(), term, "do the thing", nil, testInterval, testWindow)
	if got := len(term.SendCalls()); got != 3 {
		t.Fatalf("Enters sent = %d, want 3", got)
	}
}

func TestResendEnterMatchesPastedPlaceholder(t *testing.T) {
	for _, box := range []string{"[Pasted text #1]", "[Pasted text #2 +6 lines]"} {
		term := terminal.NewFakeSessionQueue("resend", 1, []string{unsentPane(box), workingPane})
		resendEnterUntilSubmitted(context.Background(), term, "a very long\nmulti-line prompt", nil, testInterval, testWindow)
		if got := len(term.SendCalls()); got != 1 {
			t.Fatalf("%q: Enters sent = %d, want 1", box, got)
		}
	}
}

func TestResendEnterIgnoresEchoedPromptAboveEmptyInputBox(t *testing.T) {
	echoed := "❯ do the thing\n⏺ OK\n" + testRule + "\n❯ \n" + testRule + "\n"
	term := terminal.NewFakeSession("resend", 1, echoed)
	resendEnterUntilSubmitted(context.Background(), term, "do the thing", nil, testInterval, testWindow)
	if got := len(term.SendCalls()); got != 0 {
		t.Fatalf("Enters sent = %d, want 0", got)
	}
}

func TestResendEnterHonoursStopSignal(t *testing.T) {
	term := terminal.NewFakeSession("resend", 1, unsentPane("do the thing"))
	calls := 0
	stop := func() bool { calls++; return calls > 2 }
	resendEnterUntilSubmitted(context.Background(), term, "do the thing", stop, testInterval, testWindow)
	if got := len(term.SendCalls()); got != 2 {
		t.Fatalf("Enters sent = %d, want 2", got)
	}
}

func TestResendEnterStopsAtDeadline(t *testing.T) {
	term := terminal.NewFakeSession("resend", 1, unsentPane("do the thing"))
	start := time.Now()
	resendEnterUntilSubmitted(context.Background(), term, "do the thing", nil, testInterval, 60*time.Millisecond)
	if time.Since(start) > time.Second {
		t.Fatal("loop outlived its window")
	}
	if got := len(term.SendCalls()); got == 0 || got > 12 {
		t.Fatalf("Enters sent = %d, want between 1 and 12", got)
	}
}
