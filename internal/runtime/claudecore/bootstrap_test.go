package claudecore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

const testRule = "────────────────────────────────────────"

// readyPane is what Claude Code 2.1.x renders once it is taking input. It has
// no "Tips for getting started" column and no "❯ Try" placeholder, just the
// framed empty input box.
const readyPane = " ▐▛███▜▌   Claude Code v2.1.219\n" +
	"▝▜█████▛▘  Opus 5 · Claude Team\n" +
	"  ▘▘ ▝▝    ~/.botfiles\n" +
	testRule + "\n" +
	"❯ \n" +
	testRule + "\n" +
	"   Opus 5   .botfiles:main\n"

func TestPaneHasInputBoxAcceptsModernReadyPane(t *testing.T) {
	if !paneHasInputBox(readyPane) {
		t.Fatal("Claude Code 2.1.x ready pane not recognised; prompts will never be submitted")
	}
}

func TestPaneHasInputBoxRejectsPanesThatAreNotTakingInput(t *testing.T) {
	tests := []struct {
		name string
		pane string
	}{
		{
			name: "empty pane",
			pane: "",
		},
		{
			name: "banner before the input box is framed",
			pane: " ▐▛███▜▌   Claude Code v2.1.219\n▝▜█████▛▘  Opus 5\n",
		},
		{
			// Options render as "❯ 1. Yes". A bare chevron pattern matches
			// these lines, so the paste dismisses the dialog instead of
			// submitting input.
			name: "selection dialog with a chevron-marked option",
			pane: "Quick safety check\n" + testRule + "\n❯ 1. Yes, I trust this folder\n  2. No\n" + testRule + "\n",
		},
		{
			name: "mcp server dialog with a chevron-marked option",
			pane: "New MCP server found in this project\n" + testRule + "\n❯ 1. Use this server\n  2. Skip\n" + testRule + "\n",
		},
		{
			// The echoed prompt line also starts with a chevron.
			name: "turn in flight with echoed prompt and spinner",
			pane: testRule + "\n❯ do the thing\n✻ Cogitating for 3s\n" + testRule + "\n",
		},
		{
			name: "chevron present but box never framed",
			pane: "❯ \nno rule on this screen\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if paneHasInputBox(tc.pane) {
				t.Fatalf("pane reported ready but is not taking input:\n%s", tc.pane)
			}
		})
	}
}

// nbspPane is a pane captured from Claude Code 2.1.303: the empty input box
// holds the placeholder `❯` + U+00A0 + `Try "..."`.
const nbspPane = " ▐▛███▛█   Claude Code v2.1.303\n" +
	"▝▜██████▀  Haiku 5.5 · Claude Team\n" +
	" ▝▝   ▝▝   /private/tmp\n\n" +
	testRule + "\n" +
	"❯ Try \"fix lint errors\"\n" +
	testRule + "\n" +
	"  ⏵⏵ auto mode on (shift+tab to cycle)\n"

func TestPaneIsReadyAcceptsNBSPPlaceholder(t *testing.T) {
	if !paneIsReady(nbspPane) {
		t.Fatalf("Claude Code 2.1.303 pane with NBSP placeholder not recognised:\n%s", nbspPane)
	}
}

func TestPaneIsReadyRejectsNBSPDialogAndEcho(t *testing.T) {
	for name, pane := range map[string]string{
		"selection dialog": "Quick safety check\n" + testRule + "\n❯ 1. Yes, I trust this folder\n  2. No\n" + testRule + "\n",
		"echoed prompt":    testRule + "\n❯ do the thing\n✻ Cogitating for 3s\n" + testRule + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			if paneIsReady(pane) {
				t.Fatalf("pane reported ready but is not taking input:\n%s", pane)
			}
		})
	}
}

func TestParseTerminalPermissionHandlesNBSPChevron(t *testing.T) {
	pane := "Do you want to proceed?\n❯ 1. Yes\n  2. No\n"
	perm := ParseTerminalPermission(pane)
	if perm == nil || len(perm.Options) != 2 {
		t.Fatalf("NBSP-chevron option not parsed: %+v", perm)
	}
}

func TestWaitForPaneReadySelectsTrustOptionBeforeConfirming(t *testing.T) {
	dialog := func(marker string) string {
		lines := []string{"No, exit", "Yes, I trust this folder"}
		out := " Quick safety check: Is this a project you created or one you trust?\n\n"
		for _, l := range lines {
			if l == marker {
				out += " ❯ " + l + "\n"
			} else {
				out += "   " + l + "\n"
			}
		}
		return out + " Enter to confirm · Esc to cancel\n"
	}
	term := terminal.NewFakeSessionQueue("trust", 1, []string{
		dialog("No, exit"),
		dialog("Yes, I trust this folder"),
		readyPane,
	})
	if err := WaitForPaneReady(context.Background(), term, 0); err != nil {
		t.Fatalf("WaitForPaneReady = %v, want nil once the trust dialog is cleared", err)
	}
	want := [][]string{{string(terminal.KeyDown)}, {string(terminal.KeyEnter)}}
	if got := term.SendCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("SendCalls = %v, want %v", got, want)
	}
}

func TestWaitForPaneReadyConfirmsTrustWhenStaleMarkerRemains(t *testing.T) {
	header := " Quick safety check: Is this a project you created or one you trust?\n"
	onExit := header + " ❯ No, exit\n   Yes, I trust this folder\n"
	onTrust := header + "   No, exit\n ❯ Yes, I trust this folder\n"
	// The repainted frame after a Down, with the old marker still on screen.
	stale := header +
		"   No, exit\n" +
		"─❯─Yes, I trust this folder──────\n" +
		" ❯ No, exit\n" +
		"   Yes, I trust this folder\n"
	cases := map[string][]string{
		// The unchanged capture after the Down gets no second Down.
		"stale marker after the Down": {onExit, onExit, stale, readyPane},
		// Without a Down, a stale trust marker beside a marked "No, exit"
		// is not taken as the selection.
		"stale marker before any Down": {stale, stale, onTrust, readyPane},
	}
	for name, captures := range cases {
		t.Run(name, func(t *testing.T) {
			term := terminal.NewFakeSessionQueue("trust", 1, captures)
			if err := WaitForPaneReady(context.Background(), term, 0); err != nil {
				t.Fatalf("WaitForPaneReady = %v, want nil once the trust dialog is cleared", err)
			}
			want := [][]string{{string(terminal.KeyDown)}, {string(terminal.KeyEnter)}}
			if got := term.SendCalls(); !reflect.DeepEqual(got, want) {
				t.Fatalf("SendCalls = %v, want %v", got, want)
			}
		})
	}
}

func TestWaitForPaneReadyReportsATerminalThatDiedShowingAReadyPane(t *testing.T) {
	// A PTY keeps its last screen after claude exits, so a dead terminal can
	// still show the input box. Pasting into it would report a turn that never
	// ran.
	term := terminal.NewFakeSession("dead", 1, readyPane)
	term.SetAlive(false)

	err := WaitForPaneReady(context.Background(), term, 4*PromptInjectCheckInterval)
	if !errors.Is(err, ErrTerminalGone) {
		t.Fatalf("WaitForPaneReady = %v, want ErrTerminalGone", err)
	}
}

func TestWaitForPaneReadyReportsATerminalThatDiedBeforeTheFirstCheck(t *testing.T) {
	// A deadline shorter than the check interval ends the wait before any
	// tick, so only the deadline's own liveness check can tell a dead terminal
	// from a stuck one.
	term := terminal.NewFakeSession("dead", 1, "still booting…\n")
	term.SetAlive(false)

	err := WaitForPaneReady(context.Background(), term, PromptInjectCheckInterval/5)
	if !errors.Is(err, ErrTerminalGone) {
		t.Fatalf("WaitForPaneReady = %v, want ErrTerminalGone", err)
	}
}
