package claudecore

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

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

// Captures of the trust dialog, from Claude Code 2.1.325 driven through the PTY
// launcher. Claude draws the dialog with "No, exit" selected. When the
// selection moves it redraws the option pair at the top of the screen and
// leaves the pair from the first paint in place, marker included, so a settled
// frame marks both options. Only the top pair tracks the selection: the redraw
// that follows a Down at first paint puts that marker back on "No, exit" about
// 130ms later and never touches the leftover pair. Long rule runs are trimmed
// to testRule.
const (
	trustHeader = " Quick safety check: Is this a project you created or one you trust?\n"

	// trustFirstPaint is the dialog's first draw.
	trustFirstPaint = trustHeader +
		"\n Claude Code'll be able to read, edit, and execute files here.\n" +
		"\n Security guide\n" +
		"\n ❯ No, exit\n" +
		"   Yes, I trust this folder\n" +
		"\n Enter to confirm · Esc to cancel\n"

	// trustOnTrust is the frame a Down that lands after the redraw settles on.
	trustOnTrust = "   No, exit\n" +
		"─❯─Yes, I trust this folder" + testRule + "\n" +
		trustFirstPaint

	// trustRepaintedExit is the frame about 130ms after a Down at first paint:
	// the redrawn marker is back on "No, exit" and the trust option keeps only
	// the rule residue of the row it landed on.
	trustRepaintedExit = " ❯ No, exit\n" +
		"─ ─Yes, I trust this folder" + testRule + "\n" +
		trustFirstPaint

	// trustSelected is what a release that draws the trust option selected puts
	// on screen.
	trustSelected = trustHeader + "\n ❯ Yes, I trust this folder\n"

	// trustStaleTrust is the shape a leftover trust marker would take: the live
	// pair at the top on "No, exit", a marked trust line further down.
	trustStaleTrust = " ❯ No, exit\n" +
		"─ ─Yes, I trust this folder" + testRule + "\n" +
		trustHeader + "\n ❯ Yes, I trust this folder\n"
)

// trustDialogKeys feeds captures to a trustDialogState one check interval
// apart and returns the keys it asked for.
func trustDialogKeys(frames ...string) []terminal.Key {
	base := time.Unix(1700000000, 0)
	var d trustDialogState
	var keys []terminal.Key
	for i, frame := range frames {
		if k, ok := d.next(frame, base.Add(time.Duration(i)*PromptInjectCheckInterval)); ok {
			keys = append(keys, k)
		}
	}
	return keys
}

func TestTrustDialogStateConvergesAfterARedraw(t *testing.T) {
	// First paint, the frame the Down at first paint produces, the redraw that
	// restores "No, exit", then the frame a second Down settles on.
	got := trustDialogKeys(
		trustFirstPaint,
		trustOnTrust,
		trustRepaintedExit,
		trustOnTrust,
		trustOnTrust,
	)
	want := []terminal.Key{terminal.KeyDown, terminal.KeyDown, terminal.KeyEnter}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
}

func TestTrustDialogStateHoldsEnterUntilTheTrustOptionStaysSelected(t *testing.T) {
	base := time.Unix(1700000000, 0)
	var d trustDialogState
	if k, ok := d.next(trustOnTrust, base); ok {
		t.Fatalf("first capture showing the trust option selected sent %q, want nothing", k)
	}
	if k, ok := d.next(trustOnTrust, base.Add(trustDialogConfirmHold-time.Millisecond)); ok {
		t.Fatalf("capture inside the hold sent %q, want nothing", k)
	}
	if k, ok := d.next(trustOnTrust, base.Add(trustDialogConfirmHold)); !ok || k != terminal.KeyEnter {
		t.Fatalf("capture past the hold sent %q, %v; want Enter", k, ok)
	}
}

func TestTrustDialogStateStopsSendingKeysWhenAPressDoesNotConverge(t *testing.T) {
	var d trustDialogState
	base := time.Unix(1700000000, 0)
	presses := 0
	for i := 0; i < 20; i++ {
		k, ok := d.next(trustFirstPaint, base.Add(time.Duration(i)*PromptInjectCheckInterval))
		if !ok {
			continue
		}
		if k != terminal.KeyDown {
			t.Fatalf("capture %d sent %q while the exit option was selected, want a Down or nothing", i, k)
		}
		presses++
	}
	if presses != trustDialogPressBudget {
		t.Fatalf("dialog that never converged got %d Downs, want %d", presses, trustDialogPressBudget)
	}
}

func TestTrustDialogStateTreatsTheTopmostMarkedOptionAsTheSelection(t *testing.T) {
	// Enter on a frame whose live marker is on "No, exit" exits Claude, so a
	// trust marker further down the screen is never taken as the selection,
	// however long the dialog has been up.
	var d trustDialogState
	base := time.Unix(1700000000, 0)
	for i := 0; i < 5; i++ {
		if k, ok := d.next(trustStaleTrust, base.Add(time.Duration(i)*time.Minute)); ok && k != terminal.KeyDown {
			t.Fatalf("frame %d sent %q with the exit option selected, want a Down", i, k)
		}
	}
}

func TestTrustDialogStateNeverConfirmsAFrameWithNoSelectionMarked(t *testing.T) {
	// A frame caught mid-paint can name both options with no marker. The
	// selection is unknown there, so Enter is never sent however long it stays.
	unmarked := trustHeader + "\n   No, exit\n   Yes, I trust this folder\n"
	var d trustDialogState
	base := time.Unix(1700000000, 0)
	for i := 0; i < 5; i++ {
		if k, ok := d.next(unmarked, base.Add(time.Duration(i)*time.Minute)); ok && k != terminal.KeyDown {
			t.Fatalf("frame %d sent %q with no option marked, want a Down", i, k)
		}
	}
}

func TestWaitForPaneReadySelectsTrustOptionBeforeConfirming(t *testing.T) {
	term := terminal.NewFakeSessionQueue("trust", 1, []string{
		trustFirstPaint,
		trustOnTrust,
		trustOnTrust,
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

func TestWaitForPaneReadyConfirmsTrustWhenTheFirstPaintMarkerRemains(t *testing.T) {
	// The first paint's marker outlives the redraw, so the handler meets frames
	// that mark both options. It must confirm the trust option there rather
	// than press Down and move the selection back to "No, exit".
	cases := map[string][]string{
		"both options marked":     {trustOnTrust, trustOnTrust, readyPane},
		"trust selected at first": {trustSelected, trustSelected, readyPane},
	}
	for name, captures := range cases {
		t.Run(name, func(t *testing.T) {
			term := terminal.NewFakeSessionQueue("trust", 1, captures)
			if err := WaitForPaneReady(context.Background(), term, 0); err != nil {
				t.Fatalf("WaitForPaneReady = %v, want nil once the trust dialog is cleared", err)
			}
			want := [][]string{{string(terminal.KeyEnter)}}
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
