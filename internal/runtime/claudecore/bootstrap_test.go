package claudecore

import "testing"

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
