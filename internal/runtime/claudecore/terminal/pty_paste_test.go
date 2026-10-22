//go:build linux || darwin || freebsd

package terminal

import (
	"strings"
	"testing"
)

func TestBracketedPasteCannotBeEndedByPromptText(t *testing.T) {
	// One-pass marker removal would rebuild ESC[201~ from the nested pieces.
	got := string(bracketedPaste("a\x1b[20\x1b[201~1~!rm\r\x1b[Z"))
	want := pasteStart + "a[20[201~1~!rm\r[Z" + pasteEnd
	if got != want {
		t.Fatalf("bracketedPaste = %q, want %q", got, want)
	}
	if strings.Count(got, "\x1b") != 2 {
		t.Fatalf("bracketedPaste = %q, want ESC only in the two markers", got)
	}
}
