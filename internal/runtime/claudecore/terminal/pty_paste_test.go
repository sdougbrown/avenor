//go:build linux || darwin || freebsd

package terminal

import "testing"

func TestBracketedPasteStripsEmbeddedMarkers(t *testing.T) {
	got := string(bracketedPaste("a\x1b[201~b\x1b[200~c"))
	want := "\x1b[200~abc\x1b[201~"
	if got != want {
		t.Fatalf("bracketedPaste = %q, want %q", got, want)
	}
}
