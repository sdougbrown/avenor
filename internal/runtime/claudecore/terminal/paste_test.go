package terminal

import "testing"

func TestSanitizePromptDropsControlsKeepsText(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"escape sequences":  {"a\x1b[201~b\x1b[Zc", "a[201~b[Zc"},
		"C0 and DEL":        {"x\x00\x03\x7fy", "xy"},
		"kept whitespace":   {"l1\n\tl2\r", "l1\n\tl2\r"},
		"C1 controls":       {"p\u009b1~q\u0085", "p1~q"},
		"0x9b continuation": {"ě ✛", "ě ✛"},
		"invalid UTF-8":     {"a\x9b\xffb", "ab"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := SanitizePrompt(tc.in); got != tc.want {
				t.Fatalf("SanitizePrompt(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
