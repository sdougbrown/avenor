package room

import "testing"

func TestGateAllowsNonWritesAlways(t *testing.T) {
	for _, kind := range []string{"read", "bash", "grep", ""} {
		if d := Gate("b", "a", kind, true); !d.Allow {
			t.Fatalf("kind %q should pass through: %+v", kind, d)
		}
	}
}

func TestGateSerialWindowAlwaysAllows(t *testing.T) {
	if d := Gate("b", "a", "write", false); !d.Allow {
		t.Fatalf("single-activation write should pass: %+v", d)
	}
}

func TestGateFirstWriterWinsSecondDenied(t *testing.T) {
	if d := Gate("a", "", "write", true); !d.Allow {
		t.Fatalf("first writer must be allowed: %+v", d)
	}
	d := Gate("b", "a", "edit", true)
	if d.Allow {
		t.Fatal("second writer must be denied during parallel window")
	}
	for _, want := range []string{"peer a", "re-read", "reconcile"} {
		if !contains2(d.Message, want) {
			t.Fatalf("denial message missing %q: %q", want, d.Message)
		}
	}
	// The holder may write again; the denied head may not.
	if d := Gate("a", "a", "write", true); !d.Allow {
		t.Fatalf("holder must keep write access: %+v", d)
	}
	if d := Gate("b", "a", "edit", true); d.Allow {
		t.Fatal("denied head must stay denied while holder set")
	}
}

func TestGateCaseInsensitiveKinds(t *testing.T) {
	if d := Gate("b", "a", "Write", true); d.Allow {
		t.Fatal("Write must be gated")
	}
}

func contains2(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestOptionIDSelection(t *testing.T) {
	opts := []any{
		map[string]any{"optionId": "yes", "kind": "allow"},
		map[string]any{"optionId": "no", "kind": "reject"},
	}
	if got := optionID(opts, "allow"); got != "yes" {
		t.Fatalf("allow option = %q", got)
	}
	if got := optionID(opts, "reject"); got != "no" {
		t.Fatalf("reject option = %q", got)
	}
	if got := optionID(nil, "allow"); got != "allow" {
		t.Fatalf("fallback option = %q", got)
	}
}
