package runtime

import (
	"strings"
	"testing"
)

func TestValidatePermissionMessageEmpty(t *testing.T) {
	if err := ValidatePermissionMessage(""); err != nil {
		t.Errorf("empty message should be valid, got %v", err)
	}
}

func TestValidatePermissionMessageNormal(t *testing.T) {
	if err := ValidatePermissionMessage("approve with note: looks safe"); err != nil {
		t.Errorf("normal message should be valid, got %v", err)
	}
}

func TestValidatePermissionMessageUTF8(t *testing.T) {
	if err := ValidatePermissionMessage("café — naïve"); err != nil {
		t.Errorf("UTF-8 message should be valid, got %v", err)
	}
}

func TestValidatePermissionMessageOversized(t *testing.T) {
	long := strings.Repeat("x", MaxPermissionMessageBytes+1)
	err := ValidatePermissionMessage(long)
	if err == nil {
		t.Fatal("oversized message should be rejected")
	}
}

func TestValidatePermissionMessageAtBound(t *testing.T) {
	exact := strings.Repeat("x", MaxPermissionMessageBytes)
	if err := ValidatePermissionMessage(exact); err != nil {
		t.Errorf("message at exact bound should be valid, got %v", err)
	}
}

func TestValidatePermissionMessageInvalidUTF8(t *testing.T) {
	invalid := string([]byte{0xff, 0xfe, 0xfd})
	err := ValidatePermissionMessage(invalid)
	if err == nil {
		t.Fatal("invalid UTF-8 should be rejected")
	}
}

func TestValidatePermissionMessagePreservesText(t *testing.T) {
	// The validator must not trim or normalize — just validate.
	msg := "  leading and trailing spaces  \n\t"
	if err := ValidatePermissionMessage(msg); err != nil {
		t.Errorf("message with whitespace should be valid, got %v", err)
	}
	// Verify the constant is 64 KiB
	if MaxPermissionMessageBytes != 64<<10 {
		t.Errorf("MaxPermissionMessageBytes = %d, want %d", MaxPermissionMessageBytes, 64<<10)
	}
}

func TestDecodePermissionMessageJSONRejectsMalformedText(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte{'"', 0xff, '"'},
		[]byte(`"\uD800"`),
		[]byte(`"\uDC00"`),
		[]byte(`"\uD800x"`),
	} {
		if _, err := DecodePermissionMessageJSON(raw); err == nil {
			t.Fatalf("malformed JSON message accepted: %q", raw)
		}
	}
	message, err := DecodePermissionMessageJSON([]byte(`"\uD83D\uDE00"`))
	if err != nil || message != "😀" {
		t.Fatalf("surrogate pair = %q, %v", message, err)
	}
	message, err = DecodePermissionMessageJSON([]byte(`"\\uD800"`))
	if err != nil || message != `\uD800` {
		t.Fatalf("escaped literal = %q, %v", message, err)
	}
}
