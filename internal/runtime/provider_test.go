package runtime

import (
	"strings"
	"testing"
	"unicode/utf8"
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
	// Verify UTF-8 import is used (not a real test, just prevents unused import)
	_ = utf8.Valid
}