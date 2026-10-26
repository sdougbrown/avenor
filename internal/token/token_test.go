package token

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestNewHexTokenShape(t *testing.T) {
	token, err := NewHexToken()
	if err != nil {
		t.Fatalf("NewHexToken() = %q, %v", token, err)
	}
	if len(token) != 64 {
		t.Fatalf("NewHexToken() length = %d, want 64 hex characters", len(token))
	}
	if _, err := hex.DecodeString(token); err != nil {
		t.Fatalf("NewHexToken() = %q, want hex: %v", token, err)
	}
	second, err := NewHexToken()
	if err != nil {
		t.Fatalf("NewHexToken() = %q, %v", second, err)
	}
	if strings.EqualFold(token, second) {
		t.Fatal("NewHexToken() returned the same token twice")
	}
}

func TestNewHexSize(t *testing.T) {
	s, err := NewHex(16)
	if err != nil {
		t.Fatalf("NewHex(16) = %q, %v", s, err)
	}
	if len(s) != 32 {
		t.Fatalf("NewHex(16) length = %d, want 32 hex characters", len(s))
	}
	if _, err := hex.DecodeString(s); err != nil {
		t.Fatalf("NewHex(16) = %q, want hex: %v", s, err)
	}
}
