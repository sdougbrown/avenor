package cli

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
