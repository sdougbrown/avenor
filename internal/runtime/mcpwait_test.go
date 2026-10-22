package runtime

import (
	"testing"
	"time"
)

func TestValidateClaudeMCPWait(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend string
		wait    time.Duration
		wantErr bool
	}{
		{"zero on any backend", "agy", 0, false},
		{"claude", "claude", 5 * time.Second, false},
		{"claude-channel", "claude-channel", 5 * time.Second, false},
		{"at cap", "claude", MaxClaudeMCPWait, false},
		{"over cap", "claude", MaxClaudeMCPWait + time.Nanosecond, true},
		{"negative", "claude", -time.Second, true},
		{"other backend", "agy", time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateClaudeMCPWait(tc.backend, tc.wait)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
