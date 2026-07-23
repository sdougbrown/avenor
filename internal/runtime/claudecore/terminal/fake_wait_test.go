package terminal

import (
	"context"
	"testing"
	"time"
)

func TestFakeSessionWaitTracksExit(t *testing.T) {
	session := NewFakeSession("fake", 1, "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.Kill(ctx); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if err := session.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if session.KillCalls() != 1 || session.WaitCalls() != 1 || session.Alive(ctx) {
		t.Fatalf("fake lifecycle kill=%d wait=%d alive=%v", session.KillCalls(), session.WaitCalls(), session.Alive(ctx))
	}
}
