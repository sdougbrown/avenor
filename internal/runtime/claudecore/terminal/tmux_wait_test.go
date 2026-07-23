package terminal

import (
	"context"
	"errors"
	"testing"
)

func TestTmuxSessionWaitHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&TmuxSession{name: "unused"}).Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context cancellation", err)
	}
}
