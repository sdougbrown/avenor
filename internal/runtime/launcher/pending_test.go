package launcher

import (
	"context"
	"testing"
	"time"
)

func TestDetachedLaunchRemainsLiveUntilSettled(t *testing.T) {
	var pending Pending
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	adopted, detach, settleAdopted := pending.Begin(parent)
	starting, _, settleStarting := pending.Begin(context.Background())
	detach()

	done := make(chan struct{})
	go func() {
		pending.CancelAndWait()
		close(done)
	}()
	deadline, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	select {
	case <-starting.Done():
	case <-deadline.Done():
		t.Fatal("shutdown did not cancel the pending launch")
	}
	settleStarting(false)
	select {
	case <-adopted.Done():
		t.Fatal("shutdown canceled the adopted client")
	default:
	}
	select {
	case <-done:
		t.Fatal("shutdown returned before the adopted launch settled")
	default:
	}
	settleAdopted(true)
	select {
	case <-done:
	case <-deadline.Done():
		t.Fatal("shutdown did not return after both launches settled")
	}
	cancel()
	select {
	case <-adopted.Done():
	case <-deadline.Done():
		t.Fatal("adopted client no longer follows its caller")
	}
}
