package broker

import (
	"net/http"
	"testing"
	"time"
)

func TestLastPolled(t *testing.T) {
	var nilBroker *Broker
	if _, ok := nilBroker.LastPolled("x"); ok {
		t.Fatal("nil broker has a run")
	}
	b := New("")
	b.pollTimeout = time.Millisecond
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	if _, ok := b.LastPolled("missing"); ok {
		t.Fatal("unknown run exists")
	}
	token, err := b.CreateRun("child", "")
	if err != nil {
		t.Fatal(err)
	}
	poll := func(id, token string, want int) {
		t.Helper()
		code, _ := peerPost(t, b, "/poll-control", map[string]any{"run_id": id, "token": token})
		if code != want {
			t.Fatalf("poll status = %d, want %d", code, want)
		}
	}
	poll("child", token, http.StatusOK)
	first, ok := b.LastPolled("child")
	if !ok || first.IsZero() {
		t.Fatal("missing first stamp")
	}
	poll("child", "wrong", http.StatusUnauthorized)
	if got, _ := b.LastPolled("child"); !got.Equal(first) {
		t.Fatal("rejected poll changed stamp")
	}
	poll("child", token, http.StatusOK)
	second, _ := b.LastPolled("child")
	if !second.After(first) {
		t.Fatal("second poll did not advance stamp")
	}
	creds := map[string]any{"run_id": "peer", "token": "peer-secret", "peer": true}
	if code, _ := peerPost(t, b, "/register", creds); code != http.StatusOK {
		t.Fatalf("register = %d", code)
	}
	st := b.GetRun("peer")
	st.Mu.Lock()
	st.peerDeadline = time.Now().Add(time.Second)
	before := st.peerDeadline
	st.Mu.Unlock()
	poll("peer", "peer-secret", http.StatusOK)
	stamp, ok := b.LastPolled("peer")
	st.Mu.Lock()
	deadline, seen := st.peerDeadline, st.LastSeen
	st.Mu.Unlock()
	if !ok || stamp.IsZero() || !deadline.After(before) || seen.Before(stamp) {
		t.Fatal("peer poll did not stamp and renew lease")
	}
}
