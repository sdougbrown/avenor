package poo

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

func TestMessagingPeerEnv(t *testing.T) {
	identitytest.PoisonParent(t)
	b := broker.New(broker.MakeToken())
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	token, ok, err := b.EnsureRun("rt_child", "")
	if err != nil || !ok {
		t.Fatalf("EnsureRun: %v %v", ok, err)
	}
	for _, tc := range []struct {
		name              string
		on, ready, broker bool
	}{
		{"on", true, true, true}, {"off", false, true, true}, {"not ready", true, false, true}, {"no broker", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			messagingReady = tc.ready
			t.Cleanup(func() { messagingReady = false })
			obs := filepath.Join(t.TempDir(), "observed")
			script := filepath.Join(t.TempDir(), "child")
			if err := os.WriteFile(script, []byte("#!/bin/sh\n"+identitytest.Body(obs)), 0700); err != nil {
				t.Fatal(err)
			}
			opts := runtime.StartOptions{Launch: backendlaunch.Spec{Command: script}, RuntimeID: "rt_child", BrokerToken: token, MessagingOn: tc.on}
			if tc.broker {
				opts.Broker = b
			}
			c, err := startClient(context.Background(), opts, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.shutdown()
			observed := identitytest.Read(t, obs)
			var want map[string]string
			if tc.on && tc.ready && tc.broker {
				want = map[string]string{"AVENOR_BROKER_URL": b.URL(), "AVENOR_RUN_ID": "rt_child", "AVENOR_BROKER_TOKEN": token}
			}
			identitytest.AssertPeerEnv(t, observed, want)
		})
	}
}
