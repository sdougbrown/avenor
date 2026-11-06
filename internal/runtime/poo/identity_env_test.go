package poo

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
	"github.com/sdougbrown/avenor/internal/runtime/internal/identitytest"
)

func TestMessagingPeerEnv(t *testing.T) {
	identitytest.PoisonParent(t)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv(runtime.EnvPooFeedsPath, "/poison/feeds")
	t.Setenv(runtime.EnvPooCLIRegistryPath, "/poison/registry")
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
		name       string
		on, broker bool
	}{
		{"on", true, true}, {"off", false, true}, {"no broker", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			if tc.on && tc.broker {
				want = map[string]string{"AVENOR_BROKER_URL": b.URL(), "AVENOR_RUN_ID": "rt_child", "AVENOR_BROKER_TOKEN": token}
			}
			identitytest.AssertPeerEnv(t, observed, want)
			// The declared credential list travels with the trio, so poo can
			// keep the names for the feed and hide them from a tool's child.
			declared := observed[runtime.EnvPooCredentialEnv]
			if tc.on && tc.broker {
				wantDeclared := strings.Join([]string{runtime.EnvBrokerURL, runtime.EnvBrokerRunID, runtime.EnvBrokerToken}, string(os.PathListSeparator))
				if declared != wantDeclared {
					t.Errorf("%s = %q, want %q", runtime.EnvPooCredentialEnv, declared, wantDeclared)
				}
			} else if declared != "__UNSET__" {
				t.Errorf("%s = %q, want unset", runtime.EnvPooCredentialEnv, declared)
			}
			for _, key := range []string{runtime.EnvPooFeedsPath, runtime.EnvPooCLIRegistryPath} {
				value := observed[key]
				if tc.on && tc.broker {
					if !filepath.IsAbs(value) {
						t.Errorf("%s = %q, want absolute path", key, value)
					}
					if info, err := os.Stat(value); err != nil || !info.IsDir() {
						t.Errorf("%s = %q: %v", key, value, err)
					}
				} else if value != "__UNSET__" {
					t.Errorf("%s = %q, want unset", key, value)
				}
			}
		})
	}
}
