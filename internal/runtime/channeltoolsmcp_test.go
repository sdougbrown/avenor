package runtime

import (
	"slices"
	"strings"
	"testing"
)

// TestChannelToolsMCPFor pins the one definition both ACP backends render: the
// server name, the argv order, and the token travelling in the environment
// rather than on the command line.
func TestChannelToolsMCPFor(t *testing.T) {
	server, ok := ChannelToolsMCPFor("/opt/avenor", "run_1", "tok_1", "http://127.0.0.1:9999")
	if !ok {
		t.Fatal("ChannelToolsMCPFor reported no server for complete broker coordinates")
	}
	if server.Name != "avenor-channel-tools" {
		t.Errorf("Name = %q, want avenor-channel-tools", server.Name)
	}
	if server.Command != "/opt/avenor" {
		t.Errorf("Command = %q, want /opt/avenor", server.Command)
	}
	wantArgs := []string{"channel-tools", "--run-id", "run_1", "--broker-url", "http://127.0.0.1:9999"}
	if !slices.Equal(server.Args, wantArgs) {
		t.Errorf("Args = %#v, want %#v", server.Args, wantArgs)
	}
	wantEnv := []EnvPair{{Name: EnvBrokerToken, Value: "tok_1"}}
	if !slices.Equal(server.Env, wantEnv) {
		t.Errorf("Env = %#v, want %#v", server.Env, wantEnv)
	}

	joined := strings.Join(server.Args, " ")
	if strings.Contains(joined, "--token") || strings.Contains(joined, "tok_1") {
		t.Errorf("Args carry the run token: %q", joined)
	}
}

func TestChannelToolsMCPForRequiresEveryCoordinate(t *testing.T) {
	cases := map[string]struct{ bin, runID, token, url string }{
		"no bin":         {"", "run_1", "tok_1", "http://127.0.0.1:9999"},
		"no run id":      {"/opt/avenor", "", "tok_1", "http://127.0.0.1:9999"},
		"no token":       {"/opt/avenor", "run_1", "", "http://127.0.0.1:9999"},
		"no broker url":  {"/opt/avenor", "run_1", "tok_1", ""},
		"nothing at all": {"", "", "", ""},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			server, ok := ChannelToolsMCPFor(c.bin, c.runID, c.token, c.url)
			if ok {
				t.Fatalf("ChannelToolsMCPFor reported a server: %#v", server)
			}
			if server.Name != "" || server.Command != "" || len(server.Args) != 0 || len(server.Env) != 0 {
				t.Fatalf("ChannelToolsMCPFor returned a half-built server: %#v", server)
			}
		})
	}
}
