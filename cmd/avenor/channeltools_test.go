package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestChannelToolsCoordinatesFromEnv drives the MCP server with no coordinate
// flags at all: the run id, token, and broker URL arrive only through the peer
// environment, which is how a spawning backend hands over the run token without
// putting it on an argv readable through /proc/<pid>/cmdline.
func TestChannelToolsCoordinatesFromEnv(t *testing.T) {
	if os.Getenv("AVENOR_CHANNEL_TOOLS_ENV_CHILD") == "1" {
		os.Exit(runDeclared([]string{"channel-tools"}, os.Stdout, os.Stderr))
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestChannelToolsCoordinatesFromEnv$")
	cmd.Env = append(filteredEnviron("AVENOR_RUN_ID", "AVENOR_BROKER_TOKEN", "AVENOR_BROKER_URL"),
		"AVENOR_CHANNEL_TOOLS_ENV_CHILD=1",
		"AVENOR_RUN_ID=run_env",
		"AVENOR_BROKER_TOKEN=tok_env",
		"AVENOR_BROKER_URL=http://127.0.0.1:9",
	)
	cmd.Stdin = strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}` + "\n")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("channel-tools with env coordinates: %v\nstderr: %s", err, stderr.String())
	}
	if strings.Contains(stderr.String(), "are required") {
		t.Fatalf("channel-tools rejected env coordinates: %s", stderr.String())
	}

	var replied struct {
		Result struct {
			ServerInfo struct {
				Name string `json:"name"`
			} `json:"serverInfo"`
		} `json:"result"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &replied); err != nil {
		t.Fatalf("decode initialize reply %q: %v", stdout.String(), err)
	}
	if replied.Result.ServerInfo.Name != "avenor-channel-tools" {
		t.Fatalf("initialize serverInfo.name = %q, want avenor-channel-tools", replied.Result.ServerInfo.Name)
	}
}

// filteredEnviron returns the process environment without the named variables,
// so an inherited peer credential cannot satisfy the test by accident.
func filteredEnviron(drop ...string) []string {
	keep := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		dropped := false
		for _, candidate := range drop {
			if name == candidate {
				dropped = true
				break
			}
		}
		if !dropped {
			keep = append(keep, entry)
		}
	}
	return keep
}
