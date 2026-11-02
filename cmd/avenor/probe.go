package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/opencodeacp"
	clicontract "github.com/sdougbrown/cli-contract"
)

func runProbe(args []string) int {
	return runDeclared("probe", args, handleProbe)
}

// handleProbe runs the probe and reports the stop reason's exit code.
func handleProbe(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	out := inv.Flags["out"]
	if out == "" {
		return nil, usageErrorf("avenor probe: --out is required")
	}
	dir := orDefault(inv.Flags["dir"], ".")
	timeout := inv.Durations["timeout"]
	if timeout == 0 {
		timeout = 5 * time.Minute
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	probePrompt := opencodeacp.ProbePrompt
	if prompt := inv.Flags["prompt"]; prompt != "" {
		probePrompt = prompt
	}
	transcript, err := opencodeacp.RunProbeWithPrompt(ctx, dir, probePrompt)
	if writeErr := writeTranscript(out, transcript); writeErr != nil {
		return nil, fmt.Errorf("avenor probe: write transcript: %w", writeErr)
	}
	if err != nil {
		return nil, fmt.Errorf("avenor probe: %w", err)
	}
	return exitCode(runtime.ExitCodeForStopReason(finalStopReason(transcript))), nil
}

func writeTranscript(path string, transcript []events.Event) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, event := range transcript {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return nil
}

func finalStopReason(transcript []events.Event) string {
	for i := len(transcript) - 1; i >= 0; i-- {
		if transcript[i].Event != "session.end" {
			continue
		}
		stopReason, _ := transcript[i].Fields["stop_reason"].(string)
		return stopReason
	}
	return ""
}
