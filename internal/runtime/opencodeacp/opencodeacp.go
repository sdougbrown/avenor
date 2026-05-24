package opencodeacp

import (
	"context"
	"os/exec"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/acp"
)

const backendID = "opencode-acp"

var ProbePrompt = acp.ProbePrompt

func New() runtime.Provider {
	return NewWithOptions(runtime.StartOptions{})
}

func NewWithOptions(opts runtime.StartOptions) runtime.Provider {
	return acp.NewProvider(acp.ProviderConfig{
		BackendID:           backendID,
		Bin:                 "opencode",
		Args:                []string{"acp", "--pure", "--log-level", "WARN"},
		SubprocessDiscovery: true,
		AppendCWDArg:        true,
		ConfigureSession: func(ctx context.Context, session *acp.Session, opts runtime.StartOptions) error {
			return configureSession(ctx, session.Client, session.SessionID, opts)
		},
	})
}

// sessionConfigurer is the subset of *acp.Client needed for session configuration.
type sessionConfigurer interface {
	SetSessionMode(ctx context.Context, sessionID, modeID string) error
	SetSessionModel(ctx context.Context, sessionID, modelID string) error
}

func configureSession(ctx context.Context, cfg sessionConfigurer, sessionID string, opts runtime.StartOptions) error {
	if opts.Model != "" {
		if err := cfg.SetSessionModel(ctx, sessionID, opts.Model); err != nil {
			return err
		}
	}
	if opts.Agent != "" {
		if err := cfg.SetSessionMode(ctx, sessionID, opts.Agent); err != nil {
			return err
		}
	}
	return nil
}

func opencodeAvailable() bool {
	_, err := exec.LookPath("opencode")
	return err == nil
}

func RunProbe(ctx context.Context, dir string) ([]events.Event, error) {
	return RunProbeWithPrompt(ctx, dir, ProbePrompt)
}

func RunProbeWithPrompt(ctx context.Context, dir, prompt string) ([]events.Event, error) {
	client, err := acp.NewClient(ctx, acp.ClientConfig{
		Bin:                  "opencode",
		Args:                 []string{"acp", "--pure", "--log-level", "WARN"},
		Dir:                  dir,
		AutoAnswerPermission: true,
		AppendCWDArg:         true,
	})
	if err != nil {
		return nil, err
	}
	defer client.Close()

	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := client.Initialize(initCtx); err != nil {
		return nil, err
	}

	session, err := client.NewSession(initCtx)
	if err != nil {
		return nil, err
	}

	eventsCh := client.Events()
	promptDone := make(chan struct {
		event events.Event
		err   error
	}, 1)
	go func() {
		event, err := session.Prompt(ctx, prompt)
		promptDone <- struct {
			event events.Event
			err   error
		}{event: event, err: err}
	}()

	var transcript []events.Event
	for {
		select {
		case event, ok := <-eventsCh:
			if ok {
				transcript = append(transcript, event)
			}
		case result := <-promptDone:
			if result.err != nil {
				return transcript, result.err
			}
			transcript = append(transcript, result.event)
			return transcript, nil
		case <-ctx.Done():
			_ = session.Cancel()
			return transcript, ctx.Err()
		}
	}
}