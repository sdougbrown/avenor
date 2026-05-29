package teamrunner

import (
	"context"
)

type RunResult struct {
	ExitCode   int
	StopReason string
	SessionID  string
	Reason     string
}

type RunOptions struct {
	WorkDir      string
	RunID        string
	EventSink    interface{}
	Config       *TeamConfig
	MaxRetries   int
	PhaseAttempt func(ctx context.Context, phase Phase, attemptNum int, prevSessionID string) (PhaseAttemptResult, error)
	ConditionalSkip func(skippedNames []string)
}

type PhaseAttemptResult struct {
	ExitCode      int
	SessionID     string
	StopReason    string
	LoopDirective string
	LoopLabel     string
}

func Run(ctx context.Context, opts RunOptions) (RunResult, error) {
	return RunResult{}, nil
}