// Package miniswe runs the mini-swe-agent CLI as a one-shot backend.
package miniswe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/runtime/launcher"
)

const backend = "mini-swe-agent"

type Provider struct {
	opts     runtime.StartOptions
	mu       sync.Mutex
	sessions map[string]*session
}
type limitedOutput struct{ bytes.Buffer }

func (w *limitedOutput) Write(p []byte) (int, error) {
	const max = 64 << 10
	n := len(p)
	if remaining := max - w.Len(); remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = w.Buffer.Write(p)
	}
	return n, nil
}

type session struct {
	opts        runtime.StartOptions
	mu          sync.Mutex
	used        bool
	cancel      context.CancelFunc
	subscribers map[chan events.Event]struct{}
}

func NewWithOptions(opts runtime.StartOptions) *Provider {
	return &Provider{opts: opts, sessions: make(map[string]*session)}
}

func (p *Provider) Start(_ context.Context, opts runtime.StartOptions) (runtime.Session, error) {
	opts = runtime.MergeStartOptions(p.opts, opts)
	id := "mini-" + uuid.NewString()
	s := &session{opts: opts, subscribers: make(map[chan events.Event]struct{})}
	p.mu.Lock()
	p.sessions[id] = s
	p.mu.Unlock()
	off := false
	return runtime.Session{SessionID: id, Backend: backend, Dir: opts.Dir, MessagingOn: &off}, nil
}
func (p *Provider) get(id string) (*session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.sessions[id]
	if s == nil {
		return nil, fmt.Errorf("unknown session: %s", id)
	}
	return s, nil
}
func (p *Provider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{}, fmt.Errorf("mini-swe-agent does not support resume")
}
func (s *session) emit(e events.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subscribers {
		select {
		case ch <- e:
		default:
		}
	}
}
func (p *Provider) Prompt(ctx context.Context, id, prompt string) error {
	s, err := p.get(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.used {
		s.mu.Unlock()
		return fmt.Errorf("mini-swe-agent does not support follow-up prompts")
	}
	s.used = true
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.mu.Unlock()
	defer func() { cancel(); s.mu.Lock(); s.cancel = nil; s.mu.Unlock() }()
	s.emit(events.Event{Event: "session.start", SessionID: id, Fields: map[string]any{"backend": backend}})
	finish := func(reason, output string, err error) error {
		fields := map[string]any{"stop_reason": reason}
		if output != "" {
			fields["final_output"] = output
		}
		if err != nil {
			fields["error_message"] = err.Error()
		}
		s.emit(events.Event{Event: "session.end", SessionID: id, Fields: fields})
		return err
	}
	dir, err := os.MkdirTemp("", "avenor-mini-*")
	if err != nil {
		return finish("error", "", err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "trajectory.json")
	args := []string{"--task", prompt, "--yolo", "--exit-immediately", "--output", path}
	if s.opts.Model != "" {
		args = append(args, "--model", s.opts.Model)
	}
	cmd := launcher.Command(ctx, s.opts.LaunchWrapper, "mini", args...)
	cmd.Dir = s.opts.Dir
	cmd.Env = append(runtime.LaunchEnv(os.Environ(), s.opts.Provenance), "MSWEA_CONFIGURED=true")
	var output limitedOutput
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return finish("cancelled", "", ctx.Err())
		}
		return finish("error", "", fmt.Errorf("mini-swe-agent: %w: %s", err, output.String()))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return finish("error", "", fmt.Errorf("mini-swe-agent trajectory: %w", err))
	}
	var trajectory struct {
		Info struct {
			ExitStatus string `json:"exit_status"`
			Submission string `json:"submission"`
		} `json:"info"`
	}
	if err := json.Unmarshal(data, &trajectory); err != nil {
		return finish("error", "", fmt.Errorf("mini-swe-agent trajectory: %w", err))
	}
	if trajectory.Info.ExitStatus != "Submitted" {
		return finish("error", "", fmt.Errorf("mini-swe-agent stopped: %s", trajectory.Info.ExitStatus))
	}
	return finish("end_turn", trajectory.Info.Submission, nil)
}
func (p *Provider) Cancel(_ context.Context, id string) error {
	s, err := p.get(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}
func (p *Provider) Events(ctx context.Context, id string) (<-chan events.Event, error) {
	s, err := p.get(id)
	if err != nil {
		return nil, err
	}
	ch := make(chan events.Event, 128)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	go func() { <-ctx.Done(); s.mu.Lock(); delete(s.subscribers, ch); close(ch); s.mu.Unlock() }()
	return ch, nil
}
func (p *Provider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return fmt.Errorf("mini-swe-agent does not support permission requests")
}
func (p *Provider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{Backend: backend, ModelSelection: true}, nil
}
