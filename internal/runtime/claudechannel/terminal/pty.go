//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package terminal

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/chubin/vt10x"
	"github.com/creack/pty"
)

// PTYLauncher implements Launcher by spawning processes with a PTY.
type PTYLauncher struct {
	Env []string // environment to set; defaults to TERM/COLORTERM/FORCE_COLOR
}

func (l PTYLauncher) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Cols == 0 {
		opts.Cols = 220
	}
	if opts.Rows == 0 {
		opts.Rows = 50
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", opts.Command)
	cmd.Dir = opts.Dir

	// Set explicit terminal environment.
	env := l.Env
	if len(env) == 0 {
		env = []string{
			"TERM=tmux-256color",
			"COLORTERM=truecolor",
			"FORCE_COLOR=1",
		}
	}
	cmd.Env = append(os.Environ(), env...)

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Cols: uint16(opts.Cols),
		Rows: uint16(opts.Rows),
	})
	if err != nil {
		return nil, fmt.Errorf("pty start: %w", err)
	}

	terminal := vt10x.New(vt10x.WithSize(opts.Cols, opts.Rows))

	ps := &PTYSession{
		name:       opts.Name,
		cmd:        cmd,
		ptmx:       ptmx,
		terminal:   terminal,
		cancel:     ctx.Done(),
		capErr:     nil,
		pasteErr:   nil,
		sendErr:    nil,
		killErr:    nil,
		alive:      true,
	}

	go ps.readLoop()

	return ps, nil
}

// PTYSession is a terminal.Session backed by a PTY.
type PTYSession struct {
	name       string
	cmd        *exec.Cmd
	ptmx       *os.File
	terminal   vt10x.Terminal
	cancel     <-chan struct{}
	capErr     error
	pasteErr   error
	sendErr    error
	killErr    error
	alive      bool
	pasteCalls [][]string
	sendCalls  [][]string
	mu         sync.Mutex
}

func (p *PTYSession) Name() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.name
}

func (p *PTYSession) PID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil && p.cmd.Process != nil {
		return p.cmd.Process.Pid
	}
	return 0
}

func (p *PTYSession) Capture(_ context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.capErr != nil {
		return "", p.capErr
	}
	return p.terminal.String(), nil
}

func (p *PTYSession) PasteAndEnter(_ context.Context, text string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pasteCalls = append(p.pasteCalls, []string{text})
	if p.pasteErr != nil {
		return p.pasteErr
	}
	_, err := p.ptmx.Write([]byte(text + "\r"))
	return err
}

func (p *PTYSession) SendKeys(_ context.Context, keys ...Key) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	strs := make([]string, len(keys))
	for i, k := range keys {
		strs[i] = string(k)
	}
	p.sendCalls = append(p.sendCalls, strs)
	if p.sendErr != nil {
		return p.sendErr
	}
	for _, k := range keys {
		switch k {
		case KeyEnter:
			if _, err := p.ptmx.Write([]byte{'\r'}); err != nil {
				return err
			}
		case KeyEsc:
			if _, err := p.ptmx.Write([]byte{'\x1b'}); err != nil {
				return err
			}
		default:
			// Single ASCII chars written as literal bytes.
			if len(k) == 1 && k[0] >= 0x20 && k[0] <= 0x7e {
				if _, err := p.ptmx.Write([]byte{k[0]}); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("unsupported key: %q", k)
			}
		}
	}
	return nil
}

func (p *PTYSession) Alive(_ context.Context) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.alive
}

func (p *PTYSession) Kill(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.alive = false
	if p.ptmx != nil {
		_ = p.ptmx.Close()
	}
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	return p.killErr
}

func (p *PTYSession) SetCapErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capErr = err
}

func (p *PTYSession) SetPasteErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pasteErr = err
}

func (p *PTYSession) SetSendErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sendErr = err
}

func (p *PTYSession) SetKillErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.killErr = err
}

func (p *PTYSession) SetAlive(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.alive = v
}

func (p *PTYSession) PasteCalls() [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]string, len(p.pasteCalls))
	for i, c := range p.pasteCalls {
		out[i] = append([]string{}, c...)
	}
	return out
}

func (p *PTYSession) SendCalls() [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]string, len(p.sendCalls))
	for i, c := range p.sendCalls {
		out[i] = append([]string{}, c...)
	}
	return out
}

func (p *PTYSession) readLoop() {
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.alive = false
	}()
	buf := make([]byte, 4096)
	for {
		n, err := p.ptmx.Read(buf)
		if n > 0 {
			p.mu.Lock()
			p.terminal.Write(buf[:n])
			p.mu.Unlock()
		}
		if err != nil {
			if err == io.EOF {
				return
			}
			// EIO or other PTY read error means the process died or the PTY
			// was closed. Exit rather than spinning.
			select {
			case <-p.cancel:
				return
			default:
				return
			}
		}
	}
}

var _ Session = (*PTYSession)(nil)
