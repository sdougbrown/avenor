//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package terminal

import (
	"context"
	"errors"
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

	ps := &PTYSession{
		name:     opts.Name,
		cmd:      cmd,
		ptmx:     ptmx,
		terminal: vt10x.New(vt10x.WithSize(opts.Cols, opts.Rows)),
		alive:    true,
	}

	go ps.readLoop()

	return ps, nil
}

// PTYSession is a terminal.Session backed by a PTY.
type PTYSession struct {
	mu       sync.Mutex
	name     string
	cmd      *exec.Cmd
	ptmx     *os.File
	terminal vt10x.Terminal
	alive    bool
}

func (p *PTYSession) Kind() string { return "pty" }

func (p *PTYSession) Name() string { return p.name }

func (p *PTYSession) PID() int {
	if p.cmd != nil && p.cmd.Process != nil {
		return p.cmd.Process.Pid
	}
	return 0
}

func (p *PTYSession) Capture(_ context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.terminal.String(), nil
}

func (p *PTYSession) PasteAndEnter(_ context.Context, text string) error {
	p.mu.Lock()
	ptmx := p.ptmx
	p.mu.Unlock()
	if ptmx == nil {
		return fmt.Errorf("pty closed")
	}
	_, err := ptmx.Write([]byte(text + "\r"))
	return err
}

func encodeKeys(keys []Key) ([]byte, error) {
	buf := make([]byte, 0, len(keys))
	for _, k := range keys {
		switch k {
		case KeyEnter:
			buf = append(buf, '\r')
		case KeyEsc:
			buf = append(buf, '\x1b')
		default:
			if len(k) == 1 && k[0] >= 0x20 && k[0] <= 0x7e {
				buf = append(buf, k[0])
			} else {
				return nil, fmt.Errorf("unsupported key: %q", k)
			}
		}
	}
	return buf, nil
}

func (p *PTYSession) SendKeys(_ context.Context, keys ...Key) error {
	encoded, err := encodeKeys(keys)
	if err != nil {
		return err
	}
	p.mu.Lock()
	ptmx := p.ptmx
	p.mu.Unlock()
	if ptmx == nil {
		return fmt.Errorf("pty closed")
	}
	_, err = ptmx.Write(encoded)
	return err
}

func (p *PTYSession) Alive(_ context.Context) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.alive
}

func (p *PTYSession) Kill(_ context.Context) error {
	p.mu.Lock()
	ptmx := p.ptmx
	p.ptmx = nil
	p.alive = false
	p.mu.Unlock()

	if ptmx != nil {
		_ = ptmx.Close()
	}
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	return nil
}

func (p *PTYSession) readLoop() {
	defer func() {
		p.mu.Lock()
		p.alive = false
		p.mu.Unlock()
		// Reap the child so killed processes don't become zombies.
		if p.cmd != nil {
			_ = p.cmd.Wait()
		}
	}()
	// Snapshot ptmx under the lock — Kill() nils the field concurrently,
	// and Read on the closed fd is the loop's exit signal.
	p.mu.Lock()
	ptmx := p.ptmx
	p.mu.Unlock()
	if ptmx == nil {
		return
	}
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			p.mu.Lock()
			p.terminal.Write(buf[:n])
			p.mu.Unlock()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			// EIO/EBADF/closed-fd: the process died or the PTY was closed.
			return
		}
	}
}

var _ Session = (*PTYSession)(nil)
