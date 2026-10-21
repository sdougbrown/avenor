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

	"github.com/sdougbrown/avenor/internal/runtime"
)

// PTYLauncher implements Launcher by spawning processes with a PTY.
type PTYLauncher struct {
	Env []string // environment to set; defaults to TERM/COLORTERM/FORCE_COLOR
	// AnswerProgStatus answers the hosted program's OSC 7501 detection query,
	// which is what turns its status reports on. Reports are observed either
	// way; device-attribute queries are never answered.
	AnswerProgStatus bool
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
	// Scrub the combined environment last, then add this launch's identity. The
	// hosted process is an independent Claude Code session, and inheriting the
	// launching session's marker turns a completed turn into a silent hang.
	// Scrubbing after the append also means a caller-provided Env entry cannot
	// reintroduce a scrubbed variable.
	combined := ScrubParentClaudeEnv(append(os.Environ(), env...))
	cmd.Env = runtime.LaunchEnv(combined, opts.Provenance)

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Cols: uint16(opts.Cols),
		Rows: uint16(opts.Rows),
	})
	if err != nil {
		return nil, fmt.Errorf("pty start: %w", err)
	}

	ps := &PTYSession{
		name:       opts.Name,
		cmd:        cmd,
		ptmx:       ptmx,
		terminal:   vt10x.New(vt10x.WithSize(opts.Cols, opts.Rows)),
		alive:      true,
		waitDone:   make(chan struct{}),
		answer:     l.AnswerProgStatus,
		writes:     make(chan ptyWrite),
		replies:    make(chan []byte, ptyReplyQueue),
		closed:     make(chan struct{}),
		writerDone: make(chan struct{}),
	}

	go ps.writeLoop(ptmx)
	go ps.readLoop()
	// Reap natural exits even if a caller only observes Alive. Wait still joins
	// this single reaper synchronously.
	ps.startWait()

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

	waitOnce sync.Once
	waitDone chan struct{}
	waitErr  error

	progStatusLog
	answer bool

	// writeLoop owns every ptmx write. Callers hand it requests on writes; the
	// reader hands it detection replies on replies without ever waiting.
	writes     chan ptyWrite
	replies    chan []byte
	closed     chan struct{}
	closeOnce  sync.Once
	writerDone chan struct{}
}

// ptyWrite is one caller's ordered writes. The writer performs them back to
// back, so nothing else reaches the program between them.
type ptyWrite struct {
	ctx    context.Context
	chunks [][]byte
	done   chan error
}

// ptyReplyQueue bounds detection replies waiting for the writer; a query that
// arrives while the queue is full goes unanswered.
const ptyReplyQueue = 4

// progStatusReply answers an OSC 7501 detection query.
const progStatusReply = "\x1b]7501;?\x1b\\"

var errPTYClosed = errors.New("pty closed")

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

func (p *PTYSession) PasteAndEnter(ctx context.Context, text string) error {
	return p.submit(ctx, bracketedPaste(text), []byte{'\r'})
}

const (
	pasteStart = "\x1b[200~"
	pasteEnd   = "\x1b[201~"
)

// bracketedPaste wraps text in the bracketed-paste markers Claude enables with
// ESC[?2004h. Text and Enter go out as separate writes: an Enter in the same
// burst as a long prompt is read as part of a paste and dropped, so the prompt
// never submits. The text is passed through SanitizePrompt first, so no ESC
// survives and prompt content cannot end the paste early or send keys.
func bracketedPaste(text string) []byte {
	return []byte(pasteStart + SanitizePrompt(text) + pasteEnd)
}

func encodeKeys(keys []Key) ([]byte, error) {
	buf := make([]byte, 0, len(keys))
	for _, k := range keys {
		switch k {
		case KeyEnter:
			buf = append(buf, '\r')
		case KeyEsc:
			buf = append(buf, '\x1b')
		case KeyDown:
			buf = append(buf, "\x1b[B"...)
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

func (p *PTYSession) SendKeys(ctx context.Context, keys ...Key) error {
	encoded, err := encodeKeys(keys)
	if err != nil {
		return err
	}
	return p.submit(ctx, encoded)
}

// submit hands chunks to the writer and waits for the result. A caller whose
// ctx ends gets ctx.Err(); a chunk already being written still completes, and
// the request's later chunks are skipped.
func (p *PTYSession) submit(ctx context.Context, chunks ...[]byte) error {
	select {
	case <-p.closed:
		return errPTYClosed
	default:
	}
	w := ptyWrite{ctx: ctx, chunks: chunks, done: make(chan error, 1)}
	select {
	case p.writes <- w:
	case <-p.closed:
		return errPTYClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-w.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		select {
		case err := <-w.done:
			return err
		default:
			return errPTYClosed
		}
	}
}

func (p *PTYSession) writeLoop(ptmx *os.File) {
	defer close(p.writerDone)
	for {
		// A reply waiting alongside a request goes first; either way it is
		// written between requests, never inside one.
		select {
		case b := <-p.replies:
			p.writeReply(ptmx, b)
			continue
		default:
		}
		select {
		case <-p.closed:
			return
		case b := <-p.replies:
			p.writeReply(ptmx, b)
		case w := <-p.writes:
			select {
			case <-p.closed:
				w.done <- errPTYClosed
			default:
				w.done <- writeChunks(w.ctx, ptmx, w.chunks)
			}
		}
	}
}

func writeChunks(ctx context.Context, ptmx *os.File, chunks [][]byte) error {
	for _, c := range chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := ptmx.Write(c); err != nil {
			return err
		}
	}
	return nil
}

func (p *PTYSession) writeReply(ptmx *os.File, b []byte) {
	if _, err := ptmx.Write(b); err == nil {
		p.psMarkAnswered()
	}
}

// ProgStatusAnswering reports whether this session answers OSC 7501
// detection queries.
func (p *PTYSession) ProgStatusAnswering() bool { return p.answer }

// answerQuery queues a detection reply. It runs on the reader and must not
// block: a writer stuck on a program that stopped reading input would
// otherwise stop the reader draining that program's output.
func (p *PTYSession) answerQuery() {
	if !p.answer {
		return
	}
	select {
	case p.replies <- []byte(progStatusReply):
	default:
	}
}

func (p *PTYSession) markClosed() {
	p.closeOnce.Do(func() { close(p.closed) })
}

func (p *PTYSession) Alive(_ context.Context) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.alive
}

// Interrupt asks this exact hosted process to stop gracefully. It deliberately
// does not derive or signal any other PID/process group.
func (p *PTYSession) Interrupt(_ context.Context) error {
	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Signal(os.Interrupt); errors.Is(err, os.ErrProcessDone) {
		return nil
	} else {
		return err
	}
}

// Kill rejects further writes and kills the process. The pty master stays
// open until the reaper sees the exit: a killed process can block in exit
// until its pending output is read, so the reader must keep draining, and a
// blocked master write only returns once the child's side has closed.
func (p *PTYSession) Kill(_ context.Context) error {
	p.mu.Lock()
	p.alive = false
	p.mu.Unlock()

	p.markClosed()
	var err error
	if p.cmd != nil && p.cmd.Process != nil {
		err = p.cmd.Process.Kill()
		if errors.Is(err, os.ErrProcessDone) {
			err = nil
		}
	}
	// Preserve the old Kill-only lifecycle while keeping cmd.Wait ownership in
	// one place. Wait callers still synchronously observe the same completion.
	p.startWait()
	return err
}

// Wait waits for the command's single reaper. cmd.Wait is invoked exactly once
// by startWait, even when Kill and several Wait callers race.
func (p *PTYSession) Wait(ctx context.Context) error {
	p.startWait()
	select {
	case <-p.waitDone:
		p.mu.Lock()
		err := p.waitErr
		p.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PTYSession) startWait() {
	p.waitOnce.Do(func() {
		go func() {
			var err error
			if p.cmd != nil {
				err = p.cmd.Wait()
			}
			p.mu.Lock()
			p.waitErr = err
			p.alive = false
			ptmx := p.ptmx
			p.ptmx = nil
			p.mu.Unlock()
			p.markClosed()
			if ptmx != nil {
				_ = ptmx.Close()
			}
			close(p.waitDone)
		}()
	})
}

var _ InterruptSession = (*PTYSession)(nil)
var _ ProgStatusSession = (*PTYSession)(nil)

// readLoop renders output into the screen emulator and feeds the same bytes to
// the OSC 7501 parser. The parser runs outside p.mu, and nothing it calls
// waits on the writer.
func (p *PTYSession) readLoop() {
	defer func() {
		p.mu.Lock()
		p.alive = false
		p.mu.Unlock()
		p.psDropOnExit()
	}()
	parser := NewProgStatusParser(nil, ProgStatusHooks{
		OnQuery:  p.answerQuery,
		OnReport: p.psRecord,
		OnReset:  p.psReset,
	})
	// Snapshot ptmx under the lock — the reaper nils the field concurrently,
	// and Read failing once the process is gone is the loop's exit signal.
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
			parser.Feed(buf[:n])
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
