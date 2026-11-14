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
	"syscall"
	"time"

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

// ptyCommand builds the hosted command. Every provisioned value travels in
// cmd.Env, which is the process environment and readable by its own user only;
// none of it is rendered into argv, which any local process can read. baseEnv
// defaults to the terminal defaults when the launcher carries none. hup is nil
// for a session that did not ask for a hangup grace, and exec then kills the
// hosted process on a cancelled ctx.
func ptyCommand(ctx context.Context, opts StartOptions, baseEnv []string, hup *groupHangup) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "sh", "-c", opts.Command)
	cmd.Dir = opts.Dir
	if hup != nil {
		// Cancelling ctx hangs the hosted group up instead of killing only its
		// leader, so a launch wrapper hears it and can tear down what it owns.
		// Kill owns the grace and the escalation that follow, so exec must not
		// kill the process ahead of it.
		cmd.Cancel = func() error { return hup.signal(cmd.Process) }
	}

	env := baseEnv
	if len(env) == 0 {
		env = []string{
			"TERM=tmux-256color",
			"COLORTERM=truecolor",
			"FORCE_COLOR=1",
		}
	}
	// Scrub the combined environment last, then add this launch's identity. The
	// hosted process is an independent Claude Code session, and inheriting the
	// launching session's marker turns a completed turn into a silent hang. The
	// same scrub removes an inherited peer credential, which would make this
	// child join the broker as a run another live session already holds.
	combined := ScrubParentClaudeEnv(append(os.Environ(), env...))
	// PeerEnv appends this launch's provisioned peer credential, which is what
	// makes a spawned claude a provisioned peer. It still refuses a
	// StartOptions.Env entry named in the scrub lists, since those are appended
	// after every scrub layer above; the peer names themselves are the entries
	// it lets through.
	cmd.Env = PeerEnv(runtime.LaunchEnv(combined, opts.Provenance), opts.Env)
	return cmd
}

func (l PTYLauncher) Start(ctx context.Context, opts StartOptions) (Session, error) {
	if opts.Cols == 0 {
		opts.Cols = 220
	}
	if opts.Rows == 0 {
		opts.Rows = 50
	}

	// A hangup grace opts this session into graceful teardown. The signal is
	// shared by the ctx cancel watch and Kill, so the group is hung up once.
	var hup *groupHangup
	if opts.HangupGrace > 0 {
		hup = &groupHangup{}
	}

	cmd := ptyCommand(ctx, opts, l.Env, hup)

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Cols: uint16(opts.Cols),
		Rows: uint16(opts.Rows),
	})
	if err != nil {
		return nil, fmt.Errorf("pty start: %w", err)
	}
	if ptmx, err = pollableMaster(ptmx); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("pty start: %w", err)
	}

	ps := &PTYSession{
		name:        opts.Name,
		cmd:         cmd,
		ptmx:        ptmx,
		terminal:    vt10x.New(vt10x.WithSize(opts.Cols, opts.Rows)),
		alive:       true,
		waitDone:    make(chan struct{}),
		answer:      l.AnswerProgStatus,
		writes:      make(chan ptyWrite),
		replies:     make(chan []byte, ptyReplyQueue),
		closed:      make(chan struct{}),
		writerDone:  make(chan struct{}),
		hangup:      hup,
		hangupGrace: opts.HangupGrace,
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

	// hangup is set only for a session that asked for a hangup grace; it
	// carries the single hangup shared by the ctx cancel watch and Kill.
	hangup      *groupHangup
	hangupGrace time.Duration

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

// PasteAndEnter writes the prompt as a bracketed paste, then Enter. Text and
// Enter go out as separate writes: an Enter in the same burst as a long prompt
// is read as part of a paste and dropped, so the prompt never submits.
func (p *PTYSession) PasteAndEnter(ctx context.Context, text string) error {
	return p.submit(ctx, bracketedPaste(text), []byte{'\r'})
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
	_, _ = ptmx.Write(b)
}

// ProgStatusAnswering reports whether this session answers OSC 7501
// detection queries.
func (p *PTYSession) ProgStatusAnswering() bool { return p.answer }

// answerQuery queues a detection reply. It runs on the reader and must not
// block: a writer stuck on a program that stopped reading input would
// otherwise stop the reader draining that program's output. The query counts
// as answered once the reply is queued, on the reader, so the mark precedes
// every report the program sends in response.
func (p *PTYSession) answerQuery() {
	if !p.answer {
		return
	}
	select {
	case p.replies <- []byte(progStatusReply):
		p.psMarkAnswered()
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

// signalGroup signals the process group that the hosted process leads. The PTY
// launch makes that process a session leader, so its group is the whole
// launch-wrapper tree, and a group signal is what a closing terminal delivers
// to it. A group with no members left reports ESRCH, which is not an error
// here.
func signalGroup(pgid int, sig syscall.Signal) error {
	if err := syscall.Kill(-pgid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// groupHangup hangs a session's process group up once and remembers the
// result. A cancelled ctx and Kill both reach for the same session, and a
// launch wrapper's hangup handler is not necessarily idempotent: a wrapper that
// stops a VM or container would run its whole teardown twice.
//
// The signal never follows the leader's reap: the ctx cancel watch runs before
// exec reaps the command, and Kill probes the group first.
type groupHangup struct {
	once sync.Once
	err  error
}

func (h *groupHangup) signal(proc *os.Process) error {
	if proc == nil {
		return nil
	}
	h.once.Do(func() { h.err = signalGroup(proc.Pid, syscall.SIGHUP) })
	return h.err
}

// groupAlive reports whether the process group has a member left to signal. It
// separates an empty group (ESRCH) from a probe that failed for another reason.
func groupAlive(pgid int) (bool, error) {
	switch err := syscall.Kill(-pgid, 0); {
	case err == nil:
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	default:
		return false, err
	}
}

// killHosted kills only the hosted process: the teardown a session without a
// hangup grace gets.
func (p *PTYSession) killHosted() error {
	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

// Kill rejects further writes and ends the hosted process group: it hangs the
// group up once, then kills the group if it outlasts the grace or ctx. The pty
// master stays open until the reaper sees the exit: a dying process can block
// in exit until its pending output is read, so the reader must keep draining.
// A master write still blocked on the program's input fails when the child's
// side closes or, at the latest, when the reaper closes the master.
func (p *PTYSession) Kill(ctx context.Context) error {
	p.mu.Lock()
	p.alive = false
	p.mu.Unlock()

	p.markClosed()
	// cmd.Wait stays owned by one reaper. Wait callers still synchronously
	// observe the same completion.
	p.startWait()
	if p.hangup == nil || p.cmd == nil || p.cmd.Process == nil {
		return p.killHosted()
	}
	proc := p.cmd.Process
	// A process group id is its leader's pid, and that pid is not reused while
	// a member of the group is alive. An empty group leaves -pid free for a new
	// process, so Kill signals only when a member is there to receive it.
	if alive, err := groupAlive(proc.Pid); err != nil {
		return err
	} else if !alive {
		return nil
	}
	if err := p.hangup.signal(proc); err != nil {
		return err
	}
	grace := time.NewTimer(p.hangupGrace)
	defer grace.Stop()
	select {
	case <-p.waitDone:
	case <-grace.C:
	case <-ctx.Done():
	}
	// The leader can exit on the hangup while a member that ignored it lives
	// on: a VM or container helper, or a tool child of a program the wrapper
	// does not own. That leftover is still this group's to kill even though the
	// leader was reaped. The probe below is what keeps the kill off a pid that
	// is already free; the window between probe and signal is accepted rather
	// than closed with pidfd or waitid bookkeeping, because it only exists once
	// the group is empty.
	alive, err := groupAlive(proc.Pid)
	if err != nil {
		return err
	}
	if !alive {
		return nil
	}
	return signalGroup(proc.Pid, syscall.SIGKILL)
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
