package stable

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var httpExecCommand = exec.Command

type managedHTTPServer struct {
	dir     string
	url     string
	cmd     *exec.Cmd
	exited  <-chan error
	healthy bool
	stderr  io.Writer
	mu      sync.Mutex
}

func (s *managedHTTPServer) shutdown() error {
	s.mu.Lock()
	if s.cmd == nil || s.cmd.Process == nil {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	var firstErr error

	// SIGTERM first
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}

	select {
	case <-s.exited:
	case <-time.After(3 * time.Second):
		if pgid, err := syscall.Getpgid(s.cmd.Process.Pid); err == nil {
			if killErr := syscall.Kill(-pgid, syscall.SIGKILL); killErr != nil {
				fmt.Fprintf(s.stderr, "avenor stable: SIGKILL process group %d: %v\n", pgid, killErr)
			}
		} else {
			if killErr := s.cmd.Process.Kill(); killErr != nil {
				fmt.Fprintf(s.stderr, "avenor stable: SIGKILL process %d: %v\n", s.cmd.Process.Pid, killErr)
			}
		}
		<-s.exited
	}

	s.mu.Lock()
	s.cmd = nil
	s.exited = nil
	s.url = ""
	s.mu.Unlock()

	return firstErr
}

func (s *managedHTTPServer) healthCheck(ctx context.Context) error {
	return s.healthCheckWithURL(ctx, s.url)
}

func (s *managedHTTPServer) healthCheckWithURL(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/global/health", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check: %s", resp.Status)
	}
	return nil
}

// The httpServers/httpStarting pair makes the three per-directory states
// explicit: absent (no server), present in httpStarting (a start is in
// flight — waiters block on the condition variable), and present in
// httpServers (a ready server). No sentinel values in a map[string]any.
func (s *Supervisor) getOrCreateHTTPServer(dir string) (*managedHTTPServer, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve abs dir: %w", err)
	}

	s.httpServerMu.Lock()

	for {
		if s.httpShutdownStarted {
			// Shutdown has begun: starting a server here would only be torn
			// down by the completion guard after the full start duration,
			// stretching shutdown for this caller.
			s.httpServerMu.Unlock()
			return nil, fmt.Errorf("supervisor is shutting down")
		}

		if _, starting := s.httpStarting[absDir]; starting {
			// Another goroutine is starting a server for this dir — wait.
			s.httpServerCond.Wait()
			continue
		}

		m := s.httpServers[absDir]
		if m == nil {
			// No server for this dir — claim the slot so concurrent callers
			// for the same dir wait, then release the mutex to do the
			// expensive start operation.
			s.httpStarting[absDir] = struct{}{}
			s.httpServerMu.Unlock()

			m, startErr := s.startHTTPServer(absDir)

			s.httpServerMu.Lock()
			delete(s.httpStarting, absDir)
			if startErr != nil {
				// Nothing to insert; waiters wake and retry the start.
			} else if s.httpShutdownStarted {
				// Shutdown ran while the start was in flight. Inserting would
				// orphan the process; tear it down instead.
				s.httpServerCond.Broadcast()
				s.httpServerMu.Unlock()
				if err := m.shutdown(); err != nil {
					fmt.Fprintf(os.Stderr, "avenor stable: shutdown managed http server for %s: %v\n", absDir, err)
				}
				return nil, fmt.Errorf("supervisor is shutting down")
			} else {
				s.httpServers[absDir] = m
			}
			// Wake any waiters by broadcasting. They'll re-read the map.
			s.httpServerCond.Broadcast()
			s.httpServerMu.Unlock()
			return m, startErr
		}

		s.httpServerMu.Unlock()

		// Re-validate m.url: another goroutine could call shutdown()
		// between the unlock above and the healthCheck below, clearing m.url.
		m.mu.Lock()
		url := m.url
		m.mu.Unlock()
		if url == "" {
			// Server was shut down — loop to restart.
			s.httpServerMu.Lock()
			if s.httpServers[absDir] == m {
				delete(s.httpServers, absDir)
			}
			s.httpServerCond.Broadcast()
			s.httpServerMu.Unlock()
			continue
		}

		select {
		case <-m.exited:
			// Process has exited — clean up and loop to restart.
			s.httpServerMu.Lock()
			if s.httpServers[absDir] == m {
				delete(s.httpServers, absDir)
			}
			s.httpServerMu.Unlock()
			continue
		default:
		}

		// Process still alive — verify health.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		herr := m.healthCheckWithURL(ctx, url)
		cancel()
		if herr == nil {
			m.mu.Lock()
			m.healthy = true
			m.mu.Unlock()
			return m, nil
		}
		// Server is not healthy — shut it down and loop to restart.
		m.mu.Lock()
		m.healthy = false
		m.mu.Unlock()
		if err := m.shutdown(); err != nil {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: shutdown managed http server for %s: %v\n", absDir, err)
		}
		s.httpServerMu.Lock()
		if s.httpServers[absDir] == m {
			delete(s.httpServers, absDir)
		}
		s.httpServerCond.Broadcast()
		s.httpServerMu.Unlock()
		continue
	}
}

func (s *Supervisor) startHTTPServer(absDir string) (*managedHTTPServer, error) {
	// Bind a free port first.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("bind port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	cmd := httpExecCommand("opencode", "serve", "--port", fmt.Sprintf("%d", port))
	cmd.Dir = absDir
	cmd.Stderr = s.stderrWriter()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start opencode serve: %w", err)
	}

	exited := make(chan error, 1)
	go func() {
		exited <- cmd.Wait()
	}()

	m := &managedHTTPServer{
		dir:     absDir,
		cmd:     cmd,
		exited:  exited,
		healthy: false,
		stderr:  s.stderrWriter(),
		url:     fmt.Sprintf("http://127.0.0.1:%d", port),
	}

	// Poll for health with backoff.
	backoff := 50 * time.Millisecond
	deadline := time.After(30 * time.Second)

	for {
		select {
		case err := <-exited:
			return nil, fmt.Errorf("opencode serve exited during startup: %w", err)
		default:
		}

		select {
		case <-deadline:
			cmd.Process.Kill()
			<-exited
			return nil, fmt.Errorf("opencode serve startup timed out after 30s")
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		herr := m.healthCheck(ctx)
		cancel()
		if herr == nil {
			m.mu.Lock()
			m.healthy = true
			m.mu.Unlock()
			return m, nil
		}

		time.Sleep(backoff)
		backoff *= 2
		if backoff > 500*time.Millisecond {
			backoff = 500 * time.Millisecond
		}
	}
}

func (s *Supervisor) shutdownManagedHTTPServers() {
	s.httpServerMu.Lock()
	s.httpShutdownStarted = true
	s.httpStarting = map[string]struct{}{}
	ready := s.httpServers
	s.httpServers = map[string]*managedHTTPServer{}
	s.httpServerMu.Unlock()

	// A start in flight when shutdown begins completes into the
	// httpShutdownStarted guard, which tears its server down there.
	for _, m := range ready {
		if err := m.shutdown(); err != nil {
			fmt.Fprintf(s.stderrWriter(), "avenor stable: shutdown managed http server for %s: %v\n", m.dir, err)
		}
	}
}
