package stable

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var httpExecCommand = exec.Command

// errHTTPServerStarting is used as a sentinel value in the httpServers map
// to indicate a server for this directory is currently being started.
var errHTTPServerStarting = fmt.Errorf("opencode serve for this directory is starting")

type managedHTTPServer struct {
	dir     string
	url     string
	cmd     *exec.Cmd
	exited  <-chan error
	healthy bool
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
				fmt.Fprintf(os.Stderr, "avenor stable: SIGKILL process group %d: %v\n", pgid, killErr)
			}
		} else {
			if killErr := s.cmd.Process.Kill(); killErr != nil {
				fmt.Fprintf(os.Stderr, "avenor stable: SIGKILL process %d: %v\n", s.cmd.Process.Pid, killErr)
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

func (s *Supervisor) getOrCreateHTTPServer(dir string) (*managedHTTPServer, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve abs dir: %w", err)
	}

	s.httpServerMu.Lock()

	for {
		entry := s.httpServers[absDir]

		if entry == nil {
			// No server for this dir — claim the slot with a pending sentinel
			// so concurrent callers for the same dir wait, then release the
			// mutex to do the expensive start operation.
			s.httpServers[absDir] = errHTTPServerStarting
			s.httpServerMu.Unlock()

			m, startErr := s.startHTTPServer(absDir)

			s.httpServerMu.Lock()
			if startErr != nil {
				delete(s.httpServers, absDir)
			} else {
				s.httpServers[absDir] = m
			}
			// Wake any waiters by broadcasting. They'll re-read the map.
			s.httpServerCond.Broadcast()
			s.httpServerMu.Unlock()
			return m, startErr
		}

		if entry == errHTTPServerStarting {
			// Another goroutine is starting a server for this dir — wait.
			s.httpServerCond.Wait()
			continue
		}

		// We have a managedHTTPServer — check if it's still alive.
		m, ok := entry.(*managedHTTPServer)
		if !ok {
			// Shouldn't happen — only sentinels and *managedHTTPServer go in the map.
			s.httpServerMu.Unlock()
			continue
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
			fmt.Fprintf(os.Stderr, "avenor stable: shutdown managed http server for %s: %v\n", absDir, err)
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

	cmd := httpExecCommand("opencode", "serve", "--port", fmt.Sprintf("%d", port), "--cwd", absDir)
	cmd.Stderr = os.Stderr
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
	defer s.httpServerMu.Unlock()

	for dir, entry := range s.httpServers {
		if entry == errHTTPServerStarting {
			delete(s.httpServers, dir)
			continue
		}
		if m, ok := entry.(*managedHTTPServer); ok {
			if err := m.shutdown(); err != nil {
				fmt.Fprintf(os.Stderr, "avenor stable: shutdown managed http server for %s: %v\n", dir, err)
			}
		}
		delete(s.httpServers, dir)
	}
}