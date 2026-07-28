package agy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/runtime"
	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"github.com/sdougbrown/avenor/internal/runtime/claudecore/terminal"
)

type recordingLauncher struct {
	session terminal.Session
	err     error

	mu    sync.Mutex
	calls int
	opts  terminal.StartOptions
}

func (l *recordingLauncher) Start(_ context.Context, opts terminal.StartOptions) (terminal.Session, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.opts = opts
	return l.session, l.err
}

type noTerminalProtocolSession struct {
	*terminal.FakeSession
	mu       sync.Mutex
	captures int
	pastes   int
	keys     int
}

func (s *noTerminalProtocolSession) Capture(context.Context) (string, error) {
	s.mu.Lock()
	s.captures++
	s.mu.Unlock()
	panic("RPC host must not capture terminal contents")
}

func (s *noTerminalProtocolSession) PasteAndEnter(context.Context, string) error {
	s.mu.Lock()
	s.pastes++
	s.mu.Unlock()
	panic("RPC host must not paste terminal input")
}

func (s *noTerminalProtocolSession) SendKeys(context.Context, ...terminal.Key) error {
	s.mu.Lock()
	s.keys++
	s.mu.Unlock()
	panic("RPC host must not send terminal keys")
}

func (s *noTerminalProtocolSession) protocolCalls() (captures, pastes, keys int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.captures, s.pastes, s.keys
}

type blockingReapSession struct {
	*noTerminalProtocolSession
	waitStarted chan struct{}
	killStarted chan struct{}
	release     chan struct{}
	waitOnce    sync.Once
	killOnce    sync.Once
}

func (s *blockingReapSession) Wait(ctx context.Context) error {
	s.waitOnce.Do(func() { close(s.waitStarted) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingReapSession) Kill(ctx context.Context) error {
	s.killOnce.Do(func() { close(s.killStarted) })
	return s.FakeSession.Kill(ctx)
}

func withPTYRPCSeams(t *testing.T, discover func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error), start func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error), validate func(context.Context, *rpcHost, string) error) {
	t.Helper()
	withPTYRPCSeamsAndWait(t, discover, start, validate, func(context.Context, *rpcHost) (*agyv115.FetchAvailableModelsResponse, error) {
		return &agyv115.FetchAvailableModelsResponse{
			Models: map[string]*agyv115.ModelDetails{
				"gemini-2.5-flash": {Model: agyv115.Model_MODEL_GOOGLE_GEMINI_2_5_FLASH},
			},
		}, nil
	})
}

func withPTYRPCSeamsAndWait(t *testing.T, discover func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error), start func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error), validate func(context.Context, *rpcHost, string) error, wait func(context.Context, *rpcHost) (*agyv115.FetchAvailableModelsResponse, error)) {
	t.Helper()
	oldDiscover, oldStart, oldValidate, oldWait := discoverPTYRPCHost, startPTYCascade, validatePTYSession, awaitModelAvailability
	discoverPTYRPCHost, startPTYCascade, validatePTYSession, awaitModelAvailability = discover, start, validate, wait
	t.Cleanup(func() {
		discoverPTYRPCHost, startPTYCascade, validatePTYSession, awaitModelAvailability = oldDiscover, oldStart, oldValidate, oldWait
	})
}

func TestSupportedRPCVersions(t *testing.T) {
	for version, want := range map[string]bool{
		"1.1.5":      true,
		"1.1.6":      true,
		"1.1.7":      true,
		"1.1.8":      true,
		"1.2.0":      true,
		"1.99.999":   true,
		"":           false,
		"1":          false,
		"1.1":        false,
		"01.1.8":     false,
		"1.01.8":     false,
		"1.1.08":     false,
		"1.1.x":      false,
		"1.1.8-beta": false,
		"2.0.0":      false,
	} {
		if got := supportedRPCVersion(version); got != want {
			t.Errorf("supportedRPCVersion(%q) = %v, want %v", version, got, want)
		}
	}
}

func TestPTYRPCHostStartupAcceptsCompatibleMajorAndRejectsOtherMajor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version string
		want    bool
	}{
		{"accepts 1.1.8", "1.1.8", true},
		{"accepts 1.2.0", "1.2.0", true},
		{"rejects 2.0.0", "2.0.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 51, "")}
			launcher := &recordingLauncher{session: session}
			withPTYRPCSeams(t,
				func(_ context.Context, pid int, version string, _ rpcDiscoveryOptions) (*rpcHost, error) {
					if !tc.want {
						t.Fatalf("discover should not run for rejected version %q", version)
					}
					if version != tc.version {
						t.Fatalf("version = %q, want %q", version, tc.version)
					}
					return &rpcHost{sessionID: "id"}, nil
				},
				func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
					return &agyv115.StartCascadeResponse{CascadeId: "id"}, nil
				},
				func(_ context.Context, h *rpcHost, id string) error {
					h.sessionID = id
					return nil
				},
			)
			_, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{}, "", tc.version, rpcDiscoveryOptions{})
			if tc.want {
				if err != nil {
					t.Fatalf("start host: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("start host should have rejected unsupported version")
				}
				if !strings.Contains(err.Error(), "compatible major version") {
					t.Fatalf("rejection error = %v, want compatible-major diagnostic", err)
				}
			}
		})
	}
}

func TestInteractiveAgyCommandQuotesOnlySupportedValues(t *testing.T) {
	for _, tc := range []struct {
		name   string
		resume string
		agent  string
		dir    string
		want   string
	}{
		{"new", "", "", "", "exec agy"},
		{"new workspace", "", "", "/private/workspace", "exec agy --add-dir \"$PWD\""},
		{"new agent", "", "agent one", "", "exec agy --agent 'agent one'"},
		{"resume", "conversation-123", "", "", "exec agy --conversation 'conversation-123'"},
		{"resume agent hostile", "id' ; $(touch nope)\nnext", "a'\n$(bad); *", "", "exec agy --conversation 'id'\"'\"' ; $(touch nope)\nnext' --agent 'a'\"'\"'\n$(bad); *'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := interactiveAgyCommand(tc.resume, tc.agent, tc.dir); got != tc.want {
				t.Fatalf("command = %q, want %q", got, tc.want)
			}
		})
	}
	for _, value := range []string{"", "plain", "quote'and spaces", "$(not-a-command); *\nnext"} {
		output, err := exec.Command("sh", "-c", "set -- "+posixShellQuote(value)+"; printf '%s' \"$1\"").Output()
		if err != nil || string(output) != value {
			t.Fatalf("shell quote value=%q output=%q err=%v", value, output, err)
		}
	}
}

func TestPTYRPCHostNewBindsCascadeWithoutTerminalProtocol(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 4242, "")}
	launcher := &recordingLauncher{session: session}
	var discoveredPID int
	var startCalls, validateCalls int
	withPTYRPCSeams(t,
		func(_ context.Context, pid int, version string, options rpcDiscoveryOptions) (*rpcHost, error) {
			discoveredPID = pid
			if version != "1.1.5" || options.AllowPlainHTTP {
				t.Fatalf("discovery inputs = version %q options %#v", version, options)
			}
			return &rpcHost{}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			startCalls++
			return &agyv115.StartCascadeResponse{CascadeId: "external-conversation"}, nil
		},
		func(_ context.Context, _ *rpcHost, id string) error {
			validateCalls++
			if id != "external-conversation" {
				t.Fatalf("validated id = %q", id)
			}
			return nil
		},
	)
	opts := runtime.StartOptions{Dir: "/safe/workspace", Agent: "agent ' one", Model: "model-must-not-appear"}
	host, err := startPTYRPCHost(context.Background(), launcher, opts, "", "1.1.5", rpcDiscoveryOptions{})
	if err != nil {
		t.Fatalf("start host: %v", err)
	}
	if discoveredPID != 4242 || startCalls != 1 || validateCalls != 1 || host.ConversationID() != "external-conversation" || host.RPC() == nil {
		t.Fatalf("host binding pid=%d starts=%d validates=%d id=%q rpc=%v", discoveredPID, startCalls, validateCalls, host.ConversationID(), host.RPC())
	}
	if launcher.calls != 1 || launcher.opts.Dir != opts.Dir || launcher.opts.Command != "exec agy --agent 'agent '\"'\"' one' --add-dir \"$PWD\"" {
		t.Fatalf("launch = calls=%d opts=%#v", launcher.calls, launcher.opts)
	}
	for _, forbidden := range []string{opts.Model, opts.Dir, "prompt content"} {
		if strings.Contains(launcher.opts.Command, forbidden) {
			t.Fatalf("command leaked %q: %q", forbidden, launcher.opts.Command)
		}
	}
	if err := host.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	captures, pastes, keys := session.protocolCalls()
	if captures != 0 || pastes != 0 || keys != 0 || session.KillCalls() != 1 || session.WaitCalls() != 1 {
		t.Fatalf("terminal calls capture=%d paste=%d keys=%d kill=%d wait=%d", captures, pastes, keys, session.KillCalls(), session.WaitCalls())
	}
}

func TestPTYRPCHostRetriesListenerDiscoveryDuringInteractiveStartup(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 11, "")}
	launcher := &recordingLauncher{session: session}
	attempts := 0
	withPTYRPCSeams(t,
		func(_ context.Context, pid int, _ string, _ rpcDiscoveryOptions) (*rpcHost, error) {
			if pid != 11 {
				t.Fatalf("pid = %d", pid)
			}
			attempts++
			if attempts == 1 {
				return nil, errors.New("listener not ready")
			}
			return &rpcHost{}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return &agyv115.StartCascadeResponse{CascadeId: "id"}, nil
		},
		func(context.Context, *rpcHost, string) error { return nil },
	)
	host, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{})
	if err != nil {
		t.Fatalf("start host: %v", err)
	}
	defer host.Close(context.Background())
	if attempts != 2 {
		t.Fatalf("discovery attempts = %d, want 2", attempts)
	}
}

func TestPTYRPCHostDiscoveryRetryIsBoundedByCaller(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 13, "")}
	launcher := &recordingLauncher{session: session}
	attempts := 0
	withPTYRPCSeams(t,
		func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) {
			attempts++
			return nil, errors.New("not ready")
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) { return nil, nil },
		func(context.Context, *rpcHost, string) error { return nil },
	)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := startPTYRPCHost(ctx, launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded discovery = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded discovery took %v", elapsed)
	}
	if attempts < 2 || attempts > 10 {
		t.Fatalf("bounded discovery attempts = %d", attempts)
	}
	if session.KillCalls() != 1 || session.WaitCalls() != 1 {
		t.Fatalf("cleanup kill=%d wait=%d", session.KillCalls(), session.WaitCalls())
	}
}

func TestPTYRPCHostResumeValidatesWithoutStartingCascade(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 7, "")}
	launcher := &recordingLauncher{session: session}
	startCalls := 0
	withPTYRPCSeams(t,
		func(_ context.Context, pid int, _ string, _ rpcDiscoveryOptions) (*rpcHost, error) {
			if pid != 7 {
				t.Fatalf("pid = %d", pid)
			}
			return &rpcHost{}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			startCalls++
			return nil, errors.New("must not start")
		},
		func(_ context.Context, _ *rpcHost, id string) error {
			if id != "resume-id" {
				t.Fatalf("validated id = %q", id)
			}
			return nil
		},
	)
	host, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{Agent: "a"}, "resume-id", "1.1.5", rpcDiscoveryOptions{})
	if err != nil {
		t.Fatalf("start resume host: %v", err)
	}
	defer host.Close(context.Background())
	if startCalls != 0 || host.ConversationID() != "resume-id" || launcher.opts.Command != "exec agy --conversation 'resume-id' --agent 'a'" {
		t.Fatalf("starts=%d id=%q command=%q", startCalls, host.ConversationID(), launcher.opts.Command)
	}
}

func TestPTYRPCHostStartupFailuresReap(t *testing.T) {
	cases := []struct {
		name     string
		pid      int
		setup    func(*noTerminalProtocolSession)
		discover func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error)
		start    func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error)
		validate func(context.Context, *rpcHost, string) error
	}{
		{"pid", 0, nil, nil, nil, nil},
		{"discovery", 1, nil, func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) {
			return nil, errors.New("sensitive endpoint")
		}, nil, nil},
		{"start", 1, nil, func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) { return &rpcHost{}, nil }, func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return nil, errors.New("opaque failure")
		}, nil},
		{"validation", 1, nil, func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) { return &rpcHost{}, nil }, func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return &agyv115.StartCascadeResponse{CascadeId: "opaque"}, nil
		}, func(context.Context, *rpcHost, string) error { return errors.New("opaque validation") }},
		{"process death", 1, func(s *noTerminalProtocolSession) { s.SetAlive(false) }, func(ctx context.Context, _ int, _ string, _ rpcDiscoveryOptions) (*rpcHost, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", tc.pid, "")}
			if tc.setup != nil {
				tc.setup(session)
			}
			launcher := &recordingLauncher{session: session}
			discover := tc.discover
			if discover == nil {
				discover = func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) { return &rpcHost{}, nil }
			}
			start := tc.start
			if start == nil {
				start = func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
					return &agyv115.StartCascadeResponse{CascadeId: "id"}, nil
				}
			}
			validate := tc.validate
			if validate == nil {
				validate = func(context.Context, *rpcHost, string) error { return nil }
			}
			withPTYRPCSeams(t, discover, start, validate)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := startPTYRPCHost(ctx, launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{})
			if err == nil || strings.Contains(err.Error(), "opaque") || strings.Contains(err.Error(), "endpoint") {
				t.Fatalf("error = %v", err)
			}
			if session.KillCalls() != 1 || session.WaitCalls() != 1 {
				t.Fatalf("cleanup kill=%d wait=%d", session.KillCalls(), session.WaitCalls())
			}
		})
	}
}

func TestPTYRPCHostStartupFailureClosesDiscoveredRPC(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 12, "")}
	launcher := &recordingLauncher{session: session}
	closed := 0
	rpc := &rpcHost{closeHook: func() { closed++ }}
	withPTYRPCSeams(t,
		func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) { return rpc, nil },
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return nil, errors.New("start failed")
		},
		func(context.Context, *rpcHost, string) error { return nil },
	)
	if _, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{}); err == nil {
		t.Fatal("startup unexpectedly succeeded")
	}
	if closed != 1 || session.KillCalls() != 1 || session.WaitCalls() != 1 {
		t.Fatalf("cleanup rpc=%d kill=%d wait=%d", closed, session.KillCalls(), session.WaitCalls())
	}
}

func TestPTYRPCHostCancellationAndConcurrentClose(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 8, "")}
	launcher := &recordingLauncher{session: session}
	withPTYRPCSeams(t,
		func(ctx context.Context, _ int, _ string, _ rpcDiscoveryOptions) (*rpcHost, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) { return nil, nil },
		func(context.Context, *rpcHost, string) error { return nil },
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := startPTYRPCHost(ctx, launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled startup = %v", err)
	}
	if session.KillCalls() != 1 || session.WaitCalls() != 1 {
		t.Fatalf("cancel cleanup kill=%d wait=%d", session.KillCalls(), session.WaitCalls())
	}

	// A separately created, validated host proves concurrent Close uses one
	// terminal teardown and one reaper wait.
	session = &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 9, "")}
	launcher = &recordingLauncher{session: session}
	rpcCloseCalls := 0
	withPTYRPCSeams(t,
		func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) {
			return &rpcHost{closeHook: func() { rpcCloseCalls++ }}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return &agyv115.StartCascadeResponse{CascadeId: "id"}, nil
		},
		func(context.Context, *rpcHost, string) error { return nil },
	)
	host, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = host.Close(context.Background()) }()
	}
	wg.Wait()
	if session.KillCalls() != 1 || session.WaitCalls() != 1 || rpcCloseCalls != 1 {
		t.Fatalf("concurrent cleanup kill=%d wait=%d rpc=%d", session.KillCalls(), session.WaitCalls(), rpcCloseCalls)
	}
}

func TestPTYRPCHostModelAvailabilityWait(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 42, "")}
	launcher := &recordingLauncher{session: session}
	var discoverCalls, waitCalls int
	withPTYRPCSeamsAndWait(t,
		func(_ context.Context, pid int, _ string, _ rpcDiscoveryOptions) (*rpcHost, error) {
			discoverCalls++
			return &rpcHost{client: &rpcClient{}, sessionID: "model-wait-id"}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return &agyv115.StartCascadeResponse{CascadeId: "model-wait-id"}, nil
		},
		func(context.Context, *rpcHost, string) error { return nil },
		func(_ context.Context, host *rpcHost) (*agyv115.FetchAvailableModelsResponse, error) {
			waitCalls++
			if host == nil || host.client == nil {
				return nil, errors.New("no client in availability wait")
			}
			return &agyv115.FetchAvailableModelsResponse{
				Models: map[string]*agyv115.ModelDetails{
					"gemini-2.5-flash": {Model: agyv115.Model_MODEL_GOOGLE_GEMINI_2_5_FLASH},
				},
			}, nil
		},
	)
	host, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{})
	if err != nil {
		t.Fatalf("start host: %v", err)
	}
	defer host.Close(context.Background())
	if waitCalls != 1 || discoverCalls != 1 || host.ConversationID() != "model-wait-id" {
		t.Fatalf("wait=%d discover=%d id=%q", waitCalls, discoverCalls, host.ConversationID())
	}
	if host.models == nil || len(host.models.GetModels()) == 0 {
		t.Fatal("available models were not retained")
	}
}

func TestPTYRPCHostModelAvailabilityWaitFailureReaps(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 43, "")}
	launcher := &recordingLauncher{session: session}
	withPTYRPCSeamsAndWait(t,
		func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) {
			return &rpcHost{sessionID: "fail-id"}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return &agyv115.StartCascadeResponse{CascadeId: "fail-id"}, nil
		},
		func(context.Context, *rpcHost, string) error { return nil },
		func(context.Context, *rpcHost) (*agyv115.FetchAvailableModelsResponse, error) {
			return nil, errors.New("model wait failed")
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := startPTYRPCHost(ctx, launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{}); err == nil {
		t.Fatal("start host should have failed")
	}
	if session.KillCalls() != 1 || session.WaitCalls() != 1 {
		t.Fatalf("cleanup kill=%d wait=%d", session.KillCalls(), session.WaitCalls())
	}
}

func TestPTYRPCHostConcurrentCloseHonorsWaiterDeadline(t *testing.T) {
	base := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 14, "")}
	session := &blockingReapSession{
		noTerminalProtocolSession: base,
		waitStarted:               make(chan struct{}),
		killStarted:               make(chan struct{}),
		release:                   make(chan struct{}),
	}
	launcher := &recordingLauncher{session: session}
	withPTYRPCSeams(t,
		func(_ context.Context, _ int, _ string, _ rpcDiscoveryOptions) (*rpcHost, error) {
			return &rpcHost{sessionID: "id"}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return &agyv115.StartCascadeResponse{CascadeId: "id"}, nil
		},
		func(_ context.Context, h *rpcHost, id string) error {
			h.sessionID = id // Simulate real validateSession behavior.
			return nil
		},
	)
	host, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-session.waitStarted
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- host.Close(context.Background()) }()
	<-session.killStarted
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := host.Close(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent close waiter = %v", err)
	}
	close(session.release)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner close = %v", err)
	}
	if err := host.Close(context.Background()); err != nil {
		t.Fatalf("completed close = %v", err)
	}
	if session.KillCalls() != 1 {
		t.Fatalf("kill calls = %d", session.KillCalls())
	}
}

func TestPTYRPCHostFailedCloseCanRetryReap(t *testing.T) {
	base := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 15, "")}
	session := &blockingReapSession{noTerminalProtocolSession: base, waitStarted: make(chan struct{}), killStarted: make(chan struct{}), release: make(chan struct{})}
	launcher := &recordingLauncher{session: session}
	withPTYRPCSeams(t,
		func(_ context.Context, _ int, _ string, _ rpcDiscoveryOptions) (*rpcHost, error) {
			return &rpcHost{sessionID: "id"}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return &agyv115.StartCascadeResponse{CascadeId: "id"}, nil
		},
		func(_ context.Context, h *rpcHost, id string) error { h.sessionID = id; return nil },
	)
	host, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-session.waitStarted
	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := host.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close = %v", err)
	}
	cancel()
	close(session.release)
	if err := host.Close(context.Background()); err != nil {
		t.Fatalf("retry Close = %v", err)
	}
	if session.KillCalls() < 2 {
		t.Fatalf("retry did not reattempt cleanup: kill calls = %d", session.KillCalls())
	}
}

// TestPTYRPCHostCloseCancelsInFlightPreAnswerFetch verifies that a Close
// during a blocked pre-answer GetSteps call yields zero Handle mutations and
// no goroutine leak. The fake RPC client exposes hooks to count calls and
// signal that the in-flight snapshot is blocked.
func TestPTYRPCHostCloseCancelsInFlightPreAnswerFetch(t *testing.T) {
	session := &noTerminalProtocolSession{FakeSession: terminal.NewFakeSession("agy", 17, "")}
	launcher := &recordingLauncher{session: session}
	var handleCalls atomic.Int32
	getRelease := make(chan struct{})
	getCancelled := make(chan struct{})
	// Wrap a real httptest server so the handle call path is exercised end to
	// end: any Handle reaching the wire would be visible as an incremented
	// counter on the dedicated handler.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", grpcWebContentType)
		if strings.HasSuffix(r.URL.Path, "/HandleCascadeUserInteraction") {
			handleCalls.Add(1)
		}
		_, _ = w.Write(grpcReply(t, &agyv115.HeartbeatResponse{}))
	}))
	defer server.Close()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	withPTYRPCSeams(t,
		func(context.Context, int, string, rpcDiscoveryOptions) (*rpcHost, error) {
			return &rpcHost{client: client, sessionID: "id"}, nil
		},
		func(context.Context, *rpcHost) (*agyv115.StartCascadeResponse, error) {
			return &agyv115.StartCascadeResponse{CascadeId: "id"}, nil
		},
		func(_ context.Context, h *rpcHost, id string) error { h.sessionID = id; return nil },
	)
	host, err := startPTYRPCHost(context.Background(), launcher, runtime.StartOptions{}, "", "1.1.5", rpcDiscoveryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Swap in a custom bridge that blocks the pre-answer snapshot until Close
	// cancels it. We retain the real getSteps/handle so an actual Handle call
	// would hit the wire and be counted.
	cancelled := make(chan struct{})
	host.interactions = newInteractionBridge("session", "id",
		func(ctx context.Context, offset uint32) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
			select {
			case <-cancelled:
			default:
				close(cancelled)
			}
			select {
			case <-getRelease:
			case <-ctx.Done():
				select {
				case <-getCancelled:
				default:
					close(getCancelled)
				}
				return nil, ctx.Err()
			}
			step := &agyv115.Step{Type: agyv115.CortexStepType_CORTEX_STEP_TYPE_PLANNER_RESPONSE, Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, Metadata: &agyv115.CortexStepMetadata{StepGenerationVersion: pendingGenerationVersion}, RequestedInteraction: &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_Permission{Permission: &agyv115.PermissionInteractionSpec{Resource: &agyv115.PermissionResource{Action: "act", Target: "target"}}}}}
			return &agyv115.GetCascadeTrajectoryStepsResponse{Steps: []*agyv115.Step{step}}, nil
		},
		func(context.Context, *agyv115.CascadeUserInteraction) error { return nil },
	)
	host.mapper.SetInteractionObserver(host.interactions.Observe, host.interactions.Clear)
	step := &agyv115.Step{Type: agyv115.CortexStepType_CORTEX_STEP_TYPE_PLANNER_RESPONSE, Status: agyv115.CortexStepStatus_CORTEX_STEP_STATUS_WAITING, Metadata: &agyv115.CortexStepMetadata{StepGenerationVersion: pendingGenerationVersion}, RequestedInteraction: &agyv115.RequestedInteraction{Interaction: &agyv115.RequestedInteraction_Permission{Permission: &agyv115.PermissionInteractionSpec{Resource: &agyv115.PermissionResource{Action: "act", Target: "target"}}}}}
	host.mapper.BindStreamIdentity(&agyv115.AgentStateUpdate{ConversationId: "id", TrajectoryId: "trajectory"})
	host.mapper.BeginTurn()
	streamEvents := host.mapper.ApplyStream(&agyv115.AgentStateUpdate{ConversationId: "id", TrajectoryId: "trajectory", Status: agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING, MainTrajectoryUpdate: &agyv115.TrajectoryUpdate{StepsUpdate: &agyv115.StepsUpdate{Indices: []uint32{3}, Steps: []*agyv115.Step{step}}}})
	if len(streamEvents.events) != 1 || streamEvents.events[0].Event != "permission.request" {
		t.Fatalf("stream events = %#v", streamEvents.events)
	}
	requestID := streamEvents.events[0].Fields["request_id"].(string)
	answerDone := make(chan error, 1)
	go func() {
		answerDone <- host.AnswerPermission(context.Background(), requestID, runtime.PermissionResponse{Allow: true, OptionID: "allow_once"})
	}()
	// Wait for the bridge to enter the snapshot RPC.
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("pre-answer snapshot did not start")
	}
	if err := host.Close(context.Background()); err != nil {
		t.Fatalf("host close: %v", err)
	}
	select {
	case err := <-answerDone:
		if err == nil {
			t.Fatal("AnswerPermission succeeded after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("answer goroutine leaked after Close")
	}
	if got := handleCalls.Load(); got != 0 {
		t.Fatalf("Handle sent %d mutations after Close", got)
	}
	close(getRelease)
}
