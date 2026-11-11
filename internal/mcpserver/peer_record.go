package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/childmessaging"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// codexPeerFileArg is the hook-owned input property that binds one message or
// peers call to the Codex session which made it. Codex's plugin MCP server is
// shared by every conversation in the host process, so the tool call itself
// carries the session record; the hook sets this property after the model's
// arguments and the model never needs to know it exists. It names a file, never
// a credential: the record under it is mode 0600 and the broker token stays
// inside this process. claudePeerFileArg does the same for a Claude Code
// session, whose plugin hook writes the record.
const (
	codexPeerFileArg  = "codex_peer_file"
	claudePeerFileArg = "claude_peer_file"
)

// peerBinding is the session record one call names: the host that wrote it
// and the file. The zero value is an unbound call, served as the supervisor.
type peerBinding struct {
	host string
	file string
}

// peerHosts lists each host that binds a call, keyed by the directory
// its records live under and the property that names them.
var peerHosts = []struct {
	host  string
	arg   string
	title string
	// message reports whether the message tool honors this host's binding.
	// The Claude plugin serves its own tell, ask and reply and binds only the
	// peers tool.
	message bool
}{
	{"codex", codexPeerFileArg, "Codex", true},
	{"claude", claudePeerFileArg, "Claude", false},
}

// peerRecordVersion is the only record shape this reader understands.
const peerRecordVersion = 1

// peerRecord is the identity a host's session-start hook writes for one
// session, in the shape both the Codex and Claude writers emit: the broker run
// the session acts as, its own credential, and the broker that minted it.
type peerRecord struct {
	Version   int    `json:"version"`
	SessionID string `json:"session_id"`
	BrokerURL string `json:"broker_url"`
	RunID     string `json:"run_id"`
	RunToken  string `json:"run_token"`
	Label     string `json:"label"`
}

// hostPeer is one validated record plus the directory its poller owns.
type hostPeer struct {
	record peerRecord
	dir    string
}

// readCodexPeerRecord validates the shape before trusting a byte of it: a
// regular owner-only file at <root>/peers/<host>/<session>/identity.json whose
// record names the same session. Containment is structural rather than rooted
// at a resolved AVENOR_HOME because the host spawns its MCP server with a
// narrow environment that need not carry the variable the hook saw.
func readCodexPeerRecord(file string) (*hostPeer, error) {
	return readPeerRecord(file, "codex")
}

// readPeerRecord reads one host's record under the same checks for every host.
func readPeerRecord(file, host string) (*hostPeer, error) {
	clean := filepath.Clean(file)
	if filepath.Base(clean) != "identity.json" {
		return nil, fmt.Errorf("not a peer record")
	}
	dir := filepath.Dir(clean)
	session := filepath.Base(dir)
	if filepath.Base(filepath.Dir(dir)) != host || filepath.Base(filepath.Dir(filepath.Dir(dir))) != "peers" {
		return nil, fmt.Errorf("not under the %s peer records", host)
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("peer record is not a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("peer record is readable by other users")
	}
	raw, err := os.ReadFile(clean)
	if err != nil {
		return nil, err
	}
	var record peerRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("peer record is not valid JSON")
	}
	if record.Version != peerRecordVersion {
		return nil, fmt.Errorf("unsupported peer record version %d", record.Version)
	}
	if record.SessionID != session {
		return nil, fmt.Errorf("peer record names session %q, not %q", record.SessionID, session)
	}
	if record.RunID == "" || record.RunToken == "" {
		return nil, fmt.Errorf("peer record carries no run credential")
	}
	parsed, err := url.Parse(record.BrokerURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("peer record carries no broker URL")
	}
	// The record carries the run token, so a plaintext URL may reach only this
	// host. A broker on another host must be named over https.
	if parsed.Scheme == "http" && !loopbackHost(parsed.Hostname()) {
		return nil, fmt.Errorf("peer record carries a non-loopback plaintext broker URL")
	}
	return &hostPeer{record: record, dir: dir}, nil
}

func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// provider builds the shared broker-authenticated provider over this peer's own
// credentials, so tell, ask, reply, withdraw, peers and last all speak as the
// bound session rather than as the supervisor. A reply claims the ask record the
// poller wrote under the peer's asks directory.
func (p *hostPeer) provider(ctx context.Context) (*askclaim.Provider, error) {
	shared, err := childmessaging.NewMessageProvider(messaging.SurfaceMCP, childmessaging.Options{
		RunID:     p.record.RunID,
		Token:     p.record.RunToken,
		BrokerURL: p.record.BrokerURL,
	})
	if err != nil {
		return nil, err
	}
	if err := shared.Prepare(ctx); err != nil {
		_ = shared.Close()
		return nil, err
	}
	return &askclaim.Provider{MessageProvider: shared, Dir: filepath.Join(p.dir, "asks")}, nil
}

// splitPeerFile removes the hook's binding from the router's input. The
// router validates a closed property set, so the binding never reaches it; a
// malformed envelope is left alone for the router to report in its own words.
// A call naming two bindings is refused: it would have no single identity.
func splitPeerFile(raw json.RawMessage) (json.RawMessage, peerBinding, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return raw, peerBinding{}, nil
	}
	var binding peerBinding
	for _, h := range peerHosts {
		value, ok := fields[h.arg]
		if !ok {
			continue
		}
		delete(fields, h.arg)
		var file string
		if err := json.Unmarshal(value, &file); err != nil {
			return nil, peerBinding{}, fmt.Errorf("%s must be a string", h.arg)
		}
		if binding.host != "" {
			return nil, peerBinding{}, fmt.Errorf("a call may carry only one peer binding")
		}
		binding = peerBinding{host: h.host, file: file}
	}
	if binding.host == "" {
		return raw, peerBinding{}, nil
	}
	cleaned, err := json.Marshal(fields)
	if err != nil {
		return nil, peerBinding{}, err
	}
	return cleaned, binding, nil
}

// messageSurface is the provider one merged-tool call routes through, plus the
// supervisor binding the owner surface records for a later withdraw.
type messageSurface struct {
	provider       router.Provider
	supervisorPath string
	cleanup        func()
}

// messageSurfaceFor chooses the caller's identity. A bound Codex or Claude
// call speaks as its own run; resume keeps the owner path because a
// broker-authenticated run may only continue a run it spawned.
func (s *Server) messageSurfaceFor(ctx context.Context, raw json.RawMessage, binding peerBinding, peerAllowed bool) (messageSurface, error) {
	if peerAllowed && binding.file != "" {
		refused := func(err error) error {
			return &router.ToolError{
				Code:    "refused",
				Message: "the avenor " + peerHostTitle(binding.host) + " peer record cannot be used: " + err.Error(),
				Facts:   map[string]string{},
			}
		}
		peer, err := readPeerRecord(binding.file, binding.host)
		if err != nil {
			return messageSurface{}, refused(err)
		}
		provider, err := peer.provider(ctx)
		if err != nil {
			return messageSurface{}, refused(err)
		}
		return messageSurface{provider: provider, cleanup: func() { _ = provider.Close() }}, nil
	}
	provider, cleanup, resolvedPath, err := s.getClientForSupervisorWithPath(s.messageSupervisorPath(raw))
	if err != nil {
		return messageSurface{}, err
	}
	bound, err := newMCPMessageProvider(ctx, s, provider, resolvedPath)
	if err != nil {
		cleanup()
		return messageSurface{}, err
	}
	return messageSurface{provider: bound, supervisorPath: resolvedPath, cleanup: cleanup}, nil
}

func peerHostTitle(host string) string {
	for _, h := range peerHosts {
		if h.host == host {
			return h.title
		}
	}
	return host
}

// messageAction reads the action the router will route. An absent action is the
// peers default and never reaches here for the message tool.
func messageAction(raw json.RawMessage) string {
	var fields struct {
		Action string `json:"action"`
	}
	_ = json.Unmarshal(raw, &fields)
	return strings.TrimSpace(fields.Action)
}
