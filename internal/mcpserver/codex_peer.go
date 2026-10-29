package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

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
// inside this process.
const codexPeerFileArg = "codex_peer_file"

// codexPeerRecordVersion is the only record shape this reader understands.
const codexPeerRecordVersion = 1

var codexAskID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// codexPeerRecord is the identity the session-start hook writes once per Codex
// thread: the broker run the thread acts as, its own credential, and the broker
// that minted it.
type codexPeerRecord struct {
	Version   int    `json:"version"`
	SessionID string `json:"session_id"`
	BrokerURL string `json:"broker_url"`
	RunID     string `json:"run_id"`
	RunToken  string `json:"run_token"`
	Label     string `json:"label"`
}

// codexPeer is one validated record plus the directory its poller owns.
type codexPeer struct {
	record codexPeerRecord
	dir    string
}

// readCodexPeerRecord validates the shape before trusting a byte of it: a
// regular owner-only file at <root>/peers/codex/<session>/identity.json whose
// record names the same session. Containment is structural rather than rooted
// at a resolved AVENOR_HOME because the host spawns its MCP server with a
// narrow environment that need not carry the variable the hook saw.
func readCodexPeerRecord(file string) (*codexPeer, error) {
	clean := filepath.Clean(file)
	if filepath.Base(clean) != "identity.json" {
		return nil, fmt.Errorf("not a Codex peer record")
	}
	dir := filepath.Dir(clean)
	session := filepath.Base(dir)
	if filepath.Base(filepath.Dir(dir)) != "codex" || filepath.Base(filepath.Dir(filepath.Dir(dir))) != "peers" {
		return nil, fmt.Errorf("not under the Codex peer records")
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
	var record codexPeerRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("peer record is not valid JSON")
	}
	if record.Version != codexPeerRecordVersion {
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
	return &codexPeer{record: record, dir: dir}, nil
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
// Codex session rather than as the supervisor.
func (p *codexPeer) provider(ctx context.Context) (*codexPeerProvider, error) {
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
	return &codexPeerProvider{MessageProvider: shared, asksDir: filepath.Join(p.dir, "asks")}, nil
}

// codexPeerProvider adds the inbound-ask handoff to the shared child provider.
// The poller records each ask it delivered; the router asks this provider to
// claim one before it answers, so an ask answered here cannot also be answered
// by a second consumer.
type codexPeerProvider struct {
	*childmessaging.MessageProvider
	asksDir string
}

// ClaimAsk takes ownership of one pending ask by renaming its record. A rename
// is the handoff: exactly one process can win it, and the loser reports the
// asker as unknown, which the router renders as edge-absent.
func (p *codexPeerProvider) ClaimAsk(messageID string) (string, bool) {
	if p.asksDir == "" || !codexAskID.MatchString(messageID) {
		return "", false
	}
	src := filepath.Join(p.asksDir, messageID+".json")
	dst := filepath.Join(p.asksDir, messageID+".claimed")
	if err := os.Rename(src, dst); err != nil {
		return "", false
	}
	raw, err := os.ReadFile(dst)
	if err != nil {
		return "", false
	}
	var ask struct {
		AskerRunID string `json:"asker_run_id"`
	}
	if err := json.Unmarshal(raw, &ask); err != nil || ask.AskerRunID == "" {
		return "", false
	}
	return ask.AskerRunID, true
}

// RestoreAsk renames a claimed ask back after a failed reply, so the model's
// retry still finds the edge. The record already holds the asker the poller
// wrote, so one atomic rename restores it: no write sits between the pair for a
// concurrent claim to lose, and a caller-supplied asker is never trusted.
func (p *codexPeerProvider) RestoreAsk(messageID, askerRunID string) {
	if p.asksDir == "" || !codexAskID.MatchString(messageID) {
		return
	}
	claimed := filepath.Join(p.asksDir, messageID+".claimed")
	if _, err := os.Stat(claimed); err != nil {
		return
	}
	_ = os.Rename(claimed, filepath.Join(p.asksDir, messageID+".json"))
}

// splitCodexPeerFile removes the hook's binding from the router's input. The
// router validates a closed property set, so the binding never reaches it; a
// malformed envelope is left alone for the router to report in its own words.
func splitCodexPeerFile(raw json.RawMessage) (json.RawMessage, string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return raw, "", nil
	}
	binding, ok := fields[codexPeerFileArg]
	if !ok {
		return raw, "", nil
	}
	delete(fields, codexPeerFileArg)
	var file string
	if err := json.Unmarshal(binding, &file); err != nil {
		return nil, "", fmt.Errorf("codex_peer_file must be a string")
	}
	cleaned, err := json.Marshal(fields)
	if err != nil {
		return nil, "", err
	}
	return cleaned, file, nil
}

// messageSurface is the provider one merged-tool call routes through, plus the
// supervisor binding the owner surface records for a later withdraw.
type messageSurface struct {
	provider       router.Provider
	supervisorPath string
	cleanup        func()
}

// messageSurfaceFor chooses the caller's identity. A bound Codex call speaks as
// its own run; resume keeps the owner path because a broker-authenticated run
// may only continue a run it spawned.
func (s *Server) messageSurfaceFor(ctx context.Context, raw json.RawMessage, peerFile string, peerAllowed bool) (messageSurface, error) {
	if peerAllowed && peerFile != "" {
		peer, err := readCodexPeerRecord(peerFile)
		if err != nil {
			return messageSurface{}, &router.ToolError{
				Code:    "refused",
				Message: "the avenor Codex peer record cannot be used: " + err.Error(),
				Facts:   map[string]string{},
			}
		}
		provider, err := peer.provider(ctx)
		if err != nil {
			return messageSurface{}, &router.ToolError{
				Code:    "refused",
				Message: "the avenor Codex peer record cannot be used: " + err.Error(),
				Facts:   map[string]string{},
			}
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

// messageAction reads the action the router will route. An absent action is the
// peers default and never reaches here for the message tool.
func messageAction(raw json.RawMessage) string {
	var fields struct {
		Action string `json:"action"`
	}
	_ = json.Unmarshal(raw, &fields)
	return strings.TrimSpace(fields.Action)
}
