package runtime

import (
	"strings"

	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// Environment variables that carry an avenor broker peer identity: the run the
// plugin's mods layer (marketplace/plugins/avenor/hooks) joins as instead of
// registering one of its own. A launch either provisions them or strips them.
// A child never inherits a launcher's copy — a session that loaded the plugin
// exports these into everything it spawns, and a tmux pane inherits its
// server's environment, which can hold them from a launch days back. Either
// way the child would join the broker as a run another live session is already
// using.
//
// The strip is not a per-backend duty: every name below is in ScrubbedEnvNames,
// so every launch that goes through LaunchEnv (or unsets that list) removes an
// inherited copy. Provisioned values are what a launch appends after that
// scrub, and they are one credential's worth (PeerProvision): a backend whose
// harness authenticates each request exports the run's token, while one whose
// harness trades a credential at session start exports a one-shot attach
// credential instead, so a run has one inbound consumer (see Broker.MintAttach).
const (
	EnvBrokerURL   = "AVENOR_BROKER_URL"
	EnvBrokerRunID = "AVENOR_RUN_ID"
	EnvBrokerToken = "AVENOR_BROKER_TOKEN"
)

// envRetiredPeerName is the retired spelling of the launch label. It is never
// emitted and only scrubbed: EnvLabel is the single authoritative name, and a
// child that inherited a launcher's copy would name itself after a session it
// is not part of.
const envRetiredPeerName = "AVENOR_SESSION_NAME"

var peerEnvNames = []string{EnvBrokerURL, EnvBrokerRunID, EnvBrokerToken}

// PeerEnvNames returns the peer credential vocabulary: the names a launch
// either provisions or keeps out of its child. It is a subset of
// ScrubbedEnvNames, which is the enforcement point — a backend does not strip
// these itself. The list stays separate so peer-specific code (a provisioner, a
// terminal prefix, a child-observation fixture) can name exactly what it is
// talking about.
func PeerEnvNames() []string {
	return append([]string(nil), peerEnvNames...)
}

// ScrubPeerEnv returns environ without any inherited peer credential. Matching
// is on the exact key as in ScrubIdentityEnv: duplicates and bare-key entries
// go, while a near-miss name such as AVENOR_RUN_ID_SUFFIX keeps its order.
//
// ScrubbedEnvNames already contains these names, so a launch that scrubs or
// unsets the shared list has nothing left for this to do. It remains for a
// caller that holds an environment it has not scrubbed itself and must remove
// only the peer vocabulary.
func ScrubPeerEnv(environ []string) []string {
	kept := make([]string, 0, len(environ))
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if IsPeerEnvName(name) {
			continue
		}
		kept = append(kept, entry)
	}
	return kept
}

// PeerProvision is the broker identity one launch hands its child: the run the
// launcher already holds, resolved at the launcher's broker.
type PeerProvision struct {
	broker *broker.Broker
	URL    string
	RunID  string
	// RunToken authenticates as the run for as long as the run lives. A backend
	// whose harness calls the broker per request exports it directly.
	RunToken string
}

// ProvisionPeer reports the peer identity a launch hands its child, or nothing.
// URL, run id and run token travel together — a half-configured child would
// poll with an identity the broker cannot authenticate — and the URL comes
// from Broker.URL, so no backend spells the address a second way.
func ProvisionPeer(opts StartOptions) *PeerProvision {
	if opts.Broker == nil || opts.RuntimeID == "" || opts.BrokerToken == "" {
		return nil
	}
	url := opts.Broker.URL()
	if url == "" {
		return nil
	}
	return &PeerProvision{
		broker:   opts.Broker,
		URL:      url,
		RunID:    opts.RuntimeID,
		RunToken: opts.BrokerToken,
	}
}

// Run reports the run's own credentials, for a backend that hands them to a
// harness authenticating each broker request itself. It is nil-safe: an
// unprovisioned launch reports three empty strings.
func (p *PeerProvision) Run() (url, runID, token string) {
	if p == nil {
		return "", "", ""
	}
	return p.URL, p.RunID, p.RunToken
}

// AttachEnv renders the peer vocabulary with a freshly minted one-shot attach
// credential in place of the run token. It is for a harness that trades the
// credential for the run's at session start: the broker spends the grant on the
// first session that presents it, so a nested session that inherits this
// environment cannot join the same run. Minting here, per launch, is what gives
// a resume or a retry a fresh credential. Nil when the broker will not mint one
// for this run, so the launch then carries no peer credentials at all.
func (p *PeerProvision) AttachEnv() []string {
	if p == nil {
		return nil
	}
	attach, ok := p.broker.MintAttach(p.RunID)
	if !ok {
		return nil
	}
	return p.env(attach)
}

func (p *PeerProvision) env(token string) []string {
	if p == nil {
		return nil
	}
	return []string{
		EnvBrokerURL + "=" + p.URL,
		EnvBrokerRunID + "=" + p.RunID,
		EnvBrokerToken + "=" + token,
	}
}

// IsPeerEnvName reports whether name is one of the provisioned peer
// credentials. Callers pass a name, not a NAME=VALUE entry.
func IsPeerEnvName(name string) bool {
	for _, peer := range peerEnvNames {
		if name == peer {
			return true
		}
	}
	return false
}
