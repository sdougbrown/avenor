package runtime

import "strings"

// Environment variables that carry an avenor broker peer identity: the run the
// plugin's mods layer (marketplace/plugins/avenor/hooks) joins as instead of
// registering one of its own. A launch either provisions them or strips them.
// A child never inherits a launcher's copy — a session that loaded the plugin
// exports these into everything it spawns, and a tmux pane inherits its
// server's environment, which can hold them from a launch days back. Either
// way the child would join the broker as a run another live session is already
// using.
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

// PeerEnvNames returns the peer credential vocabulary. Every backend that
// launches a child strips this whole set and then exports what it provisions,
// so the strip list has one home rather than a copy per backend.
func PeerEnvNames() []string {
	return append([]string(nil), peerEnvNames...)
}

// ScrubPeerEnv returns environ without any inherited peer credential. Matching
// is on the exact key as in ScrubIdentityEnv: duplicates and bare-key entries
// go, while a near-miss name such as AVENOR_RUN_ID_SUFFIX keeps its order.
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
