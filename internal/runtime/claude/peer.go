package claude

import "github.com/sdougbrown/avenor/internal/runtime"

// peerProvision is the broker identity a spawned Claude session joins as. The
// plugin's mods layer (marketplace/plugins/avenor/hooks) reads it from the
// environment and registers as this run instead of minting one of its own, so
// `avenor:<label>` keeps pointing at the single record the supervisor already
// provisioned for the child.
type peerProvision struct {
	url   string
	runID string
	token string
}

// provisionPeer reports the broker identity this launch hands the child. The
// hosting supervisor provisions a run for every child it starts and passes the
// run id and its token in StartOptions, so the child adopts that identity
// rather than getting a second record under the same label — which is exactly
// what makes an address ambiguous. URL, run id and token travel together or
// not at all: a half-configured child would poll with an identity the broker
// cannot authenticate.
func provisionPeer(merged runtime.StartOptions) *peerProvision {
	if merged.Broker == nil || merged.RuntimeID == "" || merged.BrokerToken == "" {
		return nil
	}
	url := merged.Broker.URL()
	if url == "" {
		return nil
	}
	return &peerProvision{
		url:   url,
		runID: merged.RuntimeID,
		token: merged.BrokerToken,
	}
}

// env renders the provision for the launcher. A nil provision renders nothing:
// a child with no provision joins no broker, and the mods layer stays off
// there. These three names are the whole peer vocabulary (runtime.PeerEnvNames)
// and are what the launch provisions; the label the supervisor registered the
// run under travels as the launch identity's AVENOR_LABEL, so exporting it a
// second time would give one value two spellings that could disagree. The mods
// layer posts no name for a provisioned run either — a harness name would
// outrank the label the supervisor owns, and a later authorized rename would
// write behind it.
func (p *peerProvision) env() []string {
	if p == nil {
		return nil
	}
	return []string{
		runtime.EnvBrokerURL + "=" + p.url,
		runtime.EnvBrokerRunID + "=" + p.runID,
		runtime.EnvBrokerToken + "=" + p.token,
	}
}
