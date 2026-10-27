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
	label string
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
	addr := merged.Broker.Addr()
	if addr == "" {
		return nil
	}
	return &peerProvision{
		url:   "http://" + addr,
		runID: merged.RuntimeID,
		token: merged.BrokerToken,
		label: merged.Label,
	}
}

// env renders the provision for the launcher. A nil provision renders nothing:
// a child with no provision joins no broker, and the mods layer stays off
// there. The label is not decorative — without it the mods layer falls back to
// naming the run by its id, which would replace the label peers address it by.
func (p *peerProvision) env() []string {
	if p == nil {
		return nil
	}
	env := []string{
		"AVENOR_BROKER_URL=" + p.url,
		"AVENOR_RUN_ID=" + p.runID,
		"AVENOR_BROKER_TOKEN=" + p.token,
	}
	if p.label != "" {
		env = append(env, "AVENOR_SESSION_NAME="+p.label)
	}
	return env
}
