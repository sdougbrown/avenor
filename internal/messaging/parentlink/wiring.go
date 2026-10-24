package parentlink

import "github.com/sdougbrown/avenor/internal/messaging"

// WiringTarget is the broker surface Install wires a parent link onto. The
// interface keeps this package from importing the broker package while every
// host that installs a link runs the same install sequence.
type WiringTarget interface {
	LocalMessenger() messaging.Messenger
	SetParentLinkToken(token string)
	SetMessenger(messaging.Messenger)
	SetParentLink(messaging.ParentLink)
	SetRunObserver(fn func())
}

// Install builds the Messenger for opts and installs it on target: the
// parent-route credential first, then the local messenger delegate, the
// parent-link seam and the run observer. The credential goes first so the
// parent routes never serve without it, and the messenger seam before the
// parent-link seam so a registration accepted in between still routes through
// the link. Every host that installs a link — test branch or supervisor — runs
// this sequence; the caller owns Start and Close.
func Install(opts Options, target WiringTarget) (*Messenger, error) {
	link, err := New(opts, target.LocalMessenger())
	if err != nil {
		return nil, err
	}
	target.SetParentLinkToken(opts.Token)
	target.SetMessenger(link)
	target.SetParentLink(link)
	target.SetRunObserver(link.RunChanged)
	return link, nil
}
