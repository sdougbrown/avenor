package parentlink

import "github.com/sdougbrown/avenor/internal/messaging"

// WiringTarget is the broker surface Install wires a parent link onto. The
// interface keeps this package from importing the broker package while every
// host that installs a link runs the same install sequence.
type WiringTarget interface {
	LocalMessenger() messaging.Messenger
	SetMessenger(messaging.Messenger)
	SetParentLink(messaging.ParentLink)
	SetRunObserver(fn func())
}

// Install builds the Messenger for opts and installs it on target as the local
// messenger delegate, the parent-link seam and the run observer. Test helpers
// wire branches through it so a test branch is wired exactly the way the
// supervisor wires one; the supervisor keeps its own call because it adds the
// parent-route credential and owns the context Start runs under. The caller
// owns Start and Close.
func Install(opts Options, target WiringTarget) (*Messenger, error) {
	link, err := New(opts, target.LocalMessenger())
	if err != nil {
		return nil, err
	}
	target.SetMessenger(link)
	target.SetParentLink(link)
	target.SetRunObserver(link.RunChanged)
	return link, nil
}
