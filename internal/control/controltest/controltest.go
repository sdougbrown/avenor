// Package controltest constructs control servers for tests in other packages
// without adding a test-only constructor to the control API.
package controltest

import "github.com/sdougbrown/avenor/internal/control"

// NewServer returns a control server and the capability it will accept, so the
// caller can authenticate as the construction owner.
func NewServer(state *control.ControlState) (*control.ControlServer, control.OwnerCapability) {
	owner, err := control.GenerateOwnerCapability()
	if err != nil {
		panic("controltest: generate owner capability: " + err.Error())
	}
	return control.NewServer(state, owner), owner
}
