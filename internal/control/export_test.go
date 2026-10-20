package control

// NewTestServer mints a random owner capability and returns it so a test can
// authenticate explicitly.
func NewTestServer(state *ControlState) (*ControlServer, OwnerCapability) {
	owner, err := GenerateOwnerCapability()
	if err != nil {
		panic("control: failed to generate test owner capability: " + err.Error())
	}
	return NewServer(state, owner), owner
}
