package mcpserver

import (
	"encoding/json"
	"os"
	"strconv"
	"syscall"
	"testing"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/control"
)

// publishOwnerRecordForStub writes the owner record a stub supervisor would
// publish beside socketPath so a launcher attach can authenticate against it.
func publishOwnerRecordForStub(t *testing.T, socketPath string) control.OwnerCapability {
	t.Helper()
	cap, err := control.GenerateOwnerCapability()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatalf("stat stub socket: %v", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat stub socket: unsupported stat type %T", info.Sys())
	}
	record := control.OwnerRecord{
		Version:        1,
		Socket:         socketPath,
		SocketIdentity: control.SocketIdentity{Dev: strconv.FormatUint(uint64(st.Dev), 10), Ino: strconv.FormatUint(st.Ino, 10)},
		CapabilityID:   cap.ID,
		Secret:         cap.Secret,
		OwnerPID:       os.Getpid(),
	}
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(control.OwnerRecordPath(socketPath), body, 0o600); err != nil {
		t.Fatalf("write stub owner record: %v", err)
	}
	return cap
}

// ownerAuthResponse answers an authenticate_owner request as cap would.
func ownerAuthResponse(req client.Request, cap control.OwnerCapability) client.Response {
	var params control.OwnerAuthRequest
	_ = json.Unmarshal(req.Params, &params)
	resp := client.Response{JSONRPC: "2.0", ID: req.ID}
	if params.Version == control.OwnerAuthVersion && params.CapabilityID == cap.ID && params.Secret == cap.Secret {
		resp.Result, _ = json.Marshal(control.OwnerAuthResult{
			Version:       control.OwnerAuthVersion,
			Authenticated: true,
			CapabilityID:  cap.ID,
		})
		return resp
	}
	data, _ := json.Marshal(control.OwnerAuthErrorData{Version: control.OwnerAuthVersion, Reason: "bad-capability"})
	resp.Error = &client.RespError{Code: control.CodeOwnerUnauthenticated, Message: "unauthenticated", Data: data}
	return resp
}
