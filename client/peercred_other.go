//go:build !linux && !darwin

package client

import (
	"errors"
	"net"
)

var errPeerCredUnsupported = errors.New("peer credentials are unsupported on this platform")

// peerUID reports the effective uid of the process on the other end of conn.
func peerUID(conn *net.UnixConn) (uint32, error) {
	return 0, errPeerCredUnsupported
}
