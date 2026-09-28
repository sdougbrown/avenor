package workflowcontroller

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// LeaseTTL is the leadership lease duration. A lease is expired iff now is
// strictly after its ExpiresAt.
const LeaseTTL = 30 * time.Second

// RenewInterval is the advisory cadence at which the lease holder renews its
// lease; renewals must land well inside LeaseTTL.
const RenewInterval = 10 * time.Second

// leaderExpiredReason and leaderTimeoutReason distinguish a lease expiry
// observed at restart recovery from one observed live on an acquisition.
const (
	leaderRecoveryReason = "recovery"
	leaderTimeoutReason  = "timeout"
)

// newLeaseID returns a fresh random lease identifier.
func newLeaseID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "lease_" + hex.EncodeToString(buf), nil
}

// leaseExpired reports whether lease has lapsed: now is strictly after its
// ExpiresAt. A nil lease is never expired.
func leaseExpired(now time.Time, lease *LeaderLease) bool {
	return lease != nil && now.After(lease.ExpiresAt)
}

// checkLeaseCAS validates the compare-and-swap guard shared by renew and
// release: the record must hold a lease with the given id, and both the lease's
// and the record's owner epoch must match ownerEpoch.
func checkLeaseCAS(rec ControllerRecord, leaseID string, ownerEpoch int64) error {
	if rec.Leader == nil {
		return ErrConflict
	}
	if rec.Leader.LeaseID != leaseID || rec.Leader.OwnerEpoch != ownerEpoch || rec.OwnerEpoch != ownerEpoch {
		return ErrConflict
	}
	return nil
}
