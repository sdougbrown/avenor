package messaging

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Message IDs are the shared client's ordering primitive: the acceptance
// deadline is encoded inside the ID, so a caller cannot extend it by mutating
// a request field and two IDs with different deadlines are always different
// IDs.
const (
	msgIDVersion = "av1"
	msgIDParts   = 3
	nonceBytes   = 16
)

// NewMsgID mints a versioned message ID whose encoded acceptance deadline is
// deadline. A zero deadline means now + MaxWait. The deadline must lie in the
// future and within MaxAcceptanceHorizon of now.
func NewMsgID(now, deadline time.Time) (string, error) {
	if deadline.IsZero() {
		deadline = now.Add(MaxWait)
	}
	deadline = deadline.UTC().Truncate(time.Millisecond)
	span := deadline.Sub(now)
	switch {
	case span <= 0:
		return "", ErrMessageIDExpired
	case span > MaxAcceptanceHorizon:
		return "", ErrMessageIDHorizon
	}
	nonce, err := nonceHex(nonceBytes)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s.%s", msgIDVersion, strconv.FormatInt(deadline.UnixMilli(), 36), nonce), nil
}

// ParseMsgID validates a versioned ID against the receiver's clock and returns
// its encoded acceptance deadline. An ID outside the format returns
// ErrMalformedMessageID, which the legacy local path may treat as an
// unversioned ID.
func ParseMsgID(id string, now time.Time) (time.Time, error) {
	parts := strings.Split(id, ".")
	if len(parts) != msgIDParts || parts[0] != msgIDVersion {
		return time.Time{}, ErrMalformedMessageID
	}
	ms, err := strconv.ParseInt(parts[1], 36, 64)
	if err != nil {
		return time.Time{}, ErrMalformedMessageID
	}
	if raw, err := hex.DecodeString(parts[2]); err != nil || len(raw) != nonceBytes {
		return time.Time{}, ErrMalformedMessageID
	}
	deadline := time.UnixMilli(ms).UTC()
	if !deadline.After(now) {
		return time.Time{}, ErrMessageIDExpired
	}
	if deadline.After(now.Add(MaxAcceptanceHorizon)) {
		return time.Time{}, ErrMessageIDHorizon
	}
	return deadline, nil
}

// NewGeneration mints an opaque broker generation.
func NewGeneration() string {
	gen, err := nonceHex(nonceBytes)
	if err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return gen
}

func nonceHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}
