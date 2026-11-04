package localhttp

import (
	"errors"
	"net/http"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestStatusError preserves the status and existing error classification.
func TestStatusError(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			err := statusError(envelope{Status: status})
			var statusErr *StatusError
			if !errors.As(err, &statusErr) || statusErr.Status != status {
				t.Fatalf("statusError(%d) = %v, want typed status", status, err)
			}
			want := ErrUnsupportedLocalProtocol
			if status == http.StatusUnauthorized {
				want = messaging.ErrAuthRejected
			}
			if !errors.Is(err, want) {
				t.Fatalf("statusError(%d) = %v, want %v", status, err, want)
			}
		})
	}
}
