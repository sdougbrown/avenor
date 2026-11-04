package localhttp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
)

// TestMalformedErrorBodyRetainsStatus verifies the status survives a bad envelope.
func TestMalformedErrorBodyRetainsStatus(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("{bad"))
	}))
	defer s.Close()
	tr, err := localhttp.New("", messaging.LocalCredentials{RunID: "run", Token: "token", BaseURL: s.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	_, err = tr.Preflight(context.Background())
	var status *localhttp.StatusError
	if !errors.As(err, &status) || status.Status != 503 || !errors.Is(err, localhttp.ErrUnsupportedLocalProtocol) {
		t.Fatalf("expected 503 wrapping protocol error, got %v", err)
	}
}
