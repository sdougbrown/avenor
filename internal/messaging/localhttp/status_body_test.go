package localhttp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
)

// TestOversizedResponsePreservesProtocolClassification verifies both error identities.
func TestOversizedResponsePreservesProtocolClassification(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", (1<<20)+1)))
	}))
	defer s.Close()
	tr, err := localhttp.New("", messaging.LocalCredentials{RunID: "run", Token: "token", BaseURL: s.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	_, err = tr.Preflight(context.Background())
	if !errors.Is(err, localhttp.ErrResponseTooLarge) || !errors.Is(err, localhttp.ErrUnsupportedLocalProtocol) {
		t.Fatalf("expected both error identities, got %v", err)
	}
}

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
