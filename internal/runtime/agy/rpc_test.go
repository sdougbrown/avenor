package agy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"google.golang.org/protobuf/proto"
)

func grpcFrame(flag byte, payload []byte) []byte {
	frame := make([]byte, 5+len(payload))
	frame[0] = flag
	binary.BigEndian.PutUint32(frame[1:], uint32(len(payload)))
	copy(frame[5:], payload)
	return frame
}

func grpcReply(t *testing.T, message proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return append(grpcFrame(grpcWebDataFlag, payload), grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...)
}

type oneByteReader struct{ data []byte }

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}

func TestGRPCWebUnaryFramingHostileInputs(t *testing.T) {
	good := append(grpcFrame(grpcWebDataFlag, []byte{1, 2}), grpcFrame(grpcWebTrailerFlag, []byte("Grpc-Status: 0\r\nGrpc-Message: okay%20now\r\n"))...)
	got, err := readGRPCWebUnary(context.Background(), &oneByteReader{data: good})
	if err != nil || !bytes.Equal(got, []byte{1, 2}) {
		t.Fatalf("fragmented read = %x, %v", got, err)
	}

	tooLarge := []byte{grpcWebDataFlag, 2, 0, 0, 1}
	cases := []struct {
		name string
		body []byte
	}{
		{"truncated header", []byte{0, 0}},
		{"truncated payload", append(grpcFrame(grpcWebDataFlag, []byte{1})[:5], 1)},
		{"unknown flag", append(grpcFrame(0x01, nil), grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...)},
		{"duplicate data", append(append(grpcFrame(grpcWebDataFlag, nil), grpcFrame(grpcWebDataFlag, nil)...), grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...)},
		{"missing data", grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))},
		{"missing trailer", grpcFrame(grpcWebDataFlag, nil)},
		{"missing status", grpcFrame(grpcWebTrailerFlag, []byte("grpc-message: no\r\n"))},
		{"duplicate trailer", append(append(append(grpcFrame(grpcWebDataFlag, nil), grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...), grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...), byte(0))},
		{"data after trailer", append(append(append(grpcFrame(grpcWebDataFlag, nil), grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...), grpcFrame(grpcWebDataFlag, nil)...), byte(0))},
		{"oversized frame", tooLarge},
		{"oversized trailer", []byte{grpcWebTrailerFlag, 0, 0, 0x20, 0x01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := readGRPCWebUnary(context.Background(), bytes.NewReader(tc.body)); err == nil {
				t.Fatal("readGRPCWebUnary succeeded")
			}
		})
	}

	var aggregate bytes.Buffer
	for aggregate.Len() <= maxGRPCWebBody {
		aggregate.Write(grpcFrame(grpcWebDataFlag, make([]byte, 1<<20)))
	}
	if _, err := readGRPCWebUnary(context.Background(), &aggregate); err == nil {
		t.Fatal("aggregate body limit was not enforced")
	}
}

func TestGRPCStatusBounds(t *testing.T) {
	for _, status := range []string{"17", "999"} {
		if _, err := parseGRPCStatus(status); err == nil {
			t.Fatalf("out-of-range grpc-status %q accepted", status)
		}
	}
}

func TestGRPCWebUnaryCancellationAndStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readGRPCWebUnary(ctx, bytes.NewReader(nil)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	_, err := readGRPCWebUnary(context.Background(), bytes.NewReader(grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 2\r\ngrpc-message: secret%20message\r\n"))))
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("status error leaked message: %v", err)
	}
}

func TestTypedUnaryWrappersWireShape(t *testing.T) {
	expectedSession := "opaque-session"
	var seenMethod, seenPath string
	var seenRequest proto.Message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.Path
		parts := strings.Split(r.URL.Path, "/")
		seenMethod = parts[len(parts)-1]
		body, err := io.ReadAll(r.Body)
		if err != nil || r.Header.Get("Content-Type") != grpcWebContentType || len(body) < 5 || body[0] != grpcWebDataFlag {
			t.Error("invalid grpc-web request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payload := body[5:]
		switch seenMethod {
		case "Heartbeat":
			seenRequest = &agyv115.HeartbeatRequest{}
		case "GetConversationMetadata":
			seenRequest = &agyv115.GetConversationMetadataRequest{}
		case "GetAvailableModels":
			seenRequest = &agyv115.GetAvailableModelsRequest{}
		case "GetAllCascadeTrajectories":
			seenRequest = &agyv115.GetAllCascadeTrajectoriesRequest{}
		case "GetCascadeTrajectorySteps":
			seenRequest = &agyv115.GetCascadeTrajectoryStepsRequest{}
		case "StartCascade":
			seenRequest = &agyv115.StartCascadeRequest{}
		case "SendUserCascadeMessage":
			seenRequest = &agyv115.SendUserCascadeMessageRequest{}
		case "HandleCascadeUserInteraction":
			seenRequest = &agyv115.HandleCascadeUserInteractionRequest{}
		case "CancelCascadeSteps":
			seenRequest = &agyv115.CancelCascadeStepsRequest{}
		case "ForceStopCascadeTree":
			seenRequest = &agyv115.ForceStopCascadeTreeRequest{}
		default:
			t.Errorf("unexpected method %q", seenMethod)
			return
		}
		if err := proto.Unmarshal(payload, seenRequest); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", grpcWebContentType)
		var response proto.Message
		switch seenMethod {
		case "Heartbeat":
			response = &agyv115.HeartbeatResponse{}
		case "GetConversationMetadata":
			response = &agyv115.GetConversationMetadataResponse{Metadata: &agyv115.TrajectoryMetadata{RootConversationId: expectedSession}}
		case "GetAvailableModels":
			response = &agyv115.GetAvailableModelsResponse{}
		case "GetAllCascadeTrajectories":
			response = &agyv115.GetAllCascadeTrajectoriesResponse{}
		case "GetCascadeTrajectorySteps":
			response = &agyv115.GetCascadeTrajectoryStepsResponse{}
		case "StartCascade":
			response = &agyv115.StartCascadeResponse{}
		case "SendUserCascadeMessage":
			response = &agyv115.SendUserCascadeMessageResponse{}
		case "HandleCascadeUserInteraction":
			response = &agyv115.HandleCascadeUserInteractionResponse{}
		case "CancelCascadeSteps":
			response = &agyv115.CancelCascadeStepsResponse{}
		case "ForceStopCascadeTree":
			response = &agyv115.ForceStopCascadeTreeResponse{}
		}
		_, _ = w.Write(grpcReply(t, response))
	}))
	defer server.Close()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()

	assertRequest := func(t *testing.T, method string, want proto.Message, invoke func() error) {
		t.Helper()
		seenMethod, seenPath, seenRequest = "", "", nil
		if err := invoke(); err != nil {
			t.Fatal(err)
		}
		wantPath := "/" + languageServerService + "/" + method
		if seenMethod != method || seenPath != wantPath || !proto.Equal(seenRequest, want) {
			t.Fatalf("%s %s request = %#v, want %s %#v", seenPath, seenMethod, seenRequest, wantPath, want)
		}
	}
	ctx := context.Background()
	assertRequest(t, "Heartbeat", &agyv115.HeartbeatRequest{}, func() error { _, err := client.heartbeat(ctx); return err })
	assertRequest(t, "GetConversationMetadata", &agyv115.GetConversationMetadataRequest{ConversationId: expectedSession}, func() error { _, err := client.getConversationMetadata(ctx, expectedSession); return err })
	assertRequest(t, "GetAvailableModels", &agyv115.GetAvailableModelsRequest{ForceRefresh: true}, func() error { _, err := client.getAvailableModels(ctx, true); return err })
	assertRequest(t, "GetAllCascadeTrajectories", &agyv115.GetAllCascadeTrajectoriesRequest{ExcludeSubtrajectories: true}, func() error { _, err := client.getAllCascadeTrajectories(ctx, true); return err })
	assertRequest(t, "GetCascadeTrajectorySteps", &agyv115.GetCascadeTrajectoryStepsRequest{CascadeId: "cascade", StepOffset: 3, TrajectoryVerbosity: agyv115.StreamTrajectoryVerbosity_CLIENT_TRAJECTORY_VERBOSITY_FULL}, func() error {
		_, err := client.getCascadeTrajectorySteps(ctx, "cascade", 3, 0, agyv115.StreamTrajectoryVerbosity_CLIENT_TRAJECTORY_VERBOSITY_FULL)
		return err
	})
	assertRequest(t, "StartCascade", &agyv115.StartCascadeRequest{Source: agyv115.CortexTrajectorySource_CORTEX_TRAJECTORY_SOURCE_CLI}, func() error { _, err := client.startCascade(ctx); return err })
	assertRequest(t, "SendUserCascadeMessage", &agyv115.SendUserCascadeMessageRequest{CascadeId: "cascade", Items: []*agyv115.TextOrScopeItem{{Chunk: &agyv115.TextOrScopeItem_Text{Text: "prompt"}}}, CascadeConfig: &agyv115.CascadeConfig{PlannerConfig: &agyv115.CascadePlannerConfig{PlanModel: agyv115.Model_MODEL_GOOGLE_GEMINI_2_5_FLASH}}}, func() error {
		_, err := client.sendUserCascadeMessage(ctx, "cascade", "prompt", agyv115.Model_MODEL_GOOGLE_GEMINI_2_5_FLASH)
		return err
	})
	assertRequest(t, "HandleCascadeUserInteraction", &agyv115.HandleCascadeUserInteractionRequest{CascadeId: "cascade", Interaction: &agyv115.CascadeUserInteraction{TrajectoryId: "trajectory", StepIndex: 4, Interaction: &agyv115.CascadeUserInteraction_Deploy{Deploy: &agyv115.CascadeDeployInteraction{}}}}, func() error {
		_, err := client.handleCascadeUserInteractionDenial(ctx, "cascade", "trajectory", 4)
		return err
	})
	assertRequest(t, "CancelCascadeSteps", &agyv115.CancelCascadeStepsRequest{CascadeId: "cascade", StepIndices: []uint32{2, 4}}, func() error { _, err := client.cancelCascadeSteps(ctx, "cascade", []uint32{2, 4}); return err })
	assertRequest(t, "ForceStopCascadeTree", &agyv115.ForceStopCascadeTreeRequest{ConversationId: expectedSession}, func() error { _, err := client.forceStopCascadeTree(ctx, expectedSession); return err })

	metadata, err := client.getConversationMetadata(ctx, expectedSession)
	if err != nil || metadata.GetMetadata().GetRootConversationId() != expectedSession {
		t.Fatalf("metadata identity = %#v, %v", metadata, err)
	}
}

func TestMissingResponseContentTypeIsRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(grpcReply(t, &agyv115.HeartbeatResponse{}))
	}))
	defer server.Close()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if _, err := client.heartbeat(context.Background()); err == nil {
		t.Fatal("response without grpc-web content type was accepted")
	}
}

func TestHTTPHeaderGRPCStatusIsSurfacedWithoutMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", grpcWebContentType)
		w.Header().Set("Grpc-Status", "7")
		w.Header().Set("Grpc-Message", "sensitive%20detail")
	}))
	defer server.Close()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	_, err = client.heartbeat(context.Background())
	if err == nil || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("header grpc error leaked message: %v", err)
	}
}

func TestOutOfRangeHeaderGRPCStatusIsRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", grpcWebContentType)
		w.Header().Set("Grpc-Status", "17")
	}))
	defer server.Close()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	_, err = client.heartbeat(context.Background())
	var statusErr *grpcStatusError
	if err == nil || errors.As(err, &statusErr) {
		t.Fatalf("out-of-range header grpc-status = %v", err)
	}
}

func TestDuplicateHeaderGRPCStatusIsRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", grpcWebContentType)
		w.Header().Add("Grpc-Status", "0")
		w.Header().Add("Grpc-Status", "7")
		_, _ = w.Write(grpcReply(t, &agyv115.HeartbeatResponse{}))
	}))
	defer server.Close()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if _, err := client.heartbeat(context.Background()); err == nil {
		t.Fatal("duplicate grpc-status headers were accepted")
	}
}

func TestTypedCallPreservesUnknownResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", grpcWebContentType)
		_, _ = w.Write(append(grpcFrame(grpcWebDataFlag, []byte{0x98, 0x06, 0x01}), grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...))
	}))
	defer server.Close()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	response, err := client.heartbeat(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := proto.Marshal(response)
	if err != nil || !bytes.Equal(got, []byte{0x98, 0x06, 0x01}) {
		t.Fatalf("unknown response = %x, %v", got, err)
	}
}

func TestDiscoveryTLSPreferenceAndExplicitPlainFallback(t *testing.T) {
	session := "expected"
	var tlsCalls, plainCalls atomic.Int32
	serverHandler := func(counter *atomic.Int32, root string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			counter.Add(1)
			w.Header().Set("Content-Type", grpcWebContentType)
			if strings.HasSuffix(r.URL.Path, "/GetConversationMetadata") {
				_, _ = w.Write(grpcReply(t, &agyv115.GetConversationMetadataResponse{Metadata: &agyv115.TrajectoryMetadata{RootConversationId: root}}))
				return
			}
			_, _ = w.Write(grpcReply(t, &agyv115.HeartbeatResponse{}))
		}
	}
	tlsServer := httptest.NewTLSServer(serverHandler(&tlsCalls, session))
	defer tlsServer.Close()
	plainServer := httptest.NewServer(serverHandler(&plainCalls, session))
	defer plainServer.Close()
	oldDiscovery := ownedListenerDiscovery
	defer func() { ownedListenerDiscovery = oldDiscovery }()
	ownedListenerDiscovery = func(context.Context, int) ([]string, error) {
		return []string{strings.TrimPrefix(tlsServer.URL, "https://"), strings.TrimPrefix(plainServer.URL, "http://")}, nil
	}
	host, err := discoverRPCHost(context.Background(), 99, "1.1.5", rpcDiscoveryOptions{AllowPlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	defer host.close()
	if !host.endpoint.tls || plainCalls.Load() != 0 || tlsCalls.Load() != 1 {
		t.Fatalf("TLS preference failed: tls=%v calls=%d/%d", host.endpoint.tls, tlsCalls.Load(), plainCalls.Load())
	}
	if err := host.validateSession(context.Background(), "wrong"); err == nil {
		t.Fatal("mismatched metadata accepted")
	}
	if err := host.validateSession(context.Background(), session); err != nil || host.sessionID != session {
		t.Fatalf("session validation = %q, %v", host.sessionID, err)
	}
	if err := host.validateSession(context.Background(), "other"); err == nil {
		t.Fatal("bound host accepted another session")
	}

	tlsServer.Close()
	plainCalls.Store(0)
	host, err = discoverRPCHost(context.Background(), 99, "1.1.5", rpcDiscoveryOptions{})
	if err == nil || plainCalls.Load() != 0 {
		t.Fatalf("plain endpoint was used without explicit fallback: %v, calls=%d", err, plainCalls.Load())
	}
	host, err = discoverRPCHost(context.Background(), 99, "1.1.5", rpcDiscoveryOptions{AllowPlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	defer host.close()
	if host.endpoint.tls || plainCalls.Load() != 1 {
		t.Fatalf("plain fallback failed: tls=%v calls=%d", host.endpoint.tls, plainCalls.Load())
	}
	if err := host.validateSession(context.Background(), session); err != nil {
		t.Fatalf("plain session validation: %v", err)
	}
}

func TestDiscoveryClosesRejectedCandidates(t *testing.T) {
	var closed atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed.Add(1)
		}
	}
	server.StartTLS()
	defer server.Close()
	oldDiscovery := ownedListenerDiscovery
	defer func() { ownedListenerDiscovery = oldDiscovery }()
	ownedListenerDiscovery = func(context.Context, int) ([]string, error) {
		return []string{strings.TrimPrefix(server.URL, "https://")}, nil
	}
	if _, err := discoverRPCHost(context.Background(), 1, "1.1.5", rpcDiscoveryOptions{}); err == nil {
		t.Fatal("failed heartbeat candidate accepted")
	}
	deadline := time.Now().Add(time.Second)
	for closed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if closed.Load() == 0 {
		t.Fatal("rejected candidate connection was left idle")
	}
}

func TestDiscoveryVersionDeadlineAndCancellation(t *testing.T) {
	if _, err := discoverRPCHost(context.Background(), 1, "1.1.4", rpcDiscoveryOptions{}); err == nil {
		t.Fatal("unsupported version accepted")
	}
	oldDiscovery := ownedListenerDiscovery
	defer func() { ownedListenerDiscovery = oldDiscovery }()
	ownedListenerDiscovery = func(ctx context.Context, _ int) ([]string, error) { <-ctx.Done(); return nil, ctx.Err() }
	start := time.Now()
	if _, err := discoverRPCHost(context.Background(), 1, "1.1.5", rpcDiscoveryOptions{}); err == nil || time.Since(start) > rpcDiscoveryTimeout+500*time.Millisecond {
		t.Fatalf("discovery deadline was not bounded: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := discoverRPCHost(ctx, 1, "1.1.5", rpcDiscoveryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestEndpointRestrictionProxyBypassAndNoMutationRetry(t *testing.T) {
	if _, err := newRPCEndpointClient(rpcEndpoint{address: "0.0.0.0:1"}); err == nil {
		t.Fatal("wildcard accepted")
	}
	if _, err := newRPCEndpointClient(rpcEndpoint{address: "localhost:1"}); err == nil {
		t.Fatal("hostname accepted")
	}
	client, err := newRPCEndpointClient(rpcEndpoint{address: "127.0.0.1:1", tls: true})
	if err != nil {
		t.Fatal(err)
	}
	if reflect.ValueOf(client.transport.Proxy).IsNil() == false {
		t.Fatal("ambient proxy was retained")
	}
	if !client.transport.DisableKeepAlives {
		t.Fatal("stale RPC connections can be reused")
	}
	client.close()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err = newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if _, err := client.cancelCascadeSteps(context.Background(), "cascade", []uint32{1}); err == nil {
		t.Fatal("mutation unexpectedly succeeded")
	}
	if calls.Load() != 1 {
		t.Fatalf("mutating RPC retried %d times", calls.Load())
	}
}

func TestDarwinAndLinuxListenerParsers(t *testing.T) {
	darwin, err := os.ReadFile(rpcFixtureDir + "/darwin-lsof.txt")
	if err != nil {
		t.Fatal(err)
	}
	darwin = bytes.ReplaceAll(darwin, []byte("REDACTED_PORT6"), []byte("42421"))
	darwin = bytes.ReplaceAll(darwin, []byte("REDACTED_PORT"), []byte("42420"))
	listeners, err := parseDarwinLsofListeners(string(darwin), 4242)
	if err != nil || !reflect.DeepEqual(listeners, []string{"127.0.0.1:42420", "[::1]:42421"}) {
		t.Fatalf("darwin parse = %v, %v", listeners, err)
	}
	if _, err := parseDarwinLsofListeners("p7\nn127.0.0.1:1 (LISTEN)\n", 4242); err == nil {
		t.Fatal("PID mismatch accepted")
	}
	if _, err := parseDarwinLsofListeners("p4242\nn*:1 (LISTEN)\n", 4242); err == nil {
		t.Fatal("wildcard accepted")
	}

	tcp, err := os.ReadFile(rpcFixtureDir + "/linux-tcp.txt")
	if err != nil {
		t.Fatal(err)
	}
	tcp6, err := os.ReadFile(rpcFixtureDir + "/linux-tcp6.txt")
	if err != nil {
		t.Fatal(err)
	}
	listeners, err = parseLinuxProcListeners(string(tcp), string(tcp6), map[string]struct{}{"11111": {}, "22222": {}})
	if err != nil || !reflect.DeepEqual(listeners, []string{"127.0.0.1:42420", "[::1]:42421"}) {
		t.Fatalf("linux parse = %v, %v", listeners, err)
	}
	listeners, err = parseLinuxProcListeners(string(tcp), string(tcp6), map[string]struct{}{})
	if err != nil || len(listeners) != 0 {
		t.Fatalf("unowned sockets accepted: %v, %v", listeners, err)
	}
}
