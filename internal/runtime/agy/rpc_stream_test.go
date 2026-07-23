package agy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"google.golang.org/protobuf/proto"
)

func grpcStreamReply(t *testing.T, messages ...proto.Message) []byte {
	t.Helper()
	var body []byte
	for _, message := range messages {
		payload, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		body = append(body, grpcFrame(grpcWebDataFlag, payload)...)
	}
	return append(body, grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...)
}

func TestGRPCWebStreamFraming(t *testing.T) {
	first := []byte{1, 2}
	second := []byte{3, 4}
	fragmented := append(append(grpcFrame(grpcWebDataFlag, first), grpcFrame(grpcWebDataFlag, second)...), grpcFrame(grpcWebTrailerFlag, []byte("gRpC-StAtUs: 0\r\ngRpc-MeSsAgE: okay%20now\r\n"))...)
	reader := &oneByteReader{data: fragmented}
	for index, want := range [][]byte{first, second} {
		got, err := readGRPCWebStreamFrame(context.Background(), reader)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("frame %d = %x, %v", index, got, err)
		}
	}
	if _, err := readGRPCWebStreamFrame(context.Background(), reader); !errors.Is(err, io.EOF) {
		t.Fatalf("success trailer = %v, want io.EOF", err)
	}

	tooLarge := []byte{grpcWebDataFlag, 2, 0, 0, 1}
	cases := []struct {
		name string
		body []byte
	}{
		{"truncated header", []byte{grpcWebDataFlag, 0}},
		{"truncated payload", grpcFrame(grpcWebDataFlag, []byte{1})[:5]},
		{"missing trailer", grpcFrame(grpcWebDataFlag, nil)},
		{"unknown flag", grpcFrame(0x02, nil)},
		{"compressed data", grpcFrame(0x01, nil)},
		{"compressed trailer", grpcFrame(0x81, nil)},
		{"oversized data", tooLarge},
		{"oversized trailer", []byte{grpcWebTrailerFlag, 0, 0, 0x20, 0x01}},
		{"missing status", grpcFrame(grpcWebTrailerFlag, []byte("grpc-message: no\r\n"))},
		{"duplicate status", grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\nGrpc-Status: 0\r\n"))},
		{"malformed status", grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: z\r\n"))},
		{"malformed message", grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\ngrpc-message: bad%ZZ\r\n"))},
		{"duplicate message", grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\ngrpc-message: one\r\nGrpc-Message: two\r\n"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := bytes.NewReader(tc.body)
			_, err := readGRPCWebStreamFrame(context.Background(), reader)
			if err == nil { // A valid data frame still needs a terminal trailer.
				_, err = readGRPCWebStreamFrame(context.Background(), reader)
			}
			if err == nil {
				t.Fatal("stream frame was accepted")
			}
		})
	}

	for _, extra := range [][]byte{
		grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n")),
		grpcFrame(grpcWebDataFlag, nil),
	} {
		body := append(grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n")), extra...)
		reader := bytes.NewReader(body)
		if _, err := readGRPCWebStreamFrame(context.Background(), reader); !errors.Is(err, io.EOF) {
			t.Fatalf("terminal trailer = %v, want io.EOF", err)
		}
		if reader.Len() != len(extra) {
			t.Fatalf("stream reader consumed %d bytes after terminal trailer", len(extra)-reader.Len())
		}
	}

	_, err := readGRPCWebStreamFrame(context.Background(), bytes.NewReader(grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 7\r\ngrpc-message: sensitive%20detail\r\n"))))
	if err == nil || !errors.As(err, new(*grpcStatusError)) || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("trailer status = %v", err)
	}
	_, err = readGRPCWebStreamFrame(context.Background(), bytes.NewReader(grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 17\r\n"))))
	var statusErr *grpcStatusError
	if err == nil || errors.As(err, &statusErr) {
		t.Fatalf("out-of-range trailer grpc-status = %v", err)
	}
}

func TestStreamAgentStateUpdatesWireAndTypedDecode(t *testing.T) {
	request := &agyv115.StreamAgentStateUpdatesRequest{ConversationId: "opaque-conversation"}
	update := &agyv115.StreamAgentStateUpdatesResponse{Update: &agyv115.AgentStateUpdate{
		Status:    agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING,
		FullyIdle: true,
		MainTrajectoryUpdate: &agyv115.TrajectoryUpdate{StepsUpdate: &agyv115.StepsUpdate{
			Indices: []uint32{4}, TotalLength: 5,
		}},
	}}
	seenPath := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath <- r.URL.Path
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != grpcWebContentType || r.Header.Get("Accept") != grpcWebContentType {
			t.Errorf("request headers/method = %s %q %q", r.Method, r.Header.Get("Content-Type"), r.Header.Get("Accept"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) < 5 || body[0] != grpcWebDataFlag {
			t.Error("invalid grpc-web stream request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if int(body[1])<<24|int(body[2])<<16|int(body[3])<<8|int(body[4]) != len(body)-5 {
			t.Error("invalid grpc-web stream request length")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotRequest := new(agyv115.StreamAgentStateUpdatesRequest)
		if err := proto.Unmarshal(body[5:], gotRequest); err != nil || !proto.Equal(gotRequest, request) {
			t.Errorf("typed request = %#v, %v", gotRequest, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payload, err := proto.Marshal(update)
		if err != nil {
			t.Error(err)
			return
		}
		payload = append(payload, 0x98, 0x06, 0x01) // field 99 remains unknown to the minimal schema.
		w.Header().Set("Content-Type", grpcWebContentType+"; charset=utf-8")
		_, _ = w.Write(append(grpcFrame(grpcWebDataFlag, payload), grpcFrame(grpcWebTrailerFlag, []byte("grpc-status: 0\r\n"))...))
	}))
	defer server.Close()
	client := newStreamTestClient(t, server)
	defer client.close()

	stream, err := client.streamAgentStateUpdates(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	path := <-seenPath
	if path != "/exa.language_server_pb.LanguageServerService/StreamAgentStateUpdates" || response.GetUpdate().GetStatus() != agyv115.CascadeRunStatus_CASCADE_RUN_STATUS_RUNNING || !response.GetUpdate().GetFullyIdle() || response.GetUpdate().GetMainTrajectoryUpdate().GetStepsUpdate().GetIndices()[0] != 4 {
		t.Fatalf("path/update = %q %#v", path, response.GetUpdate())
	}
	if got := response.ProtoReflect().GetUnknown(); !bytes.Equal(got, []byte{0x98, 0x06, 0x01}) {
		t.Fatalf("unknown response fields = %x", got)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal stream error = %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stable terminal stream error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamAgentStateUpdatesHTTPValidationAndNoRetry(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		headers func(http.Header)
	}{
		{"http status", http.StatusServiceUnavailable, func(http.Header) {}},
		{"content type", http.StatusOK, func(h http.Header) { h.Set("Content-Type", "application/json") }},
		{"duplicate grpc status", http.StatusOK, func(h http.Header) {
			h.Set("Content-Type", grpcWebContentType)
			h.Add("Grpc-Status", "0")
			h.Add("Grpc-Status", "7")
		}},
		{"invalid grpc status", http.StatusOK, func(h http.Header) { h.Set("Content-Type", grpcWebContentType); h.Set("Grpc-Status", "x") }},
		{"grpc status error", http.StatusOK, func(h http.Header) {
			h.Set("Content-Type", grpcWebContentType)
			h.Set("Grpc-Status", "7")
			h.Set("Grpc-Message", "sensitive%20detail")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				tc.headers(w.Header())
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			client := newStreamTestClient(t, server)
			defer client.close()
			_, err := client.streamAgentStateUpdates(context.Background(), &agyv115.StreamAgentStateUpdatesRequest{})
			if err == nil || calls.Load() != 1 || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("open error/calls = %v/%d", err, calls.Load())
			}
		})
	}
}

func TestStreamAgentStateUpdatesClosesBodyOnSetupError(t *testing.T) {
	body := &streamCloseSpy{ReadCloser: io.NopCloser(strings.NewReader("ignored"))}
	client := &rpcClient{
		baseURL: "http://127.0.0.1:1",
		http: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
		})},
	}
	if _, err := client.streamAgentStateUpdates(context.Background(), &agyv115.StreamAgentStateUpdatesRequest{}); err == nil {
		t.Fatal("incompatible setup response was accepted")
	}
	if !body.closed.Load() {
		t.Fatal("setup-error response body was not closed")
	}
}

func TestStreamAgentStateUpdatesCancellationAndClose(t *testing.T) {
	open := func(t *testing.T, ctx context.Context, body *blockingReadCloser) *agentStateUpdateStream {
		t.Helper()
		client := &rpcClient{baseURL: "http://127.0.0.1:1", http: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{grpcWebContentType}}, Body: body}, nil
		})}}
		stream, err := client.streamAgentStateUpdates(ctx, &agyv115.StreamAgentStateUpdatesRequest{})
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelBody := newBlockingReadCloser()
	stream := open(t, ctx, cancelBody)
	recv := make(chan error, 1)
	go func() { _, err := stream.Recv(); recv <- err }()
	<-cancelBody.entered
	cancel()
	select {
	case err := <-recv:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Recv = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not unblock blocked body read")
	}

	closeBody := newBlockingReadCloser()
	stream = open(t, context.Background(), closeBody)
	blocked := make(chan error, 1)
	go func() { _, err := stream.Recv(); blocked <- err }()
	<-closeBody.entered
	var closes sync.WaitGroup
	for range 4 {
		closes.Add(1)
		go func() { defer closes.Done(); _ = stream.Close() }()
	}
	closes.Wait()
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("Close allowed blocked Recv to succeed")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock blocked body read")
	}
	if !closeBody.closed.Load() {
		t.Fatal("Close did not close response body")
	}
	if _, err := stream.Recv(); !errors.Is(err, errGRPCWebStreamClosed) {
		t.Fatalf("closed Recv = %v", err)
	}
}

func TestStreamAgentStateUpdatesRejectsConcurrentRecv(t *testing.T) {
	started := make(chan struct{})
	releaseServer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", grpcWebContentType)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		select {
		case <-r.Context().Done():
		case <-releaseServer:
		}
	}))
	defer server.Close()
	defer close(releaseServer)
	client := newStreamTestClient(t, server)
	defer client.close()
	stream, err := client.streamAgentStateUpdates(context.Background(), &agyv115.StreamAgentStateUpdatesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	first := make(chan error, 1)
	go func() { _, err := stream.Recv(); first <- err }()
	waitForStreamRecv(t, stream)
	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "concurrent") {
		t.Fatalf("concurrent Recv = %v", err)
	}
	_ = stream.Close()
	<-first
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type blockingReadCloser struct {
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
	closed    atomic.Bool
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingReadCloser) Read([]byte) (int, error) {
	b.enterOnce.Do(func() { close(b.entered) })
	<-b.release
	return 0, io.ErrClosedPipe
}

func (b *blockingReadCloser) Close() error {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		close(b.release)
	})
	return nil
}

type streamCloseSpy struct {
	io.ReadCloser
	closed atomic.Bool
}

func (s *streamCloseSpy) Close() error {
	s.closed.Store(true)
	return s.ReadCloser.Close()
}

func waitForStreamRecv(t *testing.T, stream *agentStateUpdateStream) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !stream.inRecv.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !stream.inRecv.Load() {
		t.Fatal("Recv did not begin")
	}
}

func newStreamTestClient(t *testing.T, server *httptest.Server) *rpcClient {
	t.Helper()
	client, err := newRPCEndpointClient(rpcEndpoint{address: strings.TrimPrefix(server.URL, "http://")})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
