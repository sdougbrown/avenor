package agy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"google.golang.org/protobuf/proto"
)

var errGRPCWebStreamClosed = errors.New("agy RPC stream is closed")

// agentStateUpdateStream is a typed, server-streaming gRPC-Web response.
//
// Recv supports one caller at a time. Concurrent Recv calls fail rather than
// sharing a frame reader. Close is idempotent and may be called concurrently
// with a blocked Recv; it closes the response body to unblock that call.
type agentStateUpdateStream struct {
	ctx  context.Context
	body io.ReadCloser
	done chan struct{}

	closeOnce sync.Once
	closed    atomic.Bool
	terminal  atomic.Bool
	inRecv    atomic.Bool
}

// streamAgentStateUpdates opens the 1.1.5 typed state-update stream. It does
// not add the unary request timeout, retry, reconnect, or interpret terminal
// agent state; those lifecycle decisions belong to Stage 12.
func (c *rpcClient) streamAgentStateUpdates(ctx context.Context, request *agyv115.StreamAgentStateUpdatesRequest) (*agentStateUpdateStream, error) {
	if request == nil {
		return nil, errors.New("agy RPC stream request is required")
	}
	payload, err := proto.Marshal(request)
	if err != nil {
		return nil, errors.New("agy RPC stream request marshal failed")
	}
	if len(payload) > maxGRPCWebBody-5 {
		return nil, errors.New("agy RPC stream request body exceeds limit")
	}
	var body bytes.Buffer
	body.Grow(len(payload) + 5)
	if err := writeGRPCWebData(&body, payload); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+languageServerService+"/StreamAgentStateUpdates", &body)
	if err != nil {
		return nil, errors.New("agy RPC stream request construction failed")
	}
	req.Header.Set("Content-Type", grpcWebContentType)
	req.Header.Set("Accept", grpcWebContentType)
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("agy RPC stream request failed")
	}
	if err := validateGRPCWebResponse(resp); err != nil {
		_ = resp.Body.Close()
		return nil, err
	}

	stream := &agentStateUpdateStream{ctx: ctx, body: resp.Body, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.Close()
		case <-stream.done:
		}
	}()
	return stream, nil
}

// Recv returns the next generated state-update response. A valid successful
// gRPC-Web trailer returns io.EOF. EOF before that trailer is a transport
// error, not clean stream completion.
func (s *agentStateUpdateStream) Recv() (*agyv115.StreamAgentStateUpdatesResponse, error) {
	if s == nil {
		return nil, errGRPCWebStreamClosed
	}
	if !s.inRecv.CompareAndSwap(false, true) {
		return nil, errors.New("agy RPC stream does not support concurrent Recv")
	}
	defer s.inRecv.Store(false)
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if s.terminal.Load() {
		return nil, io.EOF
	}
	if s.closed.Load() {
		return nil, errGRPCWebStreamClosed
	}

	payload, err := readGRPCWebStreamFrame(s.ctx, s.body)
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.terminal.Store(true)
		}
		_ = s.Close()
		if s.ctx.Err() != nil {
			return nil, s.ctx.Err()
		}
		return nil, err
	}
	response := new(agyv115.StreamAgentStateUpdatesResponse)
	if err := proto.Unmarshal(payload, response); err != nil {
		_ = s.Close()
		return nil, errors.New("agy RPC stream response unmarshal failed")
	}
	return response, nil
}

// Close is safe to call repeatedly and concurrently with Recv. Closing the
// response body is deliberately the cancellation mechanism for a blocked read.
func (s *agentStateUpdateStream) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.done)
		_ = s.body.Close()
	})
	return nil
}

// readGRPCWebStreamFrame reads exactly one streaming frame. It intentionally
// differs from readGRPCWebUnary: multiple data frames are normal here, while a
// trailer terminates the protocol stream immediately; Recv then closes the
// transport body without consuming any suffix bytes. No frame payload is
// exposed except to generated protobuf decoding.
func readGRPCWebStreamFrame(ctx context.Context, r io.Reader) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errors.New("agy RPC stream is truncated or missing grpc trailer")
		}
		return nil, errors.New("agy RPC stream read failed")
	}
	flag := header[0]
	if flag != grpcWebDataFlag && flag != grpcWebTrailerFlag {
		return nil, errors.New("agy RPC stream has unknown or compressed frame flag")
	}
	length := binary.BigEndian.Uint32(header[1:])
	if flag == grpcWebTrailerFlag && length > maxGRPCWebTrailer {
		return nil, errors.New("agy RPC stream grpc trailer exceeds limit")
	}
	if flag == grpcWebDataFlag && length > maxGRPCWebBody {
		return nil, errors.New("agy RPC stream frame exceeds limit")
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, errors.New("agy RPC stream is truncated")
	}
	if flag == grpcWebDataFlag {
		return payload, nil
	}

	status, err := parseGRPCWebTrailer(payload)
	if err != nil {
		return nil, err
	}
	if status != 0 {
		return nil, &grpcStatusError{status: status}
	}
	// The gRPC-Web trailer is the protocol terminator. Recv closes the response
	// body immediately on io.EOF, so bytes after it are never consumed or
	// accepted. Waiting for transport EOF here would let a peer hold a completed
	// stream open indefinitely.
	return nil, io.EOF
}
