package agy

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	agyv115 "github.com/sdougbrown/avenor/internal/runtime/agy/interop/v115"
	"google.golang.org/protobuf/proto"
)

const (
	grpcWebContentType  = "application/grpc-web+proto"
	grpcWebDataFlag     = byte(0x00)
	grpcWebTrailerFlag  = byte(0x80)
	maxGRPCWebBody      = 32 << 20
	maxGRPCWebTrailer   = 8 << 10
	rpcDiscoveryTimeout = 3 * time.Second
	rpcRequestTimeout   = 5 * time.Second
	heartbeatAttempts   = 2
)

const languageServerService = "exa.language_server_pb.LanguageServerService"

type rpcEndpoint struct {
	address string
	tls     bool
}

type rpcClient struct {
	baseURL   string
	http      *http.Client
	transport *http.Transport
}

type rpcHost struct {
	client    *rpcClient
	endpoint  rpcEndpoint
	version   string
	sessionID string

	closeOnce sync.Once
	closeHook func() // test seam; production hosts leave this nil.
}

type rpcDiscoveryOptions struct {
	AllowPlainHTTP bool
}

type grpcStatusError struct{ status int }

func (e *grpcStatusError) Error() string {
	return fmt.Sprintf("agy RPC failed with grpc-status %d", e.status)
}

// writeGRPCWebData writes one uncompressed grpc-web protobuf data envelope.
func writeGRPCWebData(w io.Writer, payload []byte) error {
	if len(payload) > maxGRPCWebBody-5 {
		return errors.New("agy RPC request body exceeds limit")
	}
	var header [5]byte
	header[0] = grpcWebDataFlag
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	if err := writeAll(w, payload); err != nil {
		return err
	}
	return nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil || n <= 0 {
			return errors.New("agy RPC request write failed")
		}
		data = data[n:]
	}
	return nil
}

// readGRPCWebUnary consumes the complete grpc-web unary response. It does not
// expose any frame payload outside this package; callers immediately unmarshal
// it through generated protobuf messages.
func readGRPCWebUnary(ctx context.Context, r io.Reader) ([]byte, error) {
	var data []byte
	var consumed int64
	seenData := false
	seenTrailer := false

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var header [5]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) && seenTrailer {
				return data, nil
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errors.New("agy RPC response is truncated or missing grpc trailer")
			}
			return nil, errors.New("agy RPC response read failed")
		}
		flag := header[0]
		if flag != grpcWebDataFlag && flag != grpcWebTrailerFlag {
			return nil, errors.New("agy RPC response has unknown or compressed frame flag")
		}
		length := binary.BigEndian.Uint32(header[1:])
		if length > maxGRPCWebBody {
			return nil, errors.New("agy RPC response frame exceeds limit")
		}
		if flag == grpcWebTrailerFlag && length > maxGRPCWebTrailer {
			return nil, errors.New("agy RPC grpc trailer exceeds limit")
		}
		consumed += int64(len(header)) + int64(length)
		if consumed > maxGRPCWebBody {
			return nil, errors.New("agy RPC response body exceeds limit")
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, errors.New("agy RPC response is truncated")
		}

		switch flag {
		case grpcWebDataFlag:
			if seenTrailer {
				return nil, errors.New("agy RPC response contains data after grpc trailer")
			}
			if seenData {
				return nil, errors.New("agy RPC unary response contains multiple data frames")
			}
			seenData = true
			if len(data)+len(payload) > maxGRPCWebBody {
				return nil, errors.New("agy RPC response body exceeds limit")
			}
			if cap(data)-len(data) < len(payload) {
				next := make([]byte, len(data), len(data)+len(payload))
				copy(next, data)
				data = next
			}
			data = append(data, payload...)
		case grpcWebTrailerFlag:
			if seenTrailer {
				return nil, errors.New("agy RPC response contains duplicate grpc trailer")
			}
			seenTrailer = true
			status, err := parseGRPCWebTrailer(payload)
			if err != nil {
				return nil, err
			}
			if status != 0 {
				return nil, &grpcStatusError{status: status}
			}
			if !seenData {
				return nil, errors.New("agy RPC unary response is missing a data frame")
			}
		}
	}
}

func parseGRPCWebTrailer(payload []byte) (int, error) {
	if len(payload) == 0 || len(payload) > maxGRPCWebTrailer {
		return 0, errors.New("agy RPC grpc trailer is malformed")
	}
	remaining := string(payload)
	var statusText string
	seenStatus := false
	seenMessage := false
	for remaining != "" {
		line, rest, ok := strings.Cut(remaining, "\r\n")
		if !ok || line == "" {
			return 0, errors.New("agy RPC grpc trailer is malformed")
		}
		remaining = rest
		name, value, ok := strings.Cut(line, ":")
		if !ok || name == "" || !isASCIITrailerLine(line) || strings.ContainsAny(name, " \t\r\n") {
			return 0, errors.New("agy RPC grpc trailer is malformed")
		}
		value = strings.TrimSpace(value)
		switch strings.ToLower(name) {
		case "grpc-status":
			if seenStatus || value == "" {
				return 0, errors.New("agy RPC grpc trailer has invalid grpc-status")
			}
			seenStatus = true
			statusText = value
		case "grpc-message":
			if seenMessage {
				return 0, errors.New("agy RPC grpc trailer has duplicate grpc-message")
			}
			seenMessage = true
			if _, err := url.PathUnescape(value); err != nil {
				return 0, errors.New("agy RPC grpc trailer has invalid grpc-message")
			}
		}
	}
	if !seenStatus {
		return 0, errors.New("agy RPC grpc trailer is missing grpc-status")
	}
	status, err := parseGRPCStatus(statusText)
	if err != nil {
		return 0, errors.New("agy RPC grpc trailer has invalid grpc-status")
	}
	return status, nil
}

func isASCIITrailerLine(line string) bool {
	for i := 0; i < len(line); i++ {
		if line[i] < 0x20 || line[i] > 0x7e {
			return false
		}
	}
	return true
}

func parseGRPCStatus(statusText string) (int, error) {
	if statusText == "" {
		return 0, errors.New("empty grpc status")
	}
	for _, character := range statusText {
		if character < '0' || character > '9' {
			return 0, errors.New("non-decimal grpc status")
		}
	}
	status, err := strconv.Atoi(statusText)
	if err != nil || status < 0 || status > 16 {
		return 0, errors.New("invalid grpc status")
	}
	return status, nil
}

func newRPCEndpointClient(endpoint rpcEndpoint) (*rpcClient, error) {
	host, port, err := net.SplitHostPort(endpoint.address)
	if err != nil || !isLoopbackIP(host) || !validTCPPort(port) {
		return nil, errors.New("agy RPC endpoint is not an IP loopback address")
	}
	scheme := "http"
	transport := &http.Transport{
		Proxy:                 nil,
		ForceAttemptHTTP2:     false,
		DisableKeepAlives:     true,
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       time.Second,
		ResponseHeaderTimeout: time.Second,
	}
	if endpoint.tls {
		scheme = "https"
		// This transport is allocated for precisely one parsed loopback IP. Its
		// dialer refuses every other target, so self-signed verification relaxation
		// cannot affect ambient or non-loopback HTTP traffic.
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12} //nolint:gosec // constrained by DialContext below
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		gotHost, gotPort, splitErr := net.SplitHostPort(address)
		if splitErr != nil || gotHost != host || gotPort != port || !isLoopbackIP(gotHost) {
			return nil, errors.New("agy RPC transport refused non-loopback dial")
		}
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, endpoint.address)
	}
	return &rpcClient{
		baseURL:   scheme + "://" + endpoint.address,
		http:      &http.Client{Transport: transport},
		transport: transport,
	}, nil
}

func (c *rpcClient) close() {
	if c != nil && c.transport != nil {
		c.transport.CloseIdleConnections()
	}
}

// call is the only protobuf transport boundary. Requests and responses are
// generated messages, and payload bytes are marshaled/unmarshaled only here.
func (c *rpcClient) call(ctx context.Context, method string, request, response proto.Message) error {
	ctx, cancel := context.WithTimeout(ctx, rpcRequestTimeout)
	defer cancel()
	payload, err := proto.Marshal(request)
	if err != nil {
		return errors.New("agy RPC request marshal failed")
	}
	if len(payload) > maxGRPCWebBody-5 {
		return errors.New("agy RPC request body exceeds limit")
	}
	var body strings.Builder
	body.Grow(len(payload) + 5)
	if err := writeGRPCWebData(&body, payload); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+languageServerService+"/"+method, strings.NewReader(body.String()))
	if err != nil {
		return errors.New("agy RPC request construction failed")
	}
	req.Header.Set("Content-Type", grpcWebContentType)
	req.Header.Set("Accept", grpcWebContentType)
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("agy RPC request failed")
	}
	defer resp.Body.Close()
	if err := validateGRPCWebResponse(resp); err != nil {
		return err
	}
	responsePayload, err := readGRPCWebUnary(ctx, resp.Body)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(responsePayload, response); err != nil {
		return errors.New("agy RPC response unmarshal failed")
	}
	return nil
}

// validateGRPCWebResponse applies the HTTP-level gRPC-Web contract shared by
// unary and server-streaming requests. Frame-level grpc-status is validated by
// their respective readers.
func validateGRPCWebResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		return errors.New("agy RPC returned non-success HTTP status")
	}
	contentType := resp.Header.Get("Content-Type")
	mediaType, _, parseErr := mime.ParseMediaType(contentType)
	if parseErr != nil || !strings.EqualFold(mediaType, grpcWebContentType) {
		return errors.New("agy RPC returned incompatible content type")
	}
	headerStatuses := resp.Header.Values("Grpc-Status")
	if len(headerStatuses) > 1 {
		return errors.New("agy RPC returned duplicate header grpc-status")
	}
	if len(headerStatuses) == 1 {
		status, parseErr := parseGRPCStatus(headerStatuses[0])
		if parseErr != nil {
			return errors.New("agy RPC returned invalid header grpc-status")
		}
		if status != 0 {
			return &grpcStatusError{status: status}
		}
	}
	return nil
}

func (c *rpcClient) heartbeat(ctx context.Context) (*agyv115.HeartbeatResponse, error) {
	response := new(agyv115.HeartbeatResponse)
	if err := c.call(ctx, "Heartbeat", &agyv115.HeartbeatRequest{}, response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *rpcClient) getConversationMetadata(ctx context.Context, conversationID string) (*agyv115.GetConversationMetadataResponse, error) {
	response := new(agyv115.GetConversationMetadataResponse)
	if err := c.call(ctx, "GetConversationMetadata", &agyv115.GetConversationMetadataRequest{ConversationId: conversationID}, response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *rpcClient) getAvailableModels(ctx context.Context, forceRefresh bool) (*agyv115.GetAvailableModelsResponse, error) {
	response := new(agyv115.GetAvailableModelsResponse)
	if err := c.call(ctx, "GetAvailableModels", &agyv115.GetAvailableModelsRequest{ForceRefresh: forceRefresh}, response); err != nil {
		return nil, err
	}
	return response, nil
}

// modelAvailabilityDeadline is the bounded wall-clock window the startup path
// waits for model availability to resolve. The evidence-backed production
// gate showed models populated ~1 second after PTY RPC startup.
var modelAvailabilityDeadline = 3 * time.Second

// modelAvailabilityPollInterval is the spacing between normal (force=false)
// GetAvailableModels probes during the bounded wait.
var modelAvailabilityPollInterval = 100 * time.Millisecond

// waitForAvailableModels blocks until the RPC client's model map is populated
// or the context/deadline expires. It only issues normal (non-forcing) queries
// and never retries mutations such as SendUserCascadeMessage or StartCascade.
// Returns the populated response on success.
func (c *rpcClient) waitForAvailableModels(ctx context.Context) (*agyv115.FetchAvailableModelsResponse, error) {
	waitCtx, cancel := context.WithTimeout(ctx, modelAvailabilityDeadline)
	defer cancel()
	poll := time.NewTicker(modelAvailabilityPollInterval)
	defer poll.Stop()
	for {
		resp, err := c.getAvailableModels(waitCtx, false)
		if err == nil && hasResolvedModel(resp.GetResponse()) {
			return resp.GetResponse(), nil
		}
		select {
		case <-waitCtx.Done():
			if callerErr := ctx.Err(); callerErr != nil {
				return nil, callerErr
			}
			return nil, errors.New("agy RPC model availability timed out")
		case <-poll.C:
		}
	}
}

func hasResolvedModel(response *agyv115.FetchAvailableModelsResponse) bool {
	if response == nil {
		return false
	}
	for _, details := range response.GetModels() {
		if details != nil && details.GetModel() != agyv115.Model_MODEL_UNSPECIFIED {
			return true
		}
	}
	return false
}

func (c *rpcClient) getAllCascadeTrajectories(ctx context.Context, excludeSubtrajectories bool) (*agyv115.GetAllCascadeTrajectoriesResponse, error) {
	response := new(agyv115.GetAllCascadeTrajectoriesResponse)
	if err := c.call(ctx, "GetAllCascadeTrajectories", &agyv115.GetAllCascadeTrajectoriesRequest{ExcludeSubtrajectories: excludeSubtrajectories}, response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *rpcClient) getCascadeTrajectorySteps(ctx context.Context, cascadeID string, offset uint32, verbosity agyv115.SnapshotTrajectoryVerbosity, trajectoryVerbosity agyv115.StreamTrajectoryVerbosity) (*agyv115.GetCascadeTrajectoryStepsResponse, error) {
	response := new(agyv115.GetCascadeTrajectoryStepsResponse)
	request := &agyv115.GetCascadeTrajectoryStepsRequest{CascadeId: cascadeID, StepOffset: offset, Verbosity: verbosity, TrajectoryVerbosity: trajectoryVerbosity}
	if err := c.call(ctx, "GetCascadeTrajectorySteps", request, response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *rpcClient) startCascade(ctx context.Context) (*agyv115.StartCascadeResponse, error) {
	response := new(agyv115.StartCascadeResponse)
	request := &agyv115.StartCascadeRequest{Source: agyv115.CortexTrajectorySource_CORTEX_TRAJECTORY_SOURCE_CLI}
	if err := c.call(ctx, "StartCascade", request, response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *rpcClient) sendUserCascadeMessage(ctx context.Context, cascadeID, text string, model agyv115.Model) (*agyv115.SendUserCascadeMessageResponse, error) {
	response := new(agyv115.SendUserCascadeMessageResponse)
	request := &agyv115.SendUserCascadeMessageRequest{
		CascadeId: cascadeID,
		Items:     []*agyv115.TextOrScopeItem{{Chunk: &agyv115.TextOrScopeItem_Text{Text: text}}},
		CascadeConfig: &agyv115.CascadeConfig{PlannerConfig: &agyv115.CascadePlannerConfig{
			PlanModel: model,
			ToolConfig: &agyv115.CascadeToolConfig{
				AskQuestion:   &agyv115.AskQuestionToolConfig{Enabled: proto.Bool(true)},
				AskPermission: &agyv115.AskPermissionToolConfig{Enabled: proto.Bool(true)},
			},
		}},
	}
	if err := c.call(ctx, "SendUserCascadeMessage", request, response); err != nil {
		return nil, err
	}
	return response, nil
}

// handleCascadeUserInteraction sends one typed interaction mutation. Callers
// own idempotency: mutations are never retried by this RPC layer.
func (c *rpcClient) handleCascadeUserInteraction(ctx context.Context, cascadeID string, interaction *agyv115.CascadeUserInteraction) (*agyv115.HandleCascadeUserInteractionResponse, error) {
	if interaction == nil {
		return nil, errors.New("agy interaction is required")
	}
	response := new(agyv115.HandleCascadeUserInteractionResponse)
	request := &agyv115.HandleCascadeUserInteractionRequest{CascadeId: cascadeID, Interaction: interaction}
	if err := c.call(ctx, "HandleCascadeUserInteraction", request, response); err != nil {
		return nil, err
	}
	return response, nil
}

// handleCascadeUserInteractionDenial remains the Stage 9 fixture seam.
func (c *rpcClient) handleCascadeUserInteractionDenial(ctx context.Context, cascadeID, trajectoryID string, stepIndex uint32) (*agyv115.HandleCascadeUserInteractionResponse, error) {
	return c.handleCascadeUserInteraction(ctx, cascadeID, &agyv115.CascadeUserInteraction{
		TrajectoryId: trajectoryID,
		StepIndex:    stepIndex,
		Interaction:  &agyv115.CascadeUserInteraction_Deploy{Deploy: &agyv115.CascadeDeployInteraction{}},
	})
}

func (c *rpcClient) cancelCascadeSteps(ctx context.Context, cascadeID string, stepIndices []uint32) (*agyv115.CancelCascadeStepsResponse, error) {
	response := new(agyv115.CancelCascadeStepsResponse)
	request := &agyv115.CancelCascadeStepsRequest{CascadeId: cascadeID, StepIndices: append([]uint32(nil), stepIndices...)}
	if err := c.call(ctx, "CancelCascadeSteps", request, response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *rpcClient) forceStopCascadeTree(ctx context.Context, conversationID string) (*agyv115.ForceStopCascadeTreeResponse, error) {
	response := new(agyv115.ForceStopCascadeTreeResponse)
	if err := c.call(ctx, "ForceStopCascadeTree", &agyv115.ForceStopCascadeTreeRequest{ConversationId: conversationID}, response); err != nil {
		return nil, err
	}
	return response, nil
}

var ownedListenerDiscovery = discoverOwnedListeners

// discoverRPCHost identifies the process-owned RPC endpoint but intentionally
// does not bind a conversation. Interactive agy has no Avenor-owned cascade ID
// until StartCascade succeeds; validateSession performs the required metadata
// identity check before the host is registered with a provider session.
func discoverRPCHost(ctx context.Context, pid int, version string, options rpcDiscoveryOptions) (*rpcHost, error) {
	if !supportedRPCVersion(version) {
		return nil, errors.New("agy RPC requires compatible major version 1")
	}
	ctx, cancel := context.WithTimeout(ctx, rpcDiscoveryTimeout)
	defer cancel()
	candidates, err := ownedListenerDiscovery(ctx, pid)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, errors.New("agy RPC listener discovery found no owned loopback listener")
	}
	for _, tlsMode := range []bool{true, false} {
		if !tlsMode && !options.AllowPlainHTTP {
			break
		}
		for _, address := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			client, clientErr := newRPCEndpointClient(rpcEndpoint{address: address, tls: tlsMode})
			if clientErr != nil {
				continue
			}
			if err := heartbeatWithRetry(ctx, client); err != nil {
				client.close()
				continue
			}
			return &rpcHost{client: client, endpoint: rpcEndpoint{address: address, tls: tlsMode}, version: version}, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("agy RPC discovery could not validate an owned listener")
}

// startCascade starts a new interactive cascade through the typed RPC client.
func (h *rpcHost) startCascade(ctx context.Context) (*agyv115.StartCascadeResponse, error) {
	if h == nil || h.client == nil {
		return nil, errors.New("agy RPC cascade start requires a discovered host")
	}
	return h.client.startCascade(ctx)
}

func (h *rpcHost) validateSession(ctx context.Context, expectedSessionID string) error {
	if h == nil || h.client == nil {
		return errors.New("agy RPC session validation requires a discovered host")
	}
	if expectedSessionID == "" {
		return errors.New("agy RPC session validation requires an identity")
	}
	if h.sessionID != "" {
		if h.sessionID == expectedSessionID {
			return nil
		}
		return errors.New("agy RPC host is already bound to another session")
	}
	metadata, err := h.client.getConversationMetadata(ctx, expectedSessionID)
	if err != nil || metadata.GetMetadata() == nil || metadata.GetMetadata().GetRootConversationId() != expectedSessionID {
		return errors.New("agy RPC conversation metadata does not match the expected session")
	}
	h.sessionID = expectedSessionID
	return nil
}

func (h *rpcHost) close() {
	if h != nil {
		h.closeOnce.Do(func() {
			h.client.close()
			if h.closeHook != nil {
				h.closeHook()
			}
		})
	}
}

func heartbeatWithRetry(ctx context.Context, client *rpcClient) error {
	var lastErr error
	for attempt := 0; attempt < heartbeatAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := client.heartbeat(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt+1 < heartbeatAttempts {
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}

func discoverOwnedListeners(ctx context.Context, pid int) ([]string, error) {
	switch runtime.GOOS {
	case "darwin":
		return discoverDarwinListeners(ctx, pid)
	case "linux":
		return discoverLinuxListeners(pid)
	default:
		return nil, errors.New("agy RPC listener discovery is unsupported on this platform")
	}
}

var lsofCommandContext = exec.CommandContext

func discoverDarwinListeners(ctx context.Context, pid int) ([]string, error) {
	command := lsofCommandContext(ctx, "lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-iTCP", "-sTCP:LISTEN", "-Fpcn")
	output, err := command.Output()
	if err != nil {
		return nil, errors.New("agy RPC lsof listener discovery is unavailable")
	}
	return parseDarwinLsofListeners(string(output), pid)
}

func parseDarwinLsofListeners(output string, expectedPID int) ([]string, error) {
	var listeners []string
	currentPID := -1
	for _, line := range strings.Split(output, "\n") {
		if line == "" {
			continue
		}
		switch line[0] {
		case 'p':
			pid, err := strconv.Atoi(line[1:])
			if err != nil {
				return nil, errors.New("agy RPC lsof returned malformed PID")
			}
			currentPID = pid
		case 'n':
			if currentPID != expectedPID {
				return nil, errors.New("agy RPC lsof listener PID mismatch")
			}
			address := strings.TrimSuffix(line[1:], " (LISTEN)")
			host, port, err := net.SplitHostPort(address)
			if err != nil || !validTCPPort(port) || !isLoopbackIP(host) {
				return nil, errors.New("agy RPC lsof returned non-loopback or malformed listener")
			}
			listeners = appendUniqueListener(listeners, net.JoinHostPort(host, port))
		}
	}
	return listeners, nil
}

func discoverLinuxListeners(pid int) ([]string, error) {
	fdPath := filepath.Join("/proc", strconv.Itoa(pid), "fd")
	entries, err := os.ReadDir(fdPath)
	if err != nil {
		return nil, errors.New("agy RPC proc listener discovery is unavailable")
	}
	owned := make(map[string]struct{})
	for _, entry := range entries {
		link, err := os.Readlink(filepath.Join(fdPath, entry.Name()))
		if err != nil {
			continue
		}
		if inode, ok := socketInode(link); ok {
			owned[inode] = struct{}{}
		}
	}
	if len(owned) == 0 {
		return nil, errors.New("agy RPC proc listener discovery found no process sockets")
	}
	tcp, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "net", "tcp"))
	if err != nil {
		return nil, errors.New("agy RPC proc listener discovery is unavailable")
	}
	tcp6, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "net", "tcp6"))
	if err != nil {
		return nil, errors.New("agy RPC proc listener discovery is unavailable")
	}
	return parseLinuxProcListeners(string(tcp), string(tcp6), owned)
}

func socketInode(link string) (string, bool) {
	if !strings.HasPrefix(link, "socket:[") || !strings.HasSuffix(link, "]") {
		return "", false
	}
	inode := strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")
	if inode == "" || strings.ContainsAny(inode, " \t") {
		return "", false
	}
	return inode, true
}

func parseLinuxProcListeners(tcp, tcp6 string, owned map[string]struct{}) ([]string, error) {
	var listeners []string
	for _, table := range []struct {
		body string
		v6   bool
	}{{tcp, false}, {tcp6, true}} {
		lines := strings.Split(strings.TrimSpace(table.body), "\n")
		if len(lines) == 0 || !strings.Contains(lines[0], "local_address") {
			return nil, errors.New("agy RPC proc listener table is malformed")
		}
		for _, line := range lines[1:] {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			if len(fields) < 10 {
				return nil, errors.New("agy RPC proc listener row is malformed")
			}
			if fields[3] != "0A" {
				continue
			}
			if _, ok := owned[fields[9]]; !ok {
				continue
			}
			host, port, err := parseProcAddress(fields[1], table.v6)
			if err != nil || !isLoopbackIP(host) {
				return nil, errors.New("agy RPC proc returned non-loopback or malformed listener")
			}
			listeners = appendUniqueListener(listeners, net.JoinHostPort(host, port))
		}
	}
	return listeners, nil
}

func parseProcAddress(value string, v6 bool) (string, string, error) {
	hostHex, portHex, ok := strings.Cut(value, ":")
	if !ok {
		return "", "", errors.New("bad proc address")
	}
	portNumber, err := strconv.ParseUint(portHex, 16, 16)
	if err != nil || portNumber == 0 {
		return "", "", errors.New("bad proc port")
	}
	wantLength := 8
	if v6 {
		wantLength = 32
	}
	if len(hostHex) != wantLength {
		return "", "", errors.New("bad proc IP")
	}
	raw := make([]byte, wantLength/2)
	for i := range raw {
		byteValue, decodeErr := strconv.ParseUint(hostHex[i*2:i*2+2], 16, 8)
		if decodeErr != nil {
			return "", "", errors.New("bad proc IP")
		}
		raw[i] = byte(byteValue)
	}
	if v6 {
		for offset := 0; offset < len(raw); offset += 4 {
			reverseBytes(raw[offset : offset+4])
		}
	} else {
		reverseBytes(raw)
	}
	return net.IP(raw).String(), strconv.FormatUint(portNumber, 10), nil
}

func reverseBytes(data []byte) {
	for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
		data[i], data[j] = data[j], data[i]
	}
}

func isLoopbackIP(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validTCPPort(port string) bool {
	number, err := strconv.ParseUint(port, 10, 16)
	return err == nil && number != 0
}

func appendUniqueListener(listeners []string, listener string) []string {
	for _, existing := range listeners {
		if existing == listener {
			return listeners
		}
	}
	return append(listeners, listener)
}
