package control

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/events"
)

func TestSocketStartSignalsReadinessFD(t *testing.T) {
	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyReader.Close()
	defer readyWriter.Close()
	t.Setenv("AVENOR_CONTROL_READY_FD", strconv.Itoa(int(readyWriter.Fd())))

	s := NewServer(NewState("run_1", "", 0))
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	read := make(chan error, 1)
	var signal [1]byte
	go func() {
		_, err := readyReader.Read(signal[:])
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("read readiness signal: %v", err)
		}
		if signal[0] != 1 {
			t.Fatalf("readiness signal = %d, want 1", signal[0])
		}
	case <-time.After(time.Second):
		t.Fatal("control server did not signal readiness")
	}
}

func TestSocketStartRemovesStalePath(t *testing.T) {
	path := testSocketPath(t)
	stale, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}

	s := NewServer(NewState("run_1", "", 0))
	if err := s.Start(path); err != nil {
		t.Fatalf("start after stale socket: %v", err)
	}
	defer s.Stop()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("stale path was not replaced by a socket: %v", info.Mode())
	}
}

func TestSocketStartDoesNotUnlinkAmbiguousProbe(t *testing.T) {
	path := testSocketPath(t)
	if err := os.WriteFile(path, []byte("do not remove"), 0o600); err != nil {
		t.Fatal(err)
	}

	origDial := dialControlSocket
	dialControlSocket = func(string, string, time.Duration) (net.Conn, error) {
		return nil, syscall.EAGAIN
	}
	defer func() { dialControlSocket = origDial }()

	s := NewServer(NewState("run_1", "", 0))
	if err := s.Start(path); err == nil {
		t.Fatal("expected ambiguous probe to fail")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("ambiguous probe removed socket path: %v", err)
	}
}

func TestSocketStartIdentityTokenIsOneShot(t *testing.T) {
	path := testSocketPath(t)
	t.Setenv(controlIdentityTokenEnv, "child-token")

	s := NewServer(NewState("run_1", "", 0))
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	if got := os.Getenv(controlIdentityTokenEnv); got != "" {
		t.Fatalf("identity token still in environment: %q", got)
	}
	cl, err := client.Dial(path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.Close()
	if token, err := cl.Identity(); err != nil || token != "child-token" {
		t.Fatalf("identity = %q, %v; want child-token, nil", token, err)
	}
}

func TestSocketStopDoesNotUnlinkReplacement(t *testing.T) {
	path := testSocketPath(t)
	first := NewServer(NewState("run_1", "", 0))
	if err := first.Start(path); err != nil {
		t.Fatalf("start first: %v", err)
	}

	// Model a competing starter that wins after the first listener closes but
	// before the first server reaches its cleanup unlink.
	if err := first.listener.Close(); err != nil {
		t.Fatalf("close first listener: %v", err)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove old socket: %v", err)
	}
	winner, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("start replacement listener: %v", err)
	}
	defer winner.Close()

	first.Stop()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("old server removed replacement socket: %v", err)
	}
}

func TestSocketStartStopCooperateWithoutRemovingWinner(t *testing.T) {
	path := testSocketPath(t)
	first := NewServer(NewState("run_1", "", 0))
	if err := first.Start(path); err != nil {
		t.Fatalf("start first: %v", err)
	}

	origBeforeLock := beforeControlSocketLock
	origBeforeCleanup := beforeControlSocketCleanup
	defer func() {
		beforeControlSocketLock = origBeforeLock
		beforeControlSocketCleanup = origBeforeCleanup
	}()

	// Hold Stop after it owns the socket lock. Start must reach the same lock
	// boundary before cleanup can proceed, making the two operations contend
	// rather than relying on scheduler timing.
	cleanupOwnsLock := make(chan struct{})
	releaseCleanup := make(chan struct{})
	beforeControlSocketCleanup = func() {
		close(cleanupOwnsLock)
		<-releaseCleanup
	}
	startAtLock := make(chan struct{}, 2)
	beforeControlSocketLock = func() {
		startAtLock <- struct{}{}
	}

	stopped := make(chan struct{})
	go func() {
		first.Stop()
		close(stopped)
	}()
	<-cleanupOwnsLock
	<-startAtLock // Stop reached the socket-lock boundary.

	winner := NewServer(NewState("run_2", "", 0))
	started := make(chan error, 1)
	go func() { started <- winner.Start(path) }()
	<-startAtLock // The competing Start is now blocked on Stop's lock.
	close(releaseCleanup)
	if err := <-started; err != nil {
		t.Fatalf("start winner while stopping first: %v", err)
	}
	defer func() {
		beforeControlSocketLock = origBeforeLock
		beforeControlSocketCleanup = origBeforeCleanup
		winner.Stop()
	}()
	<-stopped

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("cooperative Stop cleanup removed winner socket: %v", err)
	}
}

func TestSocketLifecycleActiveListenerFails(t *testing.T) {
	state := NewState("run_1", "", 0)
	s1 := NewServer(state)
	path := testSocketPath(t)
	if err := s1.Start(path); err != nil {
		t.Fatalf("start first: %v", err)
	}
	defer s1.Stop()

	s2 := NewServer(state)
	if err := s2.Start(path); err == nil {
		t.Fatal("expected active listener failure")
	}
}

func TestOwnerRejectionForMutatingMethods(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c1 := mustDial(t, path)
	defer c1.Close()
	c2 := mustDial(t, path)
	defer c2.Close()

	_ = writeReq(t, c1, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"})
	r1 := readResp(t, c1)
	if r1.Error != nil {
		t.Fatalf("owner cancel failed: %+v", r1.Error)
	}
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 2, Method: "cancel"})
	r2 := readResp(t, c2)
	if r2.Error == nil || r2.Error.Code != -32010 {
		t.Fatalf("expected permission denied for cancel, got %+v", r2)
	}

	apParams, _ := json.Marshal(PermissionAnswer{RequestID: "req_1", OptionID: "allow"})
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 3, Method: "answer_permission", Params: apParams})
	r3 := readResp(t, c2)
	if r3.Error == nil || r3.Error.Code != -32010 {
		t.Fatalf("expected permission denied for answer_permission, got %+v", r3)
	}
}

func TestJSONAnswerPermissionAlreadyResolvedIsAccepted(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()
	if !s.PreparePermissionClaim("", "req_done", PermissionResolverAutomatic, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	if !s.MarkPermissionClaimResolved("", "req_done", "avenor") {
		t.Fatal("MarkPermissionClaimResolved returned false")
	}

	conn := mustDial(t, path)
	defer conn.Close()
	params, _ := json.Marshal(PermissionAnswer{RequestID: "req_done", OptionID: "stale"})
	_ = writeReq(t, conn, Request{JSONRPC: "2.0", ID: 1, Method: "answer_permission", Params: params})
	response := readResp(t, conn)
	if response.Error != nil {
		t.Fatalf("resolved answer returned error: %+v", response.Error)
	}
	result, ok := response.Result.(map[string]any)
	if !ok || result["accepted"] != true {
		t.Fatalf("resolved answer result = %#v, want accepted=true", response.Result)
	}
}

func TestPermissionResolverStateString(t *testing.T) {
	tests := []struct {
		state PermissionResolverState
		want  string
	}{
		{PermissionResolverUnknown, "unknown"},
		{PermissionResolverReserved, "reserved"},
		{PermissionResolverControl, "control"},
		{PermissionResolverAutomatic, "automatic"},
		{PermissionResolverFile, "file"},
		{PermissionResolverNoResolver, "none"},
		{PermissionResolverDirectDelivery, "direct_delivery"},
		{PermissionResolverResolved, "resolved"},
		{PermissionResolverState(255), "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Fatalf("PermissionResolverState(%d).String() = %q, want %q", tt.state, got, tt.want)
			}
		})
	}
}

func TestMarkPermissionClaimResolvedRetainsTombstone(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "req_1", PermissionResolverAutomatic, []any{
		map[string]any{"optionId": "allow", "kind": "allow", "requiresMessage": true},
	}) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	if !s.MarkPermissionClaimResolved("rt_1", "req_1", "avenor") {
		t.Fatal("MarkPermissionClaimResolved returned false")
	}
	if got := s.PermissionResolverState("rt_1", "req_1"); got != PermissionResolverResolved {
		t.Fatalf("resolver state = %v, want resolved", got)
	}
	s.pendingMu.Lock()
	source := s.pendingClaims[permissionClaimKey{scope: "rt_1", requestID: "req_1"}].resolutionSource
	s.pendingMu.Unlock()
	if source != "avenor" {
		t.Fatalf("resolution source = %q, want avenor", source)
	}
	if got := s.DeliverPendingPermission("rt_1", "req_1", "missing", ""); got != PermissionAnswerAlreadyResolved {
		t.Fatalf("late delivery = %v, want already-resolved", got)
	}
	if !s.AnswerPendingPermission("rt_1", "req_1", "missing", "") {
		t.Fatal("AnswerPendingPermission rejected an already-resolved claim")
	}
	if got := s.DeliverPendingPermission("rt_1", "missing", "allow", ""); got != PermissionAnswerNotFound {
		t.Fatalf("unknown delivery = %v, want not-found", got)
	}
}

func TestMarkPermissionClaimResolvedDoubleClose(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "req_1", PermissionResolverAutomatic, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}

	s.pendingMu.Lock()
	close(s.pendingClaims[permissionClaimKey{scope: "rt_1", requestID: "req_1"}].disconnectCh)
	s.pendingMu.Unlock()

	if !s.MarkPermissionClaimResolved("rt_1", "req_1", "disconnect") {
		t.Fatal("MarkPermissionClaimResolved returned false")
	}
}

func TestMarkPermissionClaimResolvedConcurrent(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "req_1", PermissionResolverAutomatic, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}

	s.pendingMu.Lock()
	close(s.pendingClaims[permissionClaimKey{scope: "rt_1", requestID: "req_1"}].disconnectCh)
	s.pendingMu.Unlock()

	const callers = 8
	results := make(chan bool, callers)
	for range callers {
		go func() {
			results <- s.MarkPermissionClaimResolved("rt_1", "req_1", "concurrent")
		}()
	}
	for range callers {
		if !<-results {
			t.Fatal("MarkPermissionClaimResolved returned false")
		}
	}
	key := permissionClaimKey{scope: "rt_1", requestID: "req_1"}
	s.pendingMu.Lock()
	claim := s.pendingClaims[key]
	s.pendingMu.Unlock()
	if claim.state != PermissionResolverResolved {
		t.Fatalf("claim state = %v, want Resolved", claim.state)
	}
	if claim.answerCh != nil {
		t.Fatal("claim.answerCh should be nil after resolution")
	}
	if claim.disconnectCh != nil {
		t.Fatal("claim.disconnectCh should be nil after resolution")
	}
	if claim.resolutionSource != "concurrent" {
		t.Fatalf("claim.resolutionSource = %q, want concurrent", claim.resolutionSource)
	}
}

func TestPreparePermissionClaimReplacesResolvedTombstone(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "req_1", PermissionResolverAutomatic, nil) {
		t.Fatal("initial PreparePermissionClaim returned false")
	}
	if !s.MarkPermissionClaimResolved("rt_1", "req_1", "avenor") {
		t.Fatal("MarkPermissionClaimResolved returned false")
	}
	if !s.PreparePermissionClaim("rt_1", "req_1", PermissionResolverFile, nil) {
		t.Fatal("resolved tombstone was not replaced")
	}
	if got := s.PermissionResolverState("rt_1", "req_1"); got != PermissionResolverFile {
		t.Fatalf("replacement state = %v, want file", got)
	}
	if got := s.DeliverPendingPermission("rt_1", "req_1", "allow", ""); got != PermissionAnswerResolverOwned {
		t.Fatalf("delivery to replacement = %v, want resolver-owned", got)
	}
}

func TestPreparePermissionClaimWaitsForDirectCompletionBeforeReusedID(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "reused", PermissionResolverNoResolver, nil) {
		t.Fatal("initial PreparePermissionClaim returned false")
	}
	if got := s.DeliverPendingPermission("rt_1", "reused", "old", ""); got != PermissionAnswerNoResolver {
		t.Fatalf("direct delivery = %v, want no-resolver", got)
	}

	origHook := beforeDirectPermissionClaimWait
	defer func() { beforeDirectPermissionClaimWait = origHook }()
	waiting := make(chan struct{})
	releaseWait := make(chan struct{})
	beforeDirectPermissionClaimWait = func() {
		close(waiting)
		<-releaseWait
	}

	prepared := make(chan bool, 1)
	go func() {
		prepared <- s.PreparePermissionClaimAfterDirectDelivery(context.Background(), "rt_1", "reused", PermissionResolverFile, nil)
	}()
	<-waiting
	if !s.MarkPermissionClaimResolved("rt_1", "reused", "direct") {
		t.Fatal("direct provider completion did not resolve claim")
	}
	close(releaseWait)
	if !<-prepared {
		t.Fatal("reused request was rejected after direct completion")
	}
	if got := s.PermissionResolverState("rt_1", "reused"); got != PermissionResolverFile {
		t.Fatalf("replacement state = %v, want file", got)
	}
}

func TestReservedClaimCreatedAfterCompletedDisconnectScanIsSignalled(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if err := s.Start(testSocketPath(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()
	conn, err := net.Dial("unix", s.path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !s.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("control client was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	scanned := make(chan struct{})
	release := make(chan struct{})
	s.mu.Lock()
	s.afterClientDisconnectScan = func() {
		close(scanned)
		<-release
	}
	s.mu.Unlock()
	if err := conn.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	select {
	case <-scanned:
	case <-time.After(time.Second):
		t.Fatal("last-client disconnect did not complete its claim scan")
	}
	// The disconnect scan is complete and found no claims. Let it release the
	// connection lock, then create the stale Reserved decision from an earlier
	// HasClients observation. Claim creation itself must inherit disconnection.
	close(release)
	if !s.PreparePermissionClaim("rt_1", "req_gap", PermissionResolverReserved, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	_, disconnectCh, ok := s.BeginPermissionClaim("rt_1", "req_gap")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	select {
	case <-disconnectCh:
	default:
		t.Fatal("Reserved claim created after disconnect scan was not signalled")
	}
}

func TestPermissionClaimsCreatedWithoutClientsSignalDisconnect(t *testing.T) {
	t.Run("direct begin", func(t *testing.T) {
		s := NewServer(NewState("run_1", "", 0))
		_, disconnectCh, ok := s.BeginPermissionClaim("rt_1", "direct")
		if !ok {
			t.Fatal("BeginPermissionClaim returned false")
		}
		select {
		case <-disconnectCh:
		default:
			t.Fatal("unprepared claim did not inherit the no-client disconnect state")
		}
	})

	t.Run("replacement after direct delivery", func(t *testing.T) {
		s := NewServer(NewState("run_1", "", 0))
		if !s.PreparePermissionClaim("rt_1", "reused", PermissionResolverNoResolver, nil) {
			t.Fatal("initial PreparePermissionClaim returned false")
		}
		if got := s.DeliverPendingPermission("rt_1", "reused", "old", ""); got != PermissionAnswerNoResolver {
			t.Fatalf("direct delivery = %v, want no-resolver", got)
		}

		origHook := beforeDirectPermissionClaimWait
		defer func() { beforeDirectPermissionClaimWait = origHook }()
		waiting := make(chan struct{})
		beforeDirectPermissionClaimWait = func() { close(waiting) }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		prepared := make(chan bool, 1)
		go func() {
			prepared <- s.PreparePermissionClaimAfterDirectDelivery(ctx, "rt_1", "reused", PermissionResolverReserved, nil)
		}()
		select {
		case <-waiting:
		case <-time.After(time.Second):
			t.Fatal("replacement did not begin waiting for direct completion")
		}
		if !s.MarkPermissionClaimResolved("rt_1", "reused", "direct") {
			t.Fatal("direct provider completion did not resolve claim")
		}
		select {
		case ok := <-prepared:
			if !ok {
				t.Fatal("replacement claim was not prepared")
			}
		case <-time.After(time.Second):
			t.Fatal("replacement claim did not finish after direct completion")
		}
		_, disconnectCh, ok := s.BeginPermissionClaim("rt_1", "reused")
		if !ok {
			t.Fatal("BeginPermissionClaim returned false for replacement")
		}
		select {
		case <-disconnectCh:
		default:
			t.Fatal("replacement claim did not inherit the no-client disconnect state")
		}
	})
}

func TestPreparePermissionClaimAfterDirectDeliveryCancellation(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "reused", PermissionResolverNoResolver, nil) {
		t.Fatal("initial PreparePermissionClaim returned false")
	}
	if got := s.DeliverPendingPermission("rt_1", "reused", "old", ""); got != PermissionAnswerNoResolver {
		t.Fatalf("direct delivery = %v, want no-resolver", got)
	}

	origHook := beforeDirectPermissionClaimWait
	defer func() { beforeDirectPermissionClaimWait = origHook }()
	waiting := make(chan struct{})
	beforeDirectPermissionClaimWait = func() { close(waiting) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prepared := make(chan bool, 1)
	go func() {
		prepared <- s.PreparePermissionClaimAfterDirectDelivery(ctx, "rt_1", "reused", PermissionResolverFile, nil)
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("replacement did not begin waiting for direct completion")
	}
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context did not cancel while waiting for direct completion")
	}
	select {
	case got := <-prepared:
		if got {
			t.Fatal("replacement succeeded after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("replacement did not return after cancellation")
	}
	if got := s.PermissionResolverState("rt_1", "reused"); got != PermissionResolverDirectDelivery {
		t.Fatalf("resolver state after cancellation = %v, want direct-delivery", got)
	}
	if !s.MarkPermissionClaimResolved("rt_1", "reused", "direct") {
		t.Fatal("direct provider completion did not resolve claim")
	}
}

func TestPreparePermissionClaimAfterDirectDeliveryAbandoned(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "reused", PermissionResolverNoResolver, nil) {
		t.Fatal("initial PreparePermissionClaim returned false")
	}
	if got := s.DeliverPendingPermission("rt_1", "reused", "old", ""); got != PermissionAnswerNoResolver {
		t.Fatalf("direct delivery = %v, want no-resolver", got)
	}

	origHook := beforeDirectPermissionClaimWait
	defer func() { beforeDirectPermissionClaimWait = origHook }()
	waiting := make(chan struct{})
	beforeDirectPermissionClaimWait = func() { close(waiting) }

	prepared := make(chan bool, 1)
	go func() {
		prepared <- s.PreparePermissionClaimAfterDirectDelivery(context.Background(), "rt_1", "reused", PermissionResolverFile, nil)
	}()
	<-waiting
	s.EndPermissionClaim("rt_1", "reused")
	select {
	case got := <-prepared:
		if got {
			t.Fatal("replacement succeeded after abandon")
		}
	case <-time.After(time.Second):
		t.Fatal("replacement did not return after abandon")
	}
	if got := s.PermissionResolverState("rt_1", "reused"); got != PermissionResolverUnknown {
		t.Fatalf("resolver state after abandon = %v, want Unknown", got)
	}
}

func TestAnswerPendingPermissionDeliversMatchingAnswer(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	answerCh, _, ok := s.BeginPermissionClaim("", "req_1")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	defer s.EndPermissionClaim("", "req_1")

	if !s.AnswerPendingPermission("", "req_1", "allow", "") {
		t.Fatal("AnswerPendingPermission returned false for matching request")
	}
	select {
	case answer := <-answerCh:
		if answer.RequestID != "req_1" || answer.OptionID != "allow" {
			t.Fatalf("answer = %+v", answer)
		}
	default:
		t.Fatal("matching answer was not delivered")
	}
}

func TestAnswerPendingPermissionRejectsMismatchedRequest(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	answerCh, _, ok := s.BeginPermissionClaim("", "req_1")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	defer s.EndPermissionClaim("", "req_1")

	if s.AnswerPendingPermission("", "req_other", "allow", "") {
		t.Fatal("AnswerPendingPermission returned true for mismatched request")
	}
	select {
	case answer := <-answerCh:
		t.Fatalf("mismatched answer was delivered: %+v", answer)
	default:
	}
}

func TestPermissionClaimsAreScopedAndReportFullChannel(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	first, _, ok := s.BeginPermissionClaim("rt_1", "0")
	if !ok {
		t.Fatal("first BeginPermissionClaim returned false")
	}
	second, _, ok := s.BeginPermissionClaim("rt_2", "0")
	if !ok {
		t.Fatal("second BeginPermissionClaim returned false for a distinct scope")
	}
	defer s.EndPermissionClaim("rt_1", "0")
	defer s.EndPermissionClaim("rt_2", "0")

	if !s.AnswerPendingPermission("rt_1", "0", "allow_1", "") {
		t.Fatal("answer for rt_1 was not delivered")
	}
	if s.AnswerPendingPermission("rt_1", "0", "duplicate", "") {
		t.Fatal("answer reported delivery to a full channel")
	}
	if !s.AnswerPendingPermission("rt_2", "0", "allow_2", "") {
		t.Fatal("answer for rt_2 was not delivered")
	}

	if got := <-first; got.OptionID != "allow_1" {
		t.Fatalf("rt_1 answer = %+v", got)
	}
	if got := <-second; got.OptionID != "allow_2" {
		t.Fatalf("rt_2 answer = %+v", got)
	}
}

func TestPermissionClaimRejectsDuplicateAfterFirstAnswerIsDrained(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	answerCh, _, ok := s.BeginPermissionClaim("rt_1", "0")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	defer s.EndPermissionClaim("rt_1", "0")

	if got := s.DeliverPendingPermission("rt_1", "0", "allow", ""); got != PermissionAnswerDelivered {
		t.Fatalf("first delivery = %v, want delivered", got)
	}
	if answer := <-answerCh; answer.OptionID != "allow" {
		t.Fatalf("first answer = %+v", answer)
	}

	// The resolver has drained the channel but has not yet applied the answer
	// or called EndPermissionClaim. A duplicate must remain non-deliverable.
	if got := s.DeliverPendingPermission("rt_1", "0", "duplicate", ""); got != PermissionAnswerChannelFull {
		t.Fatalf("duplicate delivery = %v, want channel-full", got)
	}
	select {
	case answer := <-answerCh:
		t.Fatalf("duplicate answer was delivered: %+v", answer)
	default:
	}
}

func TestBeginPermissionClaimPreservesAnswerQueuedWhileReserved(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "0", PermissionResolverReserved, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	if got := s.DeliverPendingPermission("rt_1", "0", "allow", ""); got != PermissionAnswerDelivered {
		t.Fatalf("delivery = %v, want delivered", got)
	}

	answerCh, _, ok := s.BeginPermissionClaim("rt_1", "0")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	defer s.EndPermissionClaim("rt_1", "0")
	if answer := <-answerCh; answer.OptionID != "allow" {
		t.Fatalf("queued answer = %+v", answer)
	}
	if got := s.DeliverPendingPermission("rt_1", "0", "duplicate", ""); got != PermissionAnswerChannelFull {
		t.Fatalf("duplicate delivery = %v, want channel-full", got)
	}
}

func TestHandoffPermissionClaimAtomicallyConsumesQueuedAnswerOrTransfersToFile(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "0", PermissionResolverReserved, nil) {
		t.Fatal("prepare queued-answer claim failed")
	}
	if !s.AnswerPendingPermission("rt_1", "0", "allow", "") {
		t.Fatal("queue answer failed")
	}
	answer, answered := s.HandoffPermissionClaim("rt_1", "0", PermissionResolverFile)
	if !answered || answer.OptionID != "allow" {
		t.Fatalf("handoff answer = %+v, %v", answer, answered)
	}
	s.EndPermissionClaim("rt_1", "0")

	if !s.PreparePermissionClaim("rt_1", "1", PermissionResolverReserved, nil) {
		t.Fatal("prepare file-handoff claim failed")
	}
	if answer, answered := s.HandoffPermissionClaim("rt_1", "1", PermissionResolverFile); answered {
		t.Fatalf("unexpected handoff answer: %+v", answer)
	}
	if got := s.PermissionResolverState("rt_1", "1"); got != PermissionResolverFile {
		t.Fatalf("resolver state = %v, want file", got)
	}
	if got := s.DeliverPendingPermission("rt_1", "1", "late", ""); got != PermissionAnswerResolverOwned {
		t.Fatalf("late delivery = %v, want resolver-owned", got)
	}
}

func TestHandoffPermissionClaimAtomicallyClosesCancelledClaim(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "0", PermissionResolverReserved, nil) {
		t.Fatal("prepare claim failed")
	}
	if answer, answered := s.HandoffPermissionClaim("rt_1", "0", PermissionResolverResolved); answered {
		t.Fatalf("unexpected cancellation answer: %+v", answer)
	}
	if got := s.PermissionResolverState("rt_1", "0"); got != PermissionResolverUnknown {
		t.Fatalf("resolver state = %v, want removed", got)
	}
	if got := s.DeliverPendingPermission("rt_1", "0", "late", ""); got != PermissionAnswerNotFound {
		t.Fatalf("late cancellation delivery = %v, want not-found", got)
	}
}

func TestPreparePermissionClaimRejectsExistingNoResolver(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	if !s.PreparePermissionClaim("rt_1", "0", PermissionResolverNoResolver, nil) {
		t.Fatal("initial PreparePermissionClaim returned false")
	}
	if s.PreparePermissionClaim("rt_1", "0", PermissionResolverAutomatic, nil) {
		t.Fatal("second PreparePermissionClaim replaced live no-resolver claim")
	}
	if state := s.PermissionResolverState("rt_1", "0"); state != PermissionResolverNoResolver {
		t.Fatalf("resolver state = %v, want no-resolver", state)
	}
}

func TestClearPermissionClaimsRemovesOnlyRequestedScope(t *testing.T) {
	s := NewServer(NewState("run_1", "", 0))
	s.PreparePermissionClaim("rt_1", "0", PermissionResolverFile, nil)
	s.MarkPermissionClaimResolved("rt_1", "0", "file")
	s.PreparePermissionClaim("rt_2", "0", PermissionResolverFile, nil)
	s.ClearPermissionClaims("rt_1")
	if got := s.PermissionResolverState("rt_1", "0"); got != PermissionResolverUnknown {
		t.Fatalf("rt_1 state = %v, want removed", got)
	}
	if got := s.PermissionResolverState("rt_2", "0"); got != PermissionResolverFile {
		t.Fatalf("rt_2 state = %v, want file", got)
	}
}

func TestOwnershipTransferOnDisconnect(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c1 := mustDial(t, path)
	_ = writeReq(t, c1, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"})
	r1 := readResp(t, c1)
	if r1.Error != nil {
		t.Fatalf("owner cancel failed: %+v", r1.Error)
	}
	c1.Close()

	// Poll until owner is cleared (server processes disconnect).
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		owner := s.owner
		s.mu.Unlock()
		if owner == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owner not cleared after close")
		}
		time.Sleep(5 * time.Millisecond)
	}

	c2 := mustDial(t, path)
	defer c2.Close()
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 2, Method: "cancel"})
	r2 := readResp(t, c2)
	if r2.Error != nil {
		t.Fatalf("new owner cancel failed: %+v", r2.Error)
	}
	res, ok := r2.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r2.Result)
	}
	if v, _ := res["ok"].(bool); !v {
		t.Fatal("expected ok=true")
	}
}

func TestSubscriberBackpressureLaggedEvent(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	client, srv := net.Pipe()
	defer client.Close()
	defer srv.Close()
	cs := &connState{server: s, conn: srv}
	sub := &subscriber{conn: cs, ch: make(chan events.Event, 1)}
	go sub.loop()

	for i := 0; i < 5; i++ {
		sub.enqueue(events.Event{Event: "e", Fields: map[string]any{"i": i}})
	}
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(client)
	var (
		foundLag     bool
		eventCount   int
		droppedCount float64
	)
	for scanner.Scan() {
		var n Notification
		if err := json.Unmarshal(scanner.Bytes(), &n); err != nil {
			continue
		}
		if n.Method == "event" {
			m, _ := n.Params.(map[string]any)
			ev, _ := m["event"].(string)
			if ev == "subscriber.lagged" {
				foundLag = true
				droppedCount, _ = m["dropped_count"].(float64)
			} else {
				eventCount++
			}
		}
	}
	if !foundLag {
		t.Fatal("expected subscriber.lagged notification")
	}
	if droppedCount <= 0 {
		t.Fatalf("expected dropped_count > 0, got %v", droppedCount)
	}
	if eventCount == 0 {
		t.Fatal("expected at least one event notification")
	}
}

func TestNormalizeHistoryLimit(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		want  int
	}{
		{name: "negative uses default", limit: -1, want: historyDefaultLimit},
		{name: "zero uses default", limit: 0, want: historyDefaultLimit},
		{name: "one is preserved", limit: 1, want: 1},
		{name: "maximum is preserved", limit: historyMaxLimit, want: historyMaxLimit},
		{name: "above maximum is capped", limit: historyMaxLimit + 1, want: historyMaxLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeHistoryLimit(tt.limit); got != tt.want {
				t.Fatalf("normalizeHistoryLimit(%d) = %d, want %d", tt.limit, got, tt.want)
			}
		})
	}
}

func TestResultReturnsFullTerminalOutputWhileStatusIsBounded(t *testing.T) {
	state := NewState("run_1", "demo", 0)
	s := NewServer(state)
	full := strings.Repeat("é", events.MaxFinalOutputRunes+10)
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{"runtime_id": "rt_1", "final_output": full}})

	snapshot := state.Snapshot()
	if count := len([]rune(snapshot.FinalOutput)); count != events.MaxFinalOutputRunes {
		t.Fatalf("status final_output rune count = %d, want %d", count, events.MaxFinalOutputRunes)
	}
	if !snapshot.FinalOutputTruncated {
		t.Fatal("bounded status preview did not expose truncation metadata")
	}
	history, _ := s.runtimeHistory("rt_1", 0, 1)
	if len(history) != 1 || history[0].Fields["final_output_truncated"] != true {
		t.Fatalf("bounded history did not expose truncation metadata: %#v", history)
	}
	response := s.dispatch(nil, Request{JSONRPC: "2.0", ID: 1, Method: "result"})
	result := response.Result.(map[string]any)
	if result["final_output"] != full {
		t.Fatal("result did not preserve the complete terminal output")
	}
}

func TestResultOverControlSocketReadsReplyBeyondScannerLimit(t *testing.T) {
	state := NewState("run_1", "demo", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	complete := strings.Repeat("é", 40_000) // 80 KiB UTF-8, beyond Scanner's 64 KiB default.
	s.PublishEvent(events.Event{Event: "session.end", Fields: map[string]any{"final_output": complete}})

	c, err := client.Dial(path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	result, err := c.Result("")
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got, _ := result["final_output"].(string); got != complete {
		t.Fatalf("final_output byte length = %d, want %d", len(got), len(complete))
	}
}

func TestPublishEventCanonicalizesIdentityAndPreservesProvidedMetadata(t *testing.T) {
	state := NewState("run_1", "label", 0)
	s := NewServer(state)

	first := s.PublishEvent(events.Event{Event: "agent.message_chunk", SessionID: "ses_1", Fields: map[string]any{"runtime_id": "rt_1"}})
	if got, _ := first.Fields["run_id"].(string); got != "run_1" {
		t.Fatalf("run_id = %q, want run_1", got)
	}
	if got, _ := first.Fields["run_label"].(string); got != "label" {
		t.Fatalf("run_label = %q, want label", got)
	}
	if got, _ := int64Field(first.Fields["seq"]); got != 1 {
		t.Fatalf("seq = %d, want 1", got)
	}
	if _, ok := int64Field(first.Fields["ts"]); !ok {
		t.Fatal("expected ts to be stamped")
	}

	second := s.PublishEvent(events.Event{Event: "tool.call", SessionID: "ses_1", Fields: map[string]any{"runtime_id": "rt_1"}})
	if got, _ := int64Field(second.Fields["seq"]); got != 2 {
		t.Fatalf("seq = %d, want 2", got)
	}

	preserved := s.PublishEvent(events.Event{Event: "session.end", SessionID: "ses_1", Fields: map[string]any{"runtime_id": "rt_1", "seq": int64(99), "ts": int64(123), "final_output": "done"}})
	if got, _ := int64Field(preserved.Fields["seq"]); got != 99 {
		t.Fatalf("seq = %d, want preserved 99", got)
	}
	if got, _ := int64Field(preserved.Fields["ts"]); got != 123 {
		t.Fatalf("ts = %d, want preserved 123", got)
	}

	third := s.PublishEvent(events.Event{Event: "agent.message_chunk", SessionID: "ses_1", Fields: map[string]any{"runtime_id": "rt_1"}})
	if got, _ := int64Field(third.Fields["seq"]); got != 100 {
		t.Fatalf("seq = %d, want 100", got)
	}

	snap := state.Snapshot()
	if snap.LatestSeq != 100 {
		t.Fatalf("latest_seq = %d, want 100", snap.LatestSeq)
	}
	if snap.FinalOutput != "done" {
		t.Fatalf("final_output = %q, want done", snap.FinalOutput)
	}
}

func TestPublishCanonicalEventKeepsHistoryAndNextSeqAligned(t *testing.T) {
	state := NewState("run_1", "label", 0)
	s := NewServer(state)

	s.PublishCanonicalEvent(events.Event{
		Event:     "agent.message_chunk",
		SessionID: "ses_1",
		Fields: map[string]any{
			"runtime_id": "rt_1",
			"run_id":     "run_1",
			"run_label":  "label",
			"seq":        int64(7),
			"ts":         int64(11),
		},
	})

	history, latestSeq := s.runtimeHistory("rt_1", 0, 8)
	if latestSeq != 7 {
		t.Fatalf("latestSeq = %d, want 7", latestSeq)
	}
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	if seq, _ := int64Field(history[0].Fields["seq"]); seq != 7 {
		t.Fatalf("history seq = %d, want 7", seq)
	}

	next := s.PublishEvent(events.Event{Event: "tool.call", SessionID: "ses_1", Fields: map[string]any{"runtime_id": "rt_1"}})
	if seq, _ := int64Field(next.Fields["seq"]); seq != 8 {
		t.Fatalf("next seq = %d, want 8", seq)
	}
}

func TestSubscribeHistoryReplayFiltersByRuntimeAndAfterSeq(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	s.PublishEvent(events.Event{Event: "agent.message_chunk", SessionID: "ses_1", Fields: map[string]any{"runtime_id": "rt_1"}})
	s.PublishEvent(events.Event{Event: "tool.call", SessionID: "ses_1", Fields: map[string]any{"runtime_id": "rt_1", "title": "build"}})
	s.PublishEvent(events.Event{Event: "agent.message_chunk", SessionID: "ses_2", Fields: map[string]any{"runtime_id": "rt_2"}})

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1", "replay": true, "after_seq": 1, "limit": 8})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "subscribe", Params: params})
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		subs := len(s.subs)
		s.mu.Unlock()
		if subs == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber was not registered")
		}
		time.Sleep(time.Millisecond)
	}
	s.PublishEvent(events.Event{Event: "session.end", SessionID: "ses_1", Fields: map[string]any{"runtime_id": "rt_1", "stop_reason": "end_turn"}})

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(c)
	var seqs []int64
	seenResponse := false
	for len(seqs) < 2 || !seenResponse {
		if !scanner.Scan() {
			t.Fatal("expected response and notifications")
		}
		var envelope map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
		if _, ok := envelope["id"]; ok {
			if errVal, ok := envelope["error"].(map[string]any); ok && errVal != nil {
				t.Fatalf("subscribe error: %+v", errVal)
			}
			seenResponse = true
			continue
		}
		params, _ := envelope["params"].(map[string]any)
		if params["runtime_id"] != "rt_1" {
			t.Fatalf("runtime_id = %v, want rt_1", params["runtime_id"])
		}
		seq, _ := int64Field(params["seq"])
		seqs = append(seqs, seq)
	}
	if seqs[0] != 2 || seqs[1] != 3 {
		t.Fatalf("replay/live seqs = %v, want [2 3]", seqs)
	}

	historyParams, _ := json.Marshal(map[string]any{"runtime_id": "rt_1", "after_seq": 1, "limit": 1})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 2, Method: "history", Params: historyParams})
	historyResp := readResp(t, c)
	if historyResp.Error != nil {
		t.Fatalf("history error: %+v", historyResp.Error)
	}
	result, ok := historyResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("history result type: %T", historyResp.Result)
	}
	evs, ok := result["events"].([]any)
	if !ok || len(evs) != 1 {
		t.Fatalf("history events = %#v, want 1 event", result["events"])
	}
	last, _ := evs[0].(map[string]any)
	if seq, _ := int64Field(last["seq"]); seq != 3 {
		t.Fatalf("history seq = %d, want 3", seq)
	}
}

func TestSubscriberBuffersLiveSequenceUntilReplayIsQueued(t *testing.T) {
	sub := newSubscriber(&connState{}, "rt_1", 0)
	replay := events.Event{Event: "agent.message_chunk", Fields: map[string]any{"runtime_id": "rt_1", "seq": int64(1)}}
	live := events.Event{Event: "tool.call", Fields: map[string]any{"runtime_id": "rt_1", "seq": int64(2)}}

	// A live event can arrive after registration but before the replay goroutine
	// starts. It must not advance seenSeq until the older replay is queued.
	sub.enqueue(live)
	if len(sub.pending) != 1 {
		t.Fatalf("pending len = %d, want 1", len(sub.pending))
	}
	if got := sub.seenSeq["rt_1"]; got != 0 {
		t.Fatalf("seen seq before replay = %d, want 0", got)
	}

	sub.mu.Lock()
	if !sub.recordSeqLocked(replay) {
		sub.mu.Unlock()
		t.Fatal("replay event was incorrectly treated as a duplicate")
	}
	sub.enqueueLocked(replay)
	sub.mu.Unlock()
	sub.flushPending()

	first := <-sub.ch
	second := <-sub.ch
	if seq, _ := int64Field(first.Fields["seq"]); seq != 1 {
		t.Fatalf("first seq = %d, want replay seq 1", seq)
	}
	if seq, _ := int64Field(second.Fields["seq"]); seq != 2 {
		t.Fatalf("second seq = %d, want live seq 2", seq)
	}
}

func TestSubscriberDisconnectCleansUp(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "subscribe", Params: params})
	_ = readResp(t, c)

	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		subs := len(s.subs)
		s.mu.Unlock()
		if subs == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber was not registered")
		}
		time.Sleep(time.Millisecond)
	}
	_ = c.Close()
	for {
		s.mu.Lock()
		subs := len(s.subs)
		s.mu.Unlock()
		if subs == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber did not clean up after disconnect")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSubscribeAndStatus(t *testing.T) {
	state := NewState("run_1", "label", 2)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "subscribe"})
	r1 := readResp(t, c)
	if r1.Error != nil {
		t.Fatalf("subscribe error: %+v", r1.Error)
	}
	res1, ok := r1.Result.(map[string]any)
	if !ok {
		t.Fatalf("subscribe result type: %T", r1.Result)
	}
	if subscribed, _ := res1["subscribed"].(bool); !subscribed {
		t.Fatal("expected subscribed=true")
	}
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 2, Method: "status"})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("status error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("status result type: %T", r.Result)
	}
	if runID, _ := res["run_id"].(string); runID != "run_1" {
		t.Fatalf("run_id = %q, want run_1", runID)
	}
	if runLabel, _ := res["run_label"].(string); runLabel != "label" {
		t.Fatalf("run_label = %q, want label", runLabel)
	}
	mr, _ := res["max_retries"].(float64)
	if int(mr) != 2 {
		t.Fatalf("max_retries = %v, want 2", mr)
	}
}

func mustDial(t *testing.T, path string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := net.Dial("unix", path)
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeReq(t *testing.T, c net.Conn, req Request) error {
	t.Helper()
	b, _ := json.Marshal(req)
	b = append(b, '\n')
	_, err := c.Write(b)
	return err
}

func readResp(t *testing.T, c net.Conn) Response {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(c)
	if !scanner.Scan() {
		t.Fatal("no response")
	}
	var r Response
	if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return r
}

func readNotification(t *testing.T, c net.Conn) Notification {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(c)
	if !scanner.Scan() {
		t.Fatal("no notification")
	}
	var n Notification
	if err := json.Unmarshal(scanner.Bytes(), &n); err != nil {
		t.Fatalf("decode notification: %v", err)
	}
	return n
}

func TestHTTPDebugStatusAndCancel(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	h, err := NewHTTPDebugServer("127.0.0.1:0", s)
	if err != nil {
		t.Fatalf("NewHTTPDebugServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() { _ = h.server.Serve(ln) }()
	defer h.Stop(context.Background())

	client := &http.Client{Timeout: 2 * time.Second}
	token := h.token

	// GET /status without token — assert 401
	resp, err := client.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatalf("GET /status (no token): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /status (no token): %d, want 401", resp.StatusCode)
	}

	// GET /status with token — assert 200 and run_id
	req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/status", http.NoBody)
	req.Header.Set("X-Avenor-Token", token)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /status: %d, want 200", resp.StatusCode)
	}
	var status Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode /status: %v", err)
	}
	if status.RunID != "run_1" {
		t.Fatalf("run_id = %q, want run_1", status.RunID)
	}

	// POST /cancel — assert 200 and ok:true
	req, _ = http.NewRequest(http.MethodPost, "http://"+addr+"/cancel", http.NoBody)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Avenor-Token", token)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("POST /cancel: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /cancel: %d, want 200", resp.StatusCode)
	}
	var cancelResult map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&cancelResult); err != nil {
		t.Fatalf("decode /cancel: %v", err)
	}
	if v, _ := cancelResult["ok"].(bool); !v {
		t.Fatalf("cancel ok = %v, want true", cancelResult["ok"])
	}

	// POST /answer-permission with no pending permission — assert 409
	req, _ = http.NewRequest(http.MethodPost, "http://"+addr+"/answer-permission", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Avenor-Token", token)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("POST /answer-permission: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("POST /answer-permission: %d, want 409", resp.StatusCode)
	}
}

func TestPromptDispatch(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"text": "do something"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "prompt", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("prompt error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if v, _ := res["accepted"].(bool); !v {
		t.Fatal("expected accepted=true")
	}
}

func TestInterruptAndPromptDispatch(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	ch := s.InterruptChan()
	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"text": "new task"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "interrupt_and_prompt", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("interrupt_and_prompt error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if v, _ := res["accepted"].(bool); !v {
		t.Fatal("expected accepted=true")
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected InterruptChan to fire")
	}
}

func TestPromptRejectedForNonOwner(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c1 := mustDial(t, path)
	defer c1.Close()
	_ = writeReq(t, c1, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"})
	r1 := readResp(t, c1)
	if r1.Error != nil {
		t.Fatalf("cancel error: %+v", r1.Error)
	}

	c2 := mustDial(t, path)
	defer c2.Close()
	params, _ := json.Marshal(map[string]any{"text": "hello"})
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 2, Method: "prompt", Params: params})
	r2 := readResp(t, c2)
	if r2.Error == nil || r2.Error.Code != -32010 {
		t.Fatalf("expected permission_denied for prompt, got %+v", r2)
	}
}

func TestInterruptAndPromptRejectedForNonOwner(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c1 := mustDial(t, path)
	defer c1.Close()
	_ = writeReq(t, c1, Request{JSONRPC: "2.0", ID: 1, Method: "cancel"})
	r1 := readResp(t, c1)
	if r1.Error != nil {
		t.Fatalf("cancel error: %+v", r1.Error)
	}

	c2 := mustDial(t, path)
	defer c2.Close()
	params, _ := json.Marshal(map[string]any{"text": "new task"})
	_ = writeReq(t, c2, Request{JSONRPC: "2.0", ID: 2, Method: "interrupt_and_prompt", Params: params})
	r2 := readResp(t, c2)
	if r2.Error == nil || r2.Error.Code != -32010 {
		t.Fatalf("expected permission_denied for interrupt_and_prompt, got %+v", r2)
	}
}

func TestPromptMissingTextReturnsError(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"text": ""})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "prompt", Params: params})
	r := readResp(t, c)
	if r.Error == nil || r.Error.Code != -32602 {
		t.Fatalf("expected invalid params error, got %+v", r)
	}
}

func TestDequeuePromptOrder(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)

	s.QueuePrompt("prompt1")
	s.QueuePrompt("prompt2")

	if got := s.DequeuePrompt(); got != "prompt1" {
		t.Fatalf("first dequeue = %q, want prompt1", got)
	}
	if got := s.DequeuePrompt(); got != "prompt2" {
		t.Fatalf("second dequeue = %q, want prompt2", got)
	}
	if got := s.DequeuePrompt(); got != "" {
		t.Fatalf("third dequeue = %q, want empty", got)
	}
}

func testSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(os.TempDir(), "avc-"+time.Now().Format("150405.000000")+".sock")
}

type mockStableHandler struct {
	spawnResult          any
	spawnErr             error
	listResult           any
	sendToParentCalled   int
	sendToParentMessages []string
}

func (m *mockStableHandler) Spawn(params json.RawMessage) (any, error) {
	return m.spawnResult, m.spawnErr
}

func (m *mockStableHandler) List() any {
	return m.listResult
}

func (m *mockStableHandler) Shutdown(mode string) error { return nil }

func (m *mockStableHandler) RuntimeStatus(runtimeID string) (any, error) {
	return map[string]any{"runtime_id": runtimeID, "status": "running"}, nil
}

func (m *mockStableHandler) RuntimeCancel(runtimeID string) error { return nil }

func (m *mockStableHandler) RuntimePrompt(runtimeID, text, requestID string) error { return nil }

func (m *mockStableHandler) RuntimeAnswerPermission(runtimeID, requestID, optionID, message string) error {
	return nil
}

func (m *mockStableHandler) RuntimeInterruptAndPrompt(runtimeID, text string, keepQueue bool) error {
	return nil
}

func (m *mockStableHandler) RuntimeSendToParent(runtimeID, message string) error {
	m.sendToParentCalled++
	m.sendToParentMessages = append(m.sendToParentMessages, message)
	return nil
}

func TestStableSpawnMethod(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{
		spawnResult: map[string]any{"runtime_id": "rt_1", "session_id": "ses_x"},
	})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"prompt": "hello", "dir": "/tmp"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "spawn", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("spawn error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if v, _ := res["runtime_id"].(string); v != "rt_1" {
		t.Errorf("runtime_id = %q, want rt_1", v)
	}
}

func TestStableListMethod(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{
		listResult: []map[string]any{
			{"runtime_id": "rt_1", "status": "running"},
		},
	})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "list"})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("list error: %+v", r.Error)
	}
	list, ok := r.Result.([]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if len(list) != 1 {
		t.Fatalf("len(list) = %d, want 1", len(list))
	}
}

func TestStableStatusWithRuntimeID(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "status", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("status error: %+v", r.Error)
	}
	res, ok := r.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type: %T", r.Result)
	}
	if v, _ := res["runtime_id"].(string); v != "rt_1" {
		t.Errorf("runtime_id = %q, want rt_1", v)
	}
}

func TestStableCancelWithRuntimeID(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	s.SetStableHandler(&mockStableHandler{})
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "cancel", Params: params})
	r := readResp(t, c)
	if r.Error != nil {
		t.Fatalf("cancel error: %+v", r.Error)
	}
}

func TestStableSpawnRejectedWithoutHandler(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"prompt": "hello", "dir": "/tmp"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "spawn", Params: params})
	r := readResp(t, c)
	if r.Error == nil || r.Error.Code != -32601 {
		t.Fatalf("expected -32601 method not found, got %+v", r.Error)
	}
}

func TestStableSendToParent(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	mock := &mockStableHandler{}
	s.SetStableHandler(mock)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1", "message": "help"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "send_to_parent", Params: params})
	resp := readResp(t, c)
	if resp.Error != nil {
		t.Fatalf("send_to_parent error: %+v", resp.Error)
	}
	if mock.sendToParentCalled != 1 {
		t.Errorf("sendToParentCalled = %d, want 1", mock.sendToParentCalled)
	}
	if len(mock.sendToParentMessages) != 1 || mock.sendToParentMessages[0] != "help" {
		t.Errorf("sendToParentMessages = %v, want [help]", mock.sendToParentMessages)
	}
}

func TestStableSendToParentMissingRuntimeID(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	mock := &mockStableHandler{}
	s.SetStableHandler(mock)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"message": "help"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "send_to_parent", Params: params})
	resp := readResp(t, c)
	if resp.Error == nil {
		t.Fatal("expected error for missing runtime_id, got nil")
	}
	if mock.sendToParentCalled != 0 {
		t.Errorf("sendToParentCalled = %d, want 0 (should not be called)", mock.sendToParentCalled)
	}
}

func TestStableSendToParentMissingMessage(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	mock := &mockStableHandler{}
	s.SetStableHandler(mock)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1"})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "send_to_parent", Params: params})
	resp := readResp(t, c)
	if resp.Error == nil {
		t.Fatal("expected error for missing message, got nil")
	}
	if mock.sendToParentCalled != 0 {
		t.Errorf("sendToParentCalled = %d, want 0 (should not be called)", mock.sendToParentCalled)
	}
}

func TestStableSendToParentMessageTooLarge(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	mock := &mockStableHandler{}
	s.SetStableHandler(mock)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	c := mustDial(t, path)
	defer c.Close()
	big := strings.Repeat("a", maxSendToParentMessageBytes+1)
	params, _ := json.Marshal(map[string]any{"runtime_id": "rt_1", "message": big})
	_ = writeReq(t, c, Request{JSONRPC: "2.0", ID: 1, Method: "send_to_parent", Params: params})
	resp := readResp(t, c)
	if resp.Error == nil {
		t.Fatal("expected error for oversized message, got nil")
	}
	if mock.sendToParentCalled != 0 {
		t.Errorf("sendToParentCalled = %d, want 0 (should not be called)", mock.sendToParentCalled)
	}
}

// TestPermissionClaimDisconnectChClosedOnClientDisconnect verifies that when
// all control-socket clients disconnect, the disconnect channel for pending
// claims in Reserved or Control state is closed, signaling the resolver to
// fall through to a fallback handler.
func TestPermissionClaimDisconnectChClosedOnClientDisconnect(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	// No PreparePermissionClaim needed — BeginPermissionClaim creates the claim.
	answerCh, disconnectCh, ok := s.BeginPermissionClaim("rt_1", "req_1")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	defer s.EndPermissionClaim("rt_1", "req_1")

	// Without any clients connected, signalClientDisconnect should close the channel.
	s.signalClientDisconnect()

	select {
	case <-disconnectCh:
		// expected: channel was closed
	default:
		t.Fatal("disconnectCh was not closed after signalClientDisconnect")
	}

	// The claim should still exist (signalClientDisconnect only closes the
	// channel; the resolver is responsible for calling HandoffPermissionClaim
	// to transition the state).
	if got := s.PermissionResolverState("rt_1", "req_1"); got != PermissionResolverControl {
		t.Fatalf("resolver state after disconnect = %v, want Control (resolver must handoff)", got)
	}
	_ = answerCh // still valid, just unused
}

// TestPermissionClaimDisconnectChNotClosedWithClientsRemaining verifies that
// signalClientDisconnect does NOT close the disconnect channel when at least
// one client is still connected.
func TestPermissionClaimDisconnectChNotClosedWithClientsRemaining(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	// Connect a client.
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for !s.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("HasClients() never became true")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !s.PreparePermissionClaim("rt_1", "req_1", PermissionResolverReserved, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	_, disconnectCh, ok := s.BeginPermissionClaim("rt_1", "req_1")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	defer s.EndPermissionClaim("rt_1", "req_1")

	// With a client connected, signalClientDisconnect should be a no-op
	// (the re-check inside the method returns early).
	s.signalClientDisconnect()

	select {
	case <-disconnectCh:
		t.Fatal("disconnectCh was closed while a client is still connected")
	default:
		// expected: channel still open
	}
}

// TestPermissionClaimDisconnectChFiresOnRealDisconnect verifies the end-to-end
// path: a real client connects, a claim is started, then the client
// disconnects and the disconnect channel fires.
func TestPermissionClaimDisconnectChFiresOnRealDisconnect(t *testing.T) {
	state := NewState("run_1", "", 0)
	s := NewServer(state)
	path := testSocketPath(t)
	if err := s.Start(path); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Stop()

	// Connect a client.
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Wait for the server to register the connection.
	deadline := time.Now().Add(2 * time.Second)
	for !s.HasClients() {
		if time.Now().After(deadline) {
			t.Fatal("HasClients() never became true")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Prepare a Reserved claim (as resolvePermission would) then begin it.
	if !s.PreparePermissionClaim("rt_1", "req_1", PermissionResolverReserved, nil) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	_, disconnectCh, ok := s.BeginPermissionClaim("rt_1", "req_1")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	defer s.EndPermissionClaim("rt_1", "req_1")

	// disconnectCh should not be closed while the client is connected.
	select {
	case <-disconnectCh:
		t.Fatal("disconnectCh was closed while client is still connected")
	default:
	}

	// Disconnect the client.
	conn.Close()

	// First wait for the server to process the disconnect.
	disconnectDeadline := time.Now().Add(2 * time.Second)
	for s.HasClients() {
		if time.Now().After(disconnectDeadline) {
			t.Fatal("server still reports clients after disconnect")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Now the disconnect channel should be closed. Use a blocking
	// select with a timeout — HasClients() returning false means the
	// connection was removed from s.conns, but signalClientDisconnect()
	// (called next in disconnect()) may not have run yet.
	select {
	case <-disconnectCh:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("disconnectCh was not closed within 2 seconds after all clients disconnected")
	}
}

func TestPermissionAnswerRejectsUnpairedSurrogateMessage(t *testing.T) {
	var answer PermissionAnswer
	if err := json.Unmarshal([]byte(`{"request_id":"req","option_id":"other","message":"\uD800"}`), &answer); err == nil {
		t.Fatal("PermissionAnswer accepted malformed write-in")
	}
}

func TestPermissionAnswerPreservesMessage(t *testing.T) {
	s := NewServer(NewState("run_msg", "", 0))

	scope := "rt_msg"
	s.PreparePermissionClaim(scope, "req_msg", PermissionResolverReserved, nil)
	ch, _, ok := s.BeginPermissionClaim(scope, "req_msg")
	if !ok {
		t.Fatal("BeginPermissionClaim failed")
	}

	// Deliver with a message.
	if !s.AnswerPendingPermission(scope, "req_msg", "allow", "write-in note") {
		t.Fatal("AnswerPendingPermission returned false")
	}

	select {
	case ans := <-ch:
		if ans.Message != "write-in note" {
			t.Errorf("Message = %q, want %q", ans.Message, "write-in note")
		}
		if ans.OptionID != "allow" {
			t.Errorf("OptionID = %q, want allow", ans.OptionID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for answer")
	}
}

func TestDeliverPendingPermissionRequiresMessageWithoutConsumingClaim(t *testing.T) {
	s := NewServer(NewState("run", "label", 0))
	options := []any{map[string]any{"optionId": "other", "kind": "allow", "requiresMessage": true}}
	if !s.PreparePermissionClaim("scope", "req", PermissionResolverReserved, options) {
		t.Fatal("PreparePermissionClaim returned false")
	}
	answers, _, ok := s.BeginPermissionClaim("scope", "req")
	if !ok {
		t.Fatal("BeginPermissionClaim returned false")
	}
	if got := s.DeliverPendingPermission("scope", "req", "other", ""); got != PermissionAnswerInvalid {
		t.Fatalf("empty write-in delivery = %v", got)
	}
	if got := s.DeliverPendingPermission("scope", "req", "missing", "typed"); got != PermissionAnswerInvalid {
		t.Fatalf("unknown option delivery = %v", got)
	}
	if got := s.DeliverPendingPermission("scope", "req", "other", "typed"); got != PermissionAnswerDelivered {
		t.Fatalf("valid write-in delivery = %v", got)
	}
	select {
	case answer := <-answers:
		if answer.Message != "typed" || answer.OptionID != "other" {
			t.Fatalf("answer = %#v", answer)
		}
	case <-time.After(time.Second):
		t.Fatal("valid write-in was not queued")
	}
}

func TestDeliverPendingPermissionPreservesMessage(t *testing.T) {
	s := NewServer(NewState("run_deliver", "", 0))

	scope := "rt_deliver"
	s.PreparePermissionClaim(scope, "req_deliver", PermissionResolverReserved, nil)
	_, _, ok := s.BeginPermissionClaim(scope, "req_deliver")
	if !ok {
		t.Fatal("BeginPermissionClaim failed")
	}

	if got := s.DeliverPendingPermission(scope, "req_deliver", "deny", "denial reason"); got != PermissionAnswerDelivered {
		t.Fatalf("DeliverPendingPermission = %v, want Delivered", got)
	}

	// Verify the answer was queued with the message.
	s.pendingMu.Lock()
	claim := s.pendingClaims[permissionClaimKey{scope: scope, requestID: "req_deliver"}]
	s.pendingMu.Unlock()
	if claim == nil {
		t.Fatal("claim not found")
	}
	select {
	case ans := <-claim.answerCh:
		if ans.Message != "denial reason" {
			t.Errorf("Message = %q, want %q", ans.Message, "denial reason")
		}
	default:
		t.Fatal("answer not queued")
	}
}
