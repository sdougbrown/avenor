package mcpserver

// Temporary until 356-08: the messaging verbs ControlClient gained are stubbed
// for the follow-up idempotency fake, whose final version supplies them itself.

import (
	"context"

	"github.com/sdougbrown/avenor/internal/messaging"
)

func (f *fakeIdempotentSupervisor) MessageSendContext(ctx context.Context, req messaging.SendRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageWaitContext(ctx context.Context, req messaging.WaitRequest) (messaging.WaitResult, error) {
	return messaging.WaitResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageReplyContext(ctx context.Context, req messaging.ReplyRequest) (messaging.SendResult, error) {
	return messaging.SendResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageWithdrawContext(ctx context.Context, req messaging.WithdrawRequest) (messaging.WithdrawResult, error) {
	return messaging.WithdrawResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessageReceiveContext(ctx context.Context, req messaging.ReceiveRequest) (messaging.ReceiveResult, error) {
	return messaging.ReceiveResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessagePeersContext(ctx context.Context, req messaging.PeersRequest) (messaging.PeersResult, error) {
	return messaging.PeersResult{}, nil
}
func (f *fakeIdempotentSupervisor) MessagePreflightContext(ctx context.Context) (messaging.PreflightResult, error) {
	return messaging.PreflightResult{Protocol: messaging.ProtocolVersion, Generation: "gen-test"}, nil
}
func (f *fakeIdempotentSupervisor) Resume(req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	return messaging.ResumeResult{}, nil
}
func (f *fakeIdempotentSupervisor) SessionRename(sessionID, name string) (messaging.RenameResult, error) {
	return messaging.RenameResult{}, nil
}
func (f *fakeIdempotentSupervisor) SessionLast(runtimeID string) (messaging.PeerLast, error) {
	return messaging.PeerLast{}, nil
}
func (f *fakeIdempotentSupervisor) ResumeContext(_ context.Context, req messaging.ResumeRequest) (messaging.ResumeResult, error) {
	return f.Resume(req)
}
func (f *fakeIdempotentSupervisor) SessionRenameContext(_ context.Context, sessionID, name string) (messaging.RenameResult, error) {
	return f.SessionRename(sessionID, name)
}
func (f *fakeIdempotentSupervisor) SessionLastContext(_ context.Context, runtimeID string) (messaging.PeerLast, error) {
	return f.SessionLast(runtimeID)
}
