package runstate

import "testing"

type machineStep struct {
	event    string
	snapshot *Snapshot
	want     Decision
}

func snapshotStep(snapshot Snapshot, want Decision) machineStep {
	return machineStep{snapshot: &snapshot, want: want}
}

func eventStep(event string, want Decision) machineStep {
	return machineStep{event: event, want: want}
}

func TestMachineSnapshotSequences(t *testing.T) {
	active := Snapshot{Status: "running", Phase: "working"}
	activeDecision := Decision{State: StateActive, Action: ActionContinue}

	tests := []struct {
		name  string
		steps []machineStep
	}{
		{
			name: "done",
			steps: []machineStep{
				snapshotStep(active, activeDecision),
				eventStep("session.end", Decision{State: StateActive, Action: ActionResnapshot}),
				snapshotStep(Snapshot{Status: "idle", Phase: "done"}, Decision{State: StateDone, Action: ActionExit}),
				snapshotStep(Snapshot{Status: "idle", Phase: "done"}, Decision{State: StateDone, Action: ActionExit}),
			},
		},
		{
			name: "failed",
			steps: []machineStep{
				snapshotStep(active, activeDecision),
				eventStep("agent.status", Decision{State: StateActive, Action: ActionResnapshot}),
				snapshotStep(Snapshot{Status: "idle", Phase: "failed"}, Decision{State: StateFailed, Action: ActionExit}),
				snapshotStep(Snapshot{Status: "idle", Phase: "failed"}, Decision{State: StateFailed, Action: ActionExit}),
			},
		},
		{
			name: "timeout",
			steps: []machineStep{
				snapshotStep(active, activeDecision),
				eventStep("session.end", Decision{State: StateActive, Action: ActionResnapshot}),
				snapshotStep(Snapshot{Status: "idle", Phase: "timeout"}, Decision{State: StateTimeout, Action: ActionExit}),
				snapshotStep(Snapshot{Status: "idle", Phase: "timeout"}, Decision{State: StateTimeout, Action: ActionExit}),
			},
		},
		{
			name: "killed",
			steps: []machineStep{
				snapshotStep(active, activeDecision),
				eventStep("agent.status", Decision{State: StateActive, Action: ActionResnapshot}),
				snapshotStep(Snapshot{Status: "idle", Phase: "killed"}, Decision{State: StateKilled, Action: ActionExit}),
				snapshotStep(Snapshot{Status: "idle", Phase: "killed"}, Decision{State: StateKilled, Action: ActionExit}),
			},
		},
		{
			name: "pending permission",
			steps: []machineStep{
				snapshotStep(active, activeDecision),
				eventStep("permission.request", Decision{State: StateActive, Action: ActionResnapshot}),
				snapshotStep(Snapshot{Status: "running", Phase: "waiting", PendingPermission: true}, Decision{State: StatePendingPermission, Action: ActionExit}),
				snapshotStep(Snapshot{Status: "running", Phase: "waiting", PendingPermission: true}, Decision{State: StatePendingPermission, Action: ActionExit}),
			},
		},
		{
			name: "permission cleared",
			steps: []machineStep{
				snapshotStep(Snapshot{Status: "running", Phase: "waiting", PendingPermission: true}, Decision{State: StatePendingPermission, Action: ActionExit}),
				eventStep("permission.response", Decision{State: StatePendingPermission, Action: ActionResnapshot}),
				snapshotStep(Snapshot{Status: "running", Phase: "waiting"}, activeDecision),
				snapshotStep(Snapshot{Status: "running", Phase: "working"}, activeDecision),
			},
		},
		{
			name: "lagged",
			steps: []machineStep{
				snapshotStep(active, activeDecision),
				eventStep("subscriber.lagged", Decision{State: StateActive, Action: ActionResnapshot}),
				eventStep("client.lagged", Decision{State: StateActive, Action: ActionResnapshot}),
				snapshotStep(Snapshot{Status: "idle", Phase: "done"}, Decision{State: StateDone, Action: ActionExit}),
				eventStep("subscriber.lagged", Decision{State: StateDone, Action: ActionResnapshot}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var machine Machine
			for i, step := range tt.steps {
				var got Decision
				if step.snapshot != nil {
					got = machine.ObserveSnapshot(*step.snapshot)
				} else {
					got = machine.ObserveEvent(step.event)
				}
				if got != step.want {
					t.Fatalf("step %d = %+v, want %+v", i, got, step.want)
				}
			}
		})
	}
}

func TestMachineEventWakeups(t *testing.T) {
	for _, event := range []string{
		"permission.request",
		"permission.response",
		"session.end",
		"agent.status",
		"subscriber.lagged",
		"client.lagged",
	} {
		t.Run(event, func(t *testing.T) {
			var machine Machine
			want := Decision{State: StateActive, Action: ActionResnapshot}
			if got := machine.ObserveEvent(event); got != want {
				t.Fatalf("ObserveEvent(%q) = %+v, want %+v", event, got, want)
			}
		})
	}
}

func TestMachineIgnoresNonLifecycleEvents(t *testing.T) {
	var machine Machine
	want := Decision{State: StateActive, Action: ActionContinue}
	if got := machine.ObserveEvent("agent.message_chunk"); got != want {
		t.Fatalf("ObserveEvent(non-lifecycle) = %+v, want %+v", got, want)
	}
}

func TestMachinePreservesTranslationSemantics(t *testing.T) {
	tests := []struct {
		name     string
		snapshot Snapshot
		want     Decision
	}{
		{
			name:     "running with terminal phase remains active",
			snapshot: Snapshot{Status: "running", Phase: "done"},
			want:     Decision{State: StateActive, Action: ActionContinue},
		},
		{
			name:     "idle with terminal phase completes",
			snapshot: Snapshot{Status: "idle", Phase: "done"},
			want:     Decision{State: StateDone, Action: ActionExit},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var machine Machine
			if got := machine.ObserveSnapshot(tt.snapshot); got != tt.want {
				t.Fatalf("ObserveSnapshot(%+v) = %+v, want %+v", tt.snapshot, got, tt.want)
			}
		})
	}
}

func TestMachinePendingPermissionPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		snapshot Snapshot
		want     Decision
	}{
		{
			name:     "running done still actionable",
			snapshot: Snapshot{Status: "running", Phase: "done", PendingPermission: true},
			want:     Decision{State: StatePendingPermission, Action: ActionExit},
		},
		{
			name:     "idle done still actionable",
			snapshot: Snapshot{Status: "idle", Phase: "done", PendingPermission: true},
			want:     Decision{State: StatePendingPermission, Action: ActionExit},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var machine Machine
			if got := machine.ObserveSnapshot(tt.snapshot); got != tt.want {
				t.Fatalf("ObserveSnapshot(%+v) = %+v, want %+v", tt.snapshot, got, tt.want)
			}
		})
	}
}

func TestMachineTerminalStatusSnapshots(t *testing.T) {
	tests := []struct {
		status string
		state  State
	}{
		{status: "done", state: StateDone},
		{status: "failed", state: StateFailed},
		{status: "timeout", state: StateTimeout},
		{status: "killed", state: StateKilled},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			var machine Machine
			want := Decision{State: tt.state, Action: ActionExit}
			if got := machine.ObserveSnapshot(Snapshot{Status: tt.status}); got != want {
				t.Fatalf("ObserveSnapshot(status=%q) = %+v, want %+v", tt.status, got, want)
			}
		})
	}
}
