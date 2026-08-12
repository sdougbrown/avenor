package runstate

// Snapshot contains the authoritative lifecycle fields used to decide whether
// an await operation keeps waiting or returns.
type Snapshot struct {
	Status            string
	Phase             string
	PendingPermission bool
}

// State is the normalized state of an awaited run.
type State string

const (
	StateActive            State = "active"
	StateDone              State = "done"
	StateFailed            State = "failed"
	StateTimeout           State = "timeout"
	StateKilled            State = "killed"
	StatePendingPermission State = "pending_permission"
)

// Action tells an await loop what to do after an observation.
type Action string

const (
	ActionContinue   Action = "continue"
	ActionResnapshot Action = "resnapshot"
	ActionExit       Action = "exit"
)

// Decision is the state machine result for a snapshot or event wakeup.
type Decision struct {
	State  State
	Action Action
}

// Machine derives await decisions from authoritative snapshots. Its zero value
// is ready to use.
type Machine struct {
	state State
}

// ObserveSnapshot replaces the machine state with the state derived from the
// supplied snapshot. The update is idempotent for a repeated snapshot.
func (m *Machine) ObserveSnapshot(snapshot Snapshot) Decision {
	next := stateFromSnapshot(snapshot)
	m.state = next

	action := ActionContinue
	if next != StateActive {
		action = ActionExit
	}
	return Decision{State: next, Action: action}
}

// ObserveEvent treats lifecycle events as wakeups only. State-relevant and
// lag notifications request an authoritative snapshot; no event, including a
// lag notification, changes state or directly exits the await loop.
func (m *Machine) ObserveEvent(event string) Decision {
	action := ActionContinue
	switch event {
	case "permission.request", "permission.response", "session.end", "agent.status", "subscriber.lagged", "client.lagged":
		action = ActionResnapshot
	}
	return Decision{State: m.currentState(), Action: action}
}

func (m *Machine) currentState() State {
	if m.state == "" {
		return StateActive
	}
	return m.state
}

func stateFromSnapshot(snapshot Snapshot) State {
	// A pending permission is an immediate, actionable await exit even if the
	// public status has not changed from running.
	if snapshot.PendingPermission {
		return StatePendingPermission
	}

	switch Translate(snapshot.Status, snapshot.Phase).Status {
	case "done":
		return StateDone
	case "failed":
		return StateFailed
	case "timeout":
		return StateTimeout
	case "killed":
		return StateKilled
	default:
		return StateActive
	}
}
