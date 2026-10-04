package control

// IdempotencyConflictError reports a reused idempotency key whose spawn
// parameters do not match the parameters recorded for the first use.
type IdempotencyConflictError struct{ Key string }

func (e *IdempotencyConflictError) Error() string {
	return "idempotency key reused with different parameters"
}

// IdempotencyCapacityError reports that the idempotency store is full of
// unexpired entries and cannot reserve a slot for a new key.
type IdempotencyCapacityError struct {
	Key      string
	Capacity int
}

func (e *IdempotencyCapacityError) Error() string { return "idempotency capacity exhausted" }
