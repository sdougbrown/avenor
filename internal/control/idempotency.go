package control

import "fmt"

// IdempotencyConflictError reports a reused idempotency key whose spawn
// parameters do not match the parameters recorded for the first use.
type IdempotencyConflictError struct{ Key string }

func (e *IdempotencyConflictError) Error() string {
	if e.Key == "" {
		return "idempotency key reused with different parameters"
	}
	return fmt.Sprintf("idempotency key %q reused with different parameters", e.Key)
}

// IdempotencyCapacityError reports that the idempotency store is full of
// unexpired entries and cannot reserve a slot for a new key.
type IdempotencyCapacityError struct {
	Key      string
	Capacity int
}

func (e *IdempotencyCapacityError) Error() string {
	if e.Key == "" && e.Capacity == 0 {
		return "idempotency capacity exhausted"
	}
	return fmt.Sprintf("idempotency capacity exhausted (%d unexpired entries); cannot reserve key %q", e.Capacity, e.Key)
}
