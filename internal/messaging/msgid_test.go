package messaging

import (
	"errors"
	"testing"
	"time"
)

func TestMsgID(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

	t.Run("deadline round trip", func(t *testing.T) {
		deadline := now.Add(2 * time.Minute)
		id, err := NewMsgID(now, deadline)
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		got, err := ParseMsgID(id, now)
		if err != nil {
			t.Fatalf("ParseMsgID: %v", err)
		}
		if !got.Equal(deadline) {
			t.Fatalf("deadline = %s, want %s", got, deadline)
		}
	})

	t.Run("zero deadline uses the wait cap", func(t *testing.T) {
		id, err := NewMsgID(now, time.Time{})
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		got, err := ParseMsgID(id, now)
		if err != nil {
			t.Fatalf("ParseMsgID: %v", err)
		}
		if want := now.Add(MaxWait); !got.Equal(want) {
			t.Fatalf("deadline = %s, want %s", got, want)
		}
	})

	t.Run("bounds", func(t *testing.T) {
		if _, err := NewMsgID(now, now); !errors.Is(err, ErrMessageIDExpired) {
			t.Fatalf("past deadline error = %v, want ErrMessageIDExpired", err)
		}
		if _, err := NewMsgID(now, now.Add(MaxAcceptanceHorizon+time.Second)); !errors.Is(err, ErrMessageIDHorizon) {
			t.Fatalf("beyond horizon error = %v, want ErrMessageIDHorizon", err)
		}
	})

	t.Run("receiver rejects expired and beyond-horizon ids", func(t *testing.T) {
		id, err := NewMsgID(now, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		if _, err := ParseMsgID(id, now.Add(2*time.Minute)); !errors.Is(err, ErrMessageIDExpired) {
			t.Fatalf("expired parse error = %v, want ErrMessageIDExpired", err)
		}
		if _, err := ParseMsgID(id, now.Add(-MaxAcceptanceHorizon)); !errors.Is(err, ErrMessageIDHorizon) {
			t.Fatalf("horizon parse error = %v, want ErrMessageIDHorizon", err)
		}
	})

	t.Run("malformed", func(t *testing.T) {
		for _, id := range []string{"", "av1", "av1.zz.00", "av2.abc.0123", "av1.abc.nothex"} {
			if _, err := ParseMsgID(id, now); !errors.Is(err, ErrMalformedMessageID) {
				t.Errorf("ParseMsgID(%q) error = %v, want ErrMalformedMessageID", id, err)
			}
		}
	})

	t.Run("nonce uniqueness", func(t *testing.T) {
		seen := make(map[string]bool, 128)
		for i := 0; i < 128; i++ {
			id, err := NewMsgID(now, now.Add(time.Minute))
			if err != nil {
				t.Fatalf("NewMsgID: %v", err)
			}
			if seen[id] {
				t.Fatalf("duplicate id %q", id)
			}
			seen[id] = true
		}
	})

	t.Run("deadline change yields a different id", func(t *testing.T) {
		short, err := NewMsgID(now, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		long, err := NewMsgID(now, now.Add(2*time.Minute))
		if err != nil {
			t.Fatalf("NewMsgID: %v", err)
		}
		if short == long {
			t.Fatal("ids with different deadlines are equal")
		}
		shortDeadline, err := ParseMsgID(short, now)
		if err != nil {
			t.Fatalf("ParseMsgID(short): %v", err)
		}
		longDeadline, err := ParseMsgID(long, now)
		if err != nil {
			t.Fatalf("ParseMsgID(long): %v", err)
		}
		if !longDeadline.After(shortDeadline) {
			t.Fatalf("long deadline %s is not after short deadline %s", longDeadline, shortDeadline)
		}
	})
}
