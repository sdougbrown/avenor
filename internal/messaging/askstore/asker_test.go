package askstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// TestAskerAdmitsAnUnversionedIDOnlyWhenTheCallerNamesItLegacy pins that the
// ask seam hands the admission contract's legacy flag to the store, so the
// store's admission has one owner.
func TestAskerAdmitsAnUnversionedIDOnlyWhenTheCallerNamesItLegacy(t *testing.T) {
	store := New(Options{})
	asker := NewAsker(store)
	req := messaging.SendRequest{
		From:      messaging.Identity{RunID: "alice"},
		To:        messaging.Identity{RunID: "bob"},
		MessageID: "plain-id",
		Body:      "{}",
	}
	if _, err := asker.Send(context.Background(), req, false); !errors.Is(err, messaging.ErrGenerationMismatch) {
		t.Fatalf("unversioned send without legacy = %v, want ErrGenerationMismatch", err)
	}
	res, err := asker.Send(context.Background(), req, true)
	if err != nil || res.Outcome != messaging.OutcomeSent {
		t.Fatalf("unversioned send with legacy = (%+v, %v), want sent", res, err)
	}
}

// TestAskerKeepsAnEmptyReplyPayloadValid pins that a reply with no body
// stores a payload the wire can carry, so an empty answer never breaks the
// JSON the asker reads.
func TestAskerKeepsAnEmptyReplyPayloadValid(t *testing.T) {
	store := New(Options{})
	asker := NewAsker(store)
	gen := store.Generation()
	now := time.Now().UTC()
	msgID, err := messaging.NewMsgID(now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("message id: %v", err)
	}
	send := messaging.SendRequest{
		From:       messaging.Identity{RunID: "alice"},
		To:         messaging.Identity{RunID: "bob"},
		MessageID:  msgID,
		Generation: gen,
		Body:       "{}",
	}
	if _, err := asker.Send(context.Background(), send, false); err != nil {
		t.Fatalf("send: %v", err)
	}
	outcome, err := asker.Reply(context.Background(), messaging.ReplyRequest{
		From:      messaging.Identity{RunID: "bob"},
		ToRunID:   "alice",
		MessageID: msgID,
	})
	if err != nil || outcome != messaging.OutcomeReplied {
		t.Fatalf("reply = (%q, %v), want replied", outcome, err)
	}
	res, err := asker.Wait(context.Background(), messaging.WaitRequest{
		Sender:     messaging.Identity{RunID: "alice"},
		MessageID:  msgID,
		Generation: gen,
		Timeout:    time.Second,
	})
	if err != nil || res.Outcome != messaging.OutcomeReplied {
		t.Fatalf("wait = (%+v, %v), want replied", res, err)
	}
	if res.Reply == nil {
		t.Fatal("wait reply = nil, want the reply")
	}
	if _, err := json.Marshal(res.Reply); err != nil {
		t.Fatalf("reply payload is not valid JSON on the wire: %v", err)
	}
}
