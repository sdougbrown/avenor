// Package feed turns broker messages into poo feed records.
package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/messaging"
)

// DefaultLockDir returns the feed lock directory under stateHome.
func DefaultLockDir(stateHome string) string {
	return filepath.Join(stateHome, "feed-locks")
}

// Broker holds the credentials and output for one feed consumer.
type Broker struct {
	// RunID identifies the receiving run.
	RunID string
	// Token authenticates the receiving run.
	Token string
	// BrokerURL identifies the broker serving the inbox.
	BrokerURL string
	// Limit bounds each drained batch.
	Limit int
	// RequestTimeout bounds each preflight and poll.
	RequestTimeout time.Duration
	// LockDir holds persistent advisory lock files.
	LockDir string
	// AsksDir holds the claims for delivered asks.
	AsksDir string
	// Out receives feed records.
	Out io.Writer
	// Log receives diagnostics without message text.
	Log io.Writer
}

// Run consumes messages until shutdown, parent loss, or an unrecoverable write.
// Terminal broker errors park until shutdown or parent loss to avoid restart storms.
func (b Broker) Run(ctx context.Context) int {
	parent := os.Getppid()
	return b.run(ctx, parent, os.Getppid)
}

// run owns the lock and the consumer for the lifetime of the feed.
func (b Broker) run(ctx context.Context, parent int, parentPID func() int) int {
	if parent == 1 {
		return 0
	}
	if b.Log == nil {
		b.Log = io.Discard
	}
	if b.RunID == "" || b.Token == "" || b.BrokerURL == "" {
		fmt.Fprintln(b.Log, "feed broker: missing broker credentials")
		return 2
	}
	if b.Limit <= 0 {
		b.Limit = 8
	}
	if b.RequestTimeout <= 0 {
		b.RequestTimeout = 15 * time.Second
	}
	if b.Out == nil {
		b.Out = io.Discard
	}
	if _, err := askclaim.BrokerKey(b.BrokerURL, b.RunID); err != nil {
		fmt.Fprintln(b.Log, "feed broker: invalid broker URL")
		return 1
	}
	// Start-up maintenance runs before the contested lock: a feed that loses
	// the lock still sweeps the records an earlier run left behind, and it does
	// so before it contends for the inbox.
	reporter := &pruneReporter{out: b.Log, prefix: "feed broker"}
	b.pruneAskClaims(reporter)
	if b.AsksDir == "" {
		dir, err := askclaim.DefaultDir(appdir.StateHome(), b.BrokerURL, b.RunID)
		if err != nil {
			fmt.Fprintln(b.Log, "feed broker: invalid broker URL")
			return 1
		}
		b.AsksDir = dir
	}
	unlock, held, err := AcquireLock(b.LockDir, b.BrokerURL, b.RunID)
	if err != nil {
		if errors.Is(err, ErrInvalidBrokerURL) {
			fmt.Fprintln(b.Log, "feed broker: invalid broker URL")
		} else {
			fmt.Fprintln(b.Log, "feed broker: cannot lock:", err)
		}
		return 1
	}
	if held {
		fmt.Fprintln(b.Log, "feed broker: consumer lock is held")
		return 0
	}
	defer unlock()
	alive := func() bool { return parentPID() == parent }
	err = Consume(ctx, ConsumerConfig{
		Surface:          messaging.SurfaceFeed,
		RunID:            b.RunID,
		Token:            b.Token,
		BrokerURL:        b.BrokerURL,
		AsksDir:          b.AsksDir,
		Limit:            b.Limit,
		RequestTimeout:   b.RequestTimeout,
		Log:              b.Log,
		LogPrefix:        "feed broker",
		SkipInitialPrune: true,
		Alive:            alive,
		OnRow:            b.write,
	})
	if errors.Is(err, ErrInvalidProviderConfig) {
		return 1
	}
	return 0
}

// write emits one feed record as NDJSON, with the footer an ask needs to be
// answered from the surface that received it.
func (b Broker) write(m Message) error {
	text := m.Text
	if m.ExpectsReply {
		quotedSender, _ := json.Marshal(m.From)
		if m.Answerable {
			quotedID, _ := json.Marshal(m.ID)
			text += fmt.Sprintf("\n\nThis message is an ask from %s. To answer, use call_cli with {\"name\": \"avenor_message\", \"args\": {\"action\": \"reply\", \"reply_to\": %s, \"message\": \"...\"}}.", quotedSender, quotedID)
		} else {
			text += fmt.Sprintf("\n\nThis message is an ask from %s, but it cannot be answered here.", quotedSender)
		}
	}
	line := struct {
		Text string `json:"text"`
		From string `json:"from"`
		ID   string `json:"id"`
	}{text, m.From, m.ID}
	err := json.NewEncoder(b.Out).Encode(line)
	if err == nil {
		if flusher, ok := b.Out.(interface{ Flush() error }); ok {
			err = flusher.Flush()
		}
	}
	return err
}

// pruneAskClaims sweeps the records this feed's run left behind. It is the
// start-up half of the maintenance Consume repeats hourly.
func (b *Broker) pruneAskClaims(reporter *pruneReporter) {
	b.claimPruner().prune(reporter)
}

// claimPruner names the records a Broker's run accumulates.
func (b *Broker) claimPruner() askClaimPruner {
	return askClaimPruner{asksDir: b.AsksDir, brokerURL: b.BrokerURL, runID: b.RunID}
}
