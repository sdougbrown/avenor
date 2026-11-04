// Package feed turns broker messages into poo feed records.
package feed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sdougbrown/avenor/internal/childmessaging"
	"github.com/sdougbrown/avenor/internal/durablefile"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

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

// run owns the lock and the provider for the lifetime of the feed.
func (b Broker) run(ctx context.Context, parent int, parentPID func() int) int {
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
	u, err := url.Parse(b.BrokerURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		fmt.Fprintln(b.Log, "feed broker: invalid broker URL")
		return 1
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	if (u.Scheme == "http" && u.Port() == "80") || (u.Scheme == "https" && u.Port() == "443") {
		u.Host = u.Hostname()
		if strings.Contains(u.Host, ":") {
			u.Host = "[" + u.Host + "]"
		}
	} else if u.Port() != "" {
		u.Host = net.JoinHostPort(u.Hostname(), u.Port())
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	sum := sha256.Sum256([]byte(u.String() + "\x00" + b.RunID))
	if err := os.MkdirAll(b.LockDir, 0700); err != nil {
		fmt.Fprintln(b.Log, "feed broker: cannot create lock directory:", err)
		return 1
	}
	unlock, held, err := durablefile.TryLock(filepath.Join(b.LockDir, hex.EncodeToString(sum[:])+".lock"))
	if err != nil {
		fmt.Fprintln(b.Log, "feed broker: cannot lock:", err)
		return 1
	}
	if held {
		return 0
	}
	defer unlock()
	alive := func() bool { return parentPID() == parent }
	wait := func(d time.Duration) bool {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		var timer <-chan time.Time
		if d > 0 {
			t := time.NewTimer(d)
			defer t.Stop()
			timer = t.C
		}
		for {
			if !alive() {
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case <-ticker.C:
			case <-timer:
				return true
			}
		}
	}
	limit := b.Limit
	for alive() && ctx.Err() == nil {
		p, err := childmessaging.NewMessageProvider(messaging.SurfaceFeed, childmessaging.Options{RunID: b.RunID, Token: b.Token, BrokerURL: b.BrokerURL})
		if err != nil {
			fmt.Fprintln(b.Log, "feed broker: invalid provider configuration")
			return 1
		}
		requestCtx, cancel := context.WithTimeout(context.Background(), b.RequestTimeout)
		err = p.Prepare(requestCtx)
		cancel()
		if !alive() || ctx.Err() != nil {
			_ = p.Close()
			return 0
		}
		for err == nil && alive() && ctx.Err() == nil {
			requestCtx, cancel = context.WithTimeout(context.Background(), b.RequestTimeout)
			var result messaging.ReceiveResult
			result, err = p.Receive(requestCtx, messaging.ReceiveRequest{Generation: p.Generation(), Timeout: b.RequestTimeout, Limit: limit})
			cancel()
			if errors.Is(err, localhttp.ErrResponseTooLarge) {
				fmt.Fprintln(b.Log, "feed broker: drained batch exceeded response cap; rows lost (ids unknown)")
				limit = 1
				err = nil
				continue
			}
			if err != nil {
				break
			}
			limit = b.Limit
			for i, row := range result.Messages {
				var message broker.AgentMessage
				if decodeErr := json.Unmarshal(row.Payload, &message); decodeErr != nil || message.Message == "" {
					b.lost(result.Messages[i : i+1])
					continue
				}
				sender := row.FromRunID
				if sender == "" {
					sender = "unknown"
				}
				id := message.ID
				if id == "" {
					id = row.MessageID
				}
				text := message.Message
				if message.ExpectsReply {
					quotedID, _ := json.Marshal(id)
					quotedSender, _ := json.Marshal(sender)
					quotedRunID, _ := json.Marshal(row.FromRunID)
					text += fmt.Sprintf("\n\nThis message is an ask from %s. To answer, use call_cli with {\"name\": \"avenor_reply\", \"args\": {\"reply_to\": %s, \"to\": %s, \"text\": \"...\"}}.", quotedSender, quotedID, quotedRunID)
				}
				line := struct {
					Text string `json:"text"`
					From string `json:"from"`
					ID   string `json:"id"`
				}{text, sender, id}
				writeErr := json.NewEncoder(b.Out).Encode(line)
				if writeErr == nil {
					if flusher, ok := b.Out.(interface{ Flush() error }); ok {
						writeErr = flusher.Flush()
					}
				}
				if writeErr != nil {
					b.lost(result.Messages[i:])
					_ = p.Close()
					return 0
				}
			}
		}
		_ = p.Close()
		if !alive() || ctx.Err() != nil {
			return 0
		}
		if terminal(err) {
			var status *localhttp.StatusError
			if errors.As(err, &status) {
				fmt.Fprintf(b.Log, "feed broker: terminal broker refusal (HTTP %d)\n", status.Status)
			} else {
				fmt.Fprintln(b.Log, "feed broker: terminal broker protocol or identity error")
			}
			wait(0)
			return 0
		}
		fmt.Fprintln(b.Log, "feed broker: temporary broker error; retrying")
		if !wait(2 * time.Second) {
			return 0
		}
	}
	return 0
}

// lost reports the IDs of drained messages that could not be delivered.
func (b Broker) lost(rows []messaging.InboundMessage) {
	for _, row := range rows {
		id := row.MessageID
		if id == "" {
			var payload struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(row.Payload, &payload)
			id = payload.ID
		}
		fmt.Fprintf(b.Log, "feed broker: undelivered message id %q\n", id)
	}
}

// terminal classifies typed HTTP statuses before their wrapped sentinels.
func terminal(err error) bool {
	if errors.Is(err, localhttp.ErrResponseTooLarge) {
		return false
	}
	var status *localhttp.StatusError
	if errors.As(err, &status) {
		if status.Status == http.StatusConflict || status.Status == http.StatusTooManyRequests {
			return false
		}
		return status.Status >= 400 && status.Status < 500
	}
	return errors.Is(err, localhttp.ErrUnsupportedLocalProtocol) || errors.Is(err, messaging.ErrAuthRejected) || errors.Is(err, messaging.ErrInvalidRequest)
}
