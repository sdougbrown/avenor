package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/childmessaging"
	"github.com/sdougbrown/avenor/internal/durablefile"
	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/localhttp"
	"github.com/sdougbrown/avenor/internal/runtime/broker"
)

// ErrInvalidProviderConfig reports credentials the child messaging provider
// refuses to build from. A consumer that cannot build a provider stops.
var ErrInvalidProviderConfig = errors.New("invalid provider configuration")

// ErrInvalidBrokerURL reports a broker URL a consumer cannot key.
var ErrInvalidBrokerURL = errors.New("invalid broker URL")

// Message is one inbound broker message, decoded for the surface that delivers
// it. Answerable is false for an ask the consumer cannot route a reply to:
// one with no stamped sender, or one whose claim could not be recorded.
type Message struct {
	// ID names the message, preferring the broker envelope's id.
	ID string
	// From is the stamped sender run ID, or "unknown" when the broker sent none.
	From string
	// Text is the message body without any surface-specific footer.
	Text string
	// ExpectsReply reports an ask rather than a tell.
	ExpectsReply bool
	// Answerable reports whether a reply to this ask can be routed.
	Answerable bool
}

// AcquireLock takes the consumer's advisory lock for one (broker, run) pair. A
// held lock means another consumer already serves this inbox.
func AcquireLock(lockDir, brokerURL, runID string) (unlock func() error, held bool, err error) {
	key, err := askclaim.BrokerKey(brokerURL, runID)
	if err != nil {
		return nil, false, ErrInvalidBrokerURL
	}
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return nil, false, fmt.Errorf("create lock directory %s: %w", lockDir, err)
	}
	path := filepath.Join(lockDir, key+".lock")
	unlock, held, err = durablefile.TryLock(path)
	if err != nil {
		return nil, false, fmt.Errorf("lock %s: %w", path, err)
	}
	return unlock, held, nil
}

// ConsumerConfig is what one inbox consumer supplies. The Surface decides which
// broker surface the messages are read as; every other field either defaults or
// is optional, so a caller states only what its surface adds.
type ConsumerConfig struct {
	// Surface is the messaging surface the provider speaks as.
	Surface messaging.Surface
	// RunID, Token and BrokerURL are the receiving run's broker credentials.
	RunID     string
	Token     string
	BrokerURL string
	// AsksDir holds the claims for delivered asks. It defaults to the
	// per-(broker, run) directory under the state home.
	AsksDir string
	// Limit bounds each drained batch, and defaults to 8.
	Limit int
	// RequestTimeout bounds each prepare and poll, and defaults to 15s.
	RequestTimeout time.Duration
	// Log receives diagnostics without message text. It defaults to io.Discard.
	Log io.Writer
	// LogPrefix leads every log line. It defaults to "feed broker".
	LogPrefix string
	// CancelInFlight abandons an in-flight request when ctx ends. A caller that
	// wants the current batch drained before shutdown leaves it false.
	CancelInFlight bool
	// Alive is an optional liveness predicate; nil means always alive.
	Alive func() bool
	// BeforePoll is an optional backpressure hook called before each poll. A
	// non-nil return ends the loop cleanly.
	BeforePoll func(context.Context) error
	// OnRow delivers one decoded message. A non-nil return ends the loop with
	// that error after the remaining drained rows are reported lost.
	OnRow func(Message) error
	// OnBatchDone is called after a batch that delivered at least one row.
	OnBatchDone func()
	// SkipInitialPrune says the caller already pruned this run's claims, so
	// Consume must not prune again.
	SkipInitialPrune bool
}

// Consume delivers broker messages row at a time until ctx ends, the surface is
// no longer alive, or OnRow fails. The caller holds the advisory lock that
// makes it this inbox's only consumer.
func Consume(ctx context.Context, cfg ConsumerConfig) error {
	if cfg.RunID == "" || cfg.Token == "" || cfg.BrokerURL == "" {
		return fmt.Errorf("feed consumer: run id, token, and broker URL are required")
	}
	if cfg.AsksDir == "" {
		dir, err := askclaim.DefaultDir(appdir.StateHome(), cfg.BrokerURL, cfg.RunID)
		if err != nil {
			return ErrInvalidBrokerURL
		}
		cfg.AsksDir = dir
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 8
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 15 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	if cfg.LogPrefix == "" {
		cfg.LogPrefix = "feed broker"
	}
	alive := cfg.Alive
	if alive == nil {
		alive = func() bool { return true }
	}
	reporter := &pruneReporter{out: cfg.Log, prefix: cfg.LogPrefix}
	claims := askClaimPruner{asksDir: cfg.AsksDir, brokerURL: cfg.BrokerURL, runID: cfg.RunID}
	if !cfg.SkipInitialPrune {
		claims.prune(reporter)
	}
	// Prune for as long as this consumer holds its lock, not for as long as ctx
	// lives: Consume returns on parent loss and terminal errors while ctx is
	// still open, and a pruner outliving the lock would contradict the
	// exclusivity the lock is taken for.
	stopPruner := startAskClaimPruner(ctx, claims, reporter)
	defer stopPruner()
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
	limit := cfg.Limit
	for alive() && ctx.Err() == nil {
		p, err := childmessaging.NewMessageProvider(cfg.Surface, childmessaging.Options{RunID: cfg.RunID, Token: cfg.Token, BrokerURL: cfg.BrokerURL})
		if err != nil {
			fmt.Fprintf(cfg.Log, "%s: invalid provider configuration\n", cfg.LogPrefix)
			return ErrInvalidProviderConfig
		}
		requestCtx, cancel := cfg.requestContext(ctx)
		err = p.Prepare(requestCtx)
		cancel()
		if !alive() || ctx.Err() != nil {
			_ = p.Close()
			return nil
		}
		for err == nil && alive() && ctx.Err() == nil {
			if cfg.BeforePoll != nil {
				if waitErr := cfg.BeforePoll(ctx); waitErr != nil {
					_ = p.Close()
					return nil
				}
			}
			requestCtx, cancel = cfg.requestContext(ctx)
			var result messaging.ReceiveResult
			result, err = p.Receive(requestCtx, messaging.ReceiveRequest{Generation: p.Generation(), Timeout: cfg.RequestTimeout, Limit: limit})
			cancel()
			if errors.Is(err, localhttp.ErrResponseTooLarge) {
				fmt.Fprintf(cfg.Log, "%s: drained batch exceeded response cap; rows lost (ids unknown)\n", cfg.LogPrefix)
				limit = 1
				err = nil
				continue
			}
			if err != nil {
				break
			}
			limit = cfg.Limit
			delivered := 0
			for i, row := range result.Messages {
				var decoded broker.AgentMessage
				if decodeErr := json.Unmarshal(row.Payload, &decoded); decodeErr != nil || decoded.Message == "" {
					logLost(cfg.Log, cfg.LogPrefix, result.Messages[i:i+1])
					continue
				}
				sender := row.FromRunID
				if sender == "" {
					sender = "unknown"
				}
				id := decoded.ID
				if id == "" {
					id = row.MessageID
				}
				msg := Message{ID: id, From: sender, Text: decoded.Message, ExpectsReply: decoded.ExpectsReply, Answerable: true}
				if decoded.ExpectsReply {
					if row.FromRunID == "" {
						msg.Answerable = false
					} else if recordErr := askclaim.Record(cfg.AsksDir, id, row.FromRunID); recordErr != nil {
						fmt.Fprintf(cfg.Log, "%s: failed to record ask id %q\n", cfg.LogPrefix, id)
						msg.Answerable = false
					}
				}
				if cfg.OnRow != nil {
					if rowErr := cfg.OnRow(msg); rowErr != nil {
						logLost(cfg.Log, cfg.LogPrefix, result.Messages[i:])
						_ = p.Close()
						return rowErr
					}
				}
				delivered++
			}
			if delivered > 0 && cfg.OnBatchDone != nil {
				cfg.OnBatchDone()
			}
		}
		_ = p.Close()
		if !alive() || ctx.Err() != nil {
			return nil
		}
		if terminal(err) {
			var status *localhttp.StatusError
			if errors.As(err, &status) {
				fmt.Fprintf(cfg.Log, "%s: terminal broker refusal (HTTP %d)\n", cfg.LogPrefix, status.Status)
			} else {
				fmt.Fprintf(cfg.Log, "%s: terminal broker protocol or identity error\n", cfg.LogPrefix)
			}
			wait(0)
			return nil
		}
		fmt.Fprintf(cfg.Log, "%s: temporary broker error; retrying\n", cfg.LogPrefix)
		if !wait(2 * time.Second) {
			return nil
		}
	}
	return nil
}

// requestContext bounds one request at cfg.RequestTimeout. A caller that
// abandons an in-flight request on shutdown derives it from ctx; one that
// drains the current batch derives it from the background so its own
// cancellation does not truncate a batch already on the wire.
func (cfg ConsumerConfig) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	parent := context.Background()
	if cfg.CancelInFlight {
		parent = ctx
	}
	return context.WithTimeout(parent, cfg.RequestTimeout)
}

// logLost reports the IDs of drained messages that could not be delivered.
func logLost(out io.Writer, prefix string, rows []messaging.InboundMessage) {
	for _, row := range rows {
		id := row.MessageID
		if id == "" {
			var payload struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(row.Payload, &payload)
			id = payload.ID
		}
		fmt.Fprintf(out, "%s: undelivered message id %q\n", prefix, id)
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

// askClaimPruneInterval paces the prune a running consumer performs. A consumer
// lives as long as its supervisor parent, so start-up pruning alone would leave
// every record written since then in place. The interval is a variable so a test
// can shorten it.
var askClaimPruneInterval = time.Hour

// askClaimPruner holds what one consumer prunes.
type askClaimPruner struct {
	asksDir   string
	brokerURL string
	runID     string
}

// prune removes expired records from the default root. It is housekeeping: a
// failure is reported and the consumer keeps running, which is why no caller
// branches on the error.
func (p askClaimPruner) prune(reporter *pruneReporter) {
	root, prune := askClaimPruneRoot(p.asksDir, appdir.StateHome(), p.brokerURL, p.runID)
	if !prune {
		return
	}
	if err := askclaim.PruneRoot(root, askclaim.AskClaimMaxAge); err != nil {
		reporter.report(root, err)
		return
	}
	reporter.forget(root)
}

// pruneReporter reports prune failures once per root and message. The prune
// runs hourly for the life of a consumer, and an unwritable root fails
// identically on every pass; repeating it would bury everything else in the
// log.
//
// It is a pointer-owned type rather than fields on the caller: consumers are
// passed by value, and a mutex inside one makes every copy a copylocks finding
// and every future method a cross-copy race.
type pruneReporter struct {
	mu     sync.Mutex
	logged map[string]string
	out    io.Writer
	prefix string
}

func (r *pruneReporter) report(root string, err error) {
	message := err.Error()
	r.mu.Lock()
	if r.logged == nil {
		r.logged = make(map[string]string)
	}
	if r.logged[root] == message {
		r.mu.Unlock()
		return
	}
	r.logged[root] = message
	r.mu.Unlock()
	prefix := r.prefix
	if prefix == "" {
		prefix = "feed broker"
	}
	fmt.Fprintf(r.out, "%s: prune ask claim records: %v\n", prefix, err)
}

// forget clears the suppression for a root that pruned cleanly, so a later
// failure is reported again.
func (r *pruneReporter) forget(root string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.logged, root)
}

// startAskClaimPruner prunes on each tick and returns a stop function that
// waits for the goroutine to exit. Waiting matters for an embedder whose ctx
// outlives the consumer.
//
// Consumers for different runs hold different locks (the key is the (broker,
// run) pair) and prune the same root concurrently. That is benign because every
// step of PruneRoot is best-effort and idempotent; it is not an invariant to
// lean on.
func startAskClaimPruner(ctx context.Context, claims askClaimPruner, reporter *pruneReporter) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(askClaimPruneInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				claims.prune(reporter)
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// askClaimPruneRoot reports the directory to prune, if any. The supervised poo
// path always passes --asks-dir, and it passes the default one, so keying on
// an empty flag would prune exactly the deployments that do not accumulate.
func askClaimPruneRoot(asksDir, stateHome, brokerURL, runID string) (string, bool) {
	defaultDir, err := askclaim.DefaultDir(stateHome, brokerURL, runID)
	if err != nil {
		return "", false
	}
	if asksDir != "" && asksDir != defaultDir {
		// A caller's own directory is the caller's to clean: pruning it would
		// mean deleting files under a path whose layout we merely assume.
		return "", false
	}
	return askclaim.DefaultRoot(stateHome), true
}
