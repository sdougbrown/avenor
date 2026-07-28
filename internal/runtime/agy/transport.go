package agy

import (
	"context"
	"errors"
	"os"
	"reflect"

	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// transportKind is fixed for the lifetime of a logical session. A session
// never changes transport after registration.
type transportKind uint8

const (
	transportHeadless transportKind = iota
	transportRPC
)

const autoRPCFallbackDiagnostic = "rpc_probe_failed_fell_back_to_headless"

// rpcSessionHost is the provider-facing portion of a validated PTY/RPC host.
// Keeping this boundary narrow makes dispatch deterministic in provider tests;
// the production implementation is *ptyRPCHost.
type rpcSessionHost interface {
	ConversationID() string
	RunTurn(context.Context, string, string, func(events.Event)) error
	ArmCancellation()
	DisarmCancellation()
	Cancel(context.Context) error
	AnswerPermission(context.Context, string, runtime.PermissionResponse) error
	Close(context.Context) error
}

type rpcSessionHostFactory func(context.Context, runtime.StartOptions, string, string) (rpcSessionHost, error)

// transportSelection transfers ownership of rpc to the caller. A successful
// RPC selection retains it for the session lifetime; a cleanup failure returns
// it alongside the error so the Provider can keep retrying exact-host cleanup.
type transportSelection struct {
	kind transportKind
	rpc  rpcSessionHost
}

// selectTransport probes only when policy permits it. The optional factory
// seam is used by Provider tests; production supplies the PTY-backed factory.
// version is the already-validated version for this Provider invocation, not
// shared mutable process state.
func selectTransport(ctx context.Context, getenv func(string) string, opts runtime.StartOptions, resumeID, version string, factory rpcSessionHostFactory) (transportSelection, string, error) {
	if getenv == nil {
		getenv = os.Getenv
	}

	mode := getenv("AVENOR_AGY_TRANSPORT")
	switch mode {
	case "", "headless":
		return transportSelection{kind: transportHeadless}, "", nil
	case "rpc", "auto":
		var (
			host rpcSessionHost
			err  error
		)
		if factory == nil {
			err = errors.New("agy RPC host factory is unavailable")
		} else {
			host, err = factory(ctx, opts, resumeID, version)
			if err == nil && nilRPCSessionHost(host) {
				err = errors.New("agy RPC host startup failed")
			}
		}
		if err == nil {
			return transportSelection{kind: transportRPC, rpc: host}, "", nil
		}
		// A factory may have allocated its host before reporting a late probe
		// failure. It remains the caller's responsibility to clean it up.
		if !nilRPCSessionHost(host) {
			if closeErr := host.Close(context.Background()); closeErr != nil {
				if retryErr := host.Close(context.Background()); retryErr != nil {
					return transportSelection{rpc: host}, "", errors.New("agy RPC probe cleanup failed")
				}
			}
		}
		if mode == "auto" {
			// Do not retain or expose startup errors: they can include endpoint,
			// process, filesystem, or conversation data.
			return transportSelection{kind: transportHeadless}, autoRPCFallbackDiagnostic, nil
		}
		return transportSelection{}, "", err
	default:
		return transportSelection{}, "", errors.New("invalid AVENOR_AGY_TRANSPORT")
	}
}

// nilRPCSessionHost handles typed nil pointers returned through the interface
// factory boundary. Calling a method on such a host would panic the supervisor.
func nilRPCSessionHost(host rpcSessionHost) bool {
	if host == nil {
		return true
	}
	value := reflect.ValueOf(host)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
