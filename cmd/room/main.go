// Command room is the two-headed (N-headed) interactive room: a human
// operator and persistent agent heads in one shared workspace, riding on an
// avenor stable supervisor.
//
// Start a stable supervisor first:
//
//	avenor stable --control-socket /tmp/room/avenor.sock --parked-timeout 0
//
// Then run the room (heads default to two pi sessions on local models):
//
//	go run ./cmd/room --socket /tmp/room/avenor.sock --dir . \
//	  --head a=sparky/gemma4:26b --head b=sparky/qwen3.8:27b --thinking off
//
// Unqualified input fans out to every head. "@a ...", "@b ..." route to
// specific heads. "/quit" shuts the supervisor down gracefully.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/sdougbrown/avenor/client"
	"github.com/sdougbrown/avenor/internal/room"
)

func main() {
	socket := flag.String("socket", "", "avenor stable control socket (required)")
	dir := flag.String("dir", ".", "shared workspace directory")
	thinking := flag.String("thinking", "off", "run-level thinking level for every head")
	maxDepth := flag.Int("max-depth", 2, "maximum peer-causal hops per operator turn")
	maxAuto := flag.Int("max-auto", 8, "maximum automatic activations per operator turn")
	excerpt := flag.Int("excerpt", 1200, "character bound for peer-facing excerpts")
	heads := headsFlag{}
	flag.Var(&heads, "head", "head spec name=model[@backend], repeatable; default a=sparky/gemma4:26b, b=sparky/qwen3.8:27b")
	flag.Parse()

	if *socket == "" {
		fmt.Fprintln(os.Stderr, "--socket is required")
		os.Exit(2)
	}
	specs := headSpecs(heads, *thinking)

	c, err := client.Dial(*socket)
	if err != nil {
		fatal(err)
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if os.Getenv("ROOM_DEBUG") != "" {
		sigCh := make(chan os.Signal, 8)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
		go func() {
			for s := range sigCh {
				fmt.Fprintf(os.Stderr, "room-debug: received signal %v\n", s)
			}
		}()
	}

	if os.Getenv("ROOM_DEBUG") != "" {
		go func() {
			<-ctx.Done()
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			fmt.Fprintf(os.Stderr, "room-debug: ctx canceled; stacks:\n%s\n", buf[:n])
		}()
	}

	r, err := room.New(c, room.Options{
		Dir:          *dir,
		ExcerptLimit: *excerpt,
		MaxDepth:     *maxDepth,
		MaxAuto:      *maxAuto,
	})
	if err != nil {
		fatal(err)
	}
	fmt.Printf("room: starting heads %v in %s (log: %s)\n", headNames(specs), *dir, r.LogPath())
	if err := r.Start(ctx, specs); err != nil {
		fatal(err)
	}
	fmt.Printf("room: ready. type a prompt (unqualified = all heads; @name targets one; /quit exits)\n")

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for {
		fmt.Print("you> ")
		if !in.Scan() {
			break
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		if line == "/quit" || line == "/exit" {
			break
		}
		text, targets := route(line, headNames(specs))
		digest, err := r.HumanTurn(ctx, text, targets)
		if err != nil {
			fmt.Printf("room: error: %v\n", err)
			continue
		}
		fmt.Print(digest)
	}
	fmt.Println("room: shutting down supervisor")
	_ = c.Shutdown("graceful")
}

// route splits leading @name tokens from the prompt text. Unknown names are
// reported and dropped rather than silently ignored.
func route(line string, valid []string) (string, []string) {
	var targets []string
	for {
		if !strings.HasPrefix(line, "@") {
			break
		}
		name, rest, _ := strings.Cut(line[1:], " ")
		if name == "" {
			break
		}
		if !contains(valid, name) {
			fmt.Printf("room: no head named %q (have %v); ignoring\n", name, valid)
		} else {
			targets = append(targets, name)
		}
		line = strings.TrimSpace(rest)
	}
	return line, targets
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

type headsFlag struct{ specs map[string]string }

func (h *headsFlag) String() string {
	if h.specs == nil {
		return ""
	}
	var parts []string
	for k, v := range h.specs {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

func (h *headsFlag) Set(s string) error {
	if h.specs == nil {
		h.specs = map[string]string{}
	}
	name, model, ok := strings.Cut(s, "=")
	if !ok || name == "" || model == "" {
		return fmt.Errorf("head spec must be name=model, got %q", s)
	}
	h.specs[name] = model
	return nil
}

func headSpecs(h headsFlag, thinking string) []room.HeadSpec {
	if len(h.specs) == 0 {
		h.specs = map[string]string{
			"a": "sparky/gemma4:26b",
			"b": "sparky/qwen3.8:27b",
		}
	}
	// Deterministic order: sort names.
	names := make([]string, 0, len(h.specs))
	for n := range h.specs {
		names = append(names, n)
	}
	sortNames(names)
	specs := make([]room.HeadSpec, 0, len(names))
	for _, n := range names {
		sp := headSpec(n, h.specs[n])
		sp.Thinking = thinking
		specs = append(specs, sp)
	}
	return specs
}

// headSpec splits "model" or "model@backend"; default backend is pi.
func headSpec(name, spec string) room.HeadSpec {
	model, backend, has := strings.Cut(spec, "@")
	if !has {
		backend = "pi"
	}
	return room.HeadSpec{Name: name, Backend: backend, Model: model}
}

func headNames(specs []room.HeadSpec) []string {
	out := make([]string, len(specs))
	for i, s := range specs {
		out[i] = s.Name
	}
	return out
}

func sortNames(names []string) {
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "room:", err)
	os.Exit(1)
}
