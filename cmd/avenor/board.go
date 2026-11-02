package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/board"
	"github.com/sdougbrown/cli-contract"
)

func runBoard(args []string) int {
	return runDeclared("board", args, handleBoard)
}

// handleBoard serves the board UI until a signal arrives; the handler owns
// signal handling and shuts down gracefully.
func handleBoard(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	socket := inv.Flags["socket"]

	var scanDirs []string
	if socket == "" {
		if scan := inv.Flags["scan"]; scan != "" {
			for _, d := range splitPathList(scan) {
				scanDirs = append(scanDirs, board.ResolveHome(d))
			}
		}
		// If no --scan and no --socket, default to ~/.avenor/sockets
		if len(scanDirs) == 0 {
			home, _ := os.UserHomeDir()
			scanDirs = []string{filepath.Join(home, ".avenor", "sockets")}
		}
	}

	cfg := board.Config{
		Addr:           orDefault(inv.Flags["addr"], "127.0.0.1:0"),
		ScanDirs:       scanDirs,
		Socket:         board.ResolveHome(socket),
		AllowMutations: inv.Bools["allow-mutations"],
		Token:          inv.Flags["token"],
		NoAuth:         inv.Bools["no-auth"],
		NoOpen:         inv.Bools["no-open"],
	}

	srv, err := board.NewServer(cfg)
	if err != nil {
		return nil, fmt.Errorf("avenor board: %w", err)
	}

	if err := srv.Start(); err != nil {
		return nil, fmt.Errorf("avenor board: %w", err)
	}

	srv.PrintStartupInfo()

	if !inv.Bools["no-open"] && hasDisplay() {
		url := fmt.Sprintf("http://%s/board/", srv.Addr())
		openBrowser(url)
	}

	// Wait for signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Fprintln(os.Stderr, "\navenor board: shutting down...")
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		fmt.Fprintf(os.Stderr, "avenor board: server shutdown: %v\n", err)
	}
	return exitCode(0), nil
}

// hasDisplay returns true when a graphical display is available (X11 or
// Wayland). On headless machines (CI servers, Tailscale boxes), this returns
// false so the browser open step is silently skipped.
func hasDisplay() bool {
	if os.Getenv("DISPLAY") != "" {
		return true
	}
	if os.Getenv("WAYLAND_DISPLAY") != "" {
		return true
	}
	return false
}

func openBrowser(url string) {
	var cmd string
	switch {
	case lookPath("xdg-open") != "":
		cmd = lookPath("xdg-open")
	case lookPath("open") != "":
		cmd = lookPath("open")
	default:
		return // headless — no browser opener available
	}
	p, err := os.StartProcess(cmd, []string{cmd, url}, &os.ProcAttr{})
	if err != nil {
		// Not worth failing over — just log to stderr
		fmt.Fprintf(os.Stderr, "avenor board: could not open browser: %v\n", err)
		return
	}
	_ = p.Release()
}

func lookPath(name string) string {
	path, err := exec.LookPath(name)
	if err != nil {
		return ""
	}
	return path
}

func splitPathList(s string) []string {
	var result []string
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}
