package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/board"
)

func runBoard(args []string) int {
	fs := flag.NewFlagSet("board", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:0", "bind address (:0 = random port)")
	scan := fs.String("scan", "", "directories to scan for *.sock files (comma-separated, default: ~/.avenor/sockets)")
	socket := fs.String("socket", "", "single socket path; if set, --scan is ignored")
	token := fs.String("token", "", "auth token; required for non-loopback unless --no-auth is set")
	allowMutations := fs.Bool("allow-mutations", false, "enable POST /api/cancel and POST /api/prompt")
	noAuth := fs.Bool("no-auth", false, "disable auth entirely (allows remote access without token)")
	noOpen := fs.Bool("no-open", false, "do not open browser after starting")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	var scanDirs []string
	if *socket == "" {
		if *scan != "" {
			for _, d := range splitPathList(*scan) {
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
		Addr:           *addr,
		ScanDirs:       scanDirs,
		Socket:         board.ResolveHome(*socket),
		AllowMutations: *allowMutations,
		Token:          *token,
		NoAuth:         *noAuth,
		NoOpen:         *noOpen,
	}

	srv, err := board.NewServer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor board: %v\n", err)
		return 1
	}

	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "avenor board: %v\n", err)
		return 1
	}

	srv.PrintStartupInfo()

	if !*noOpen && hasDisplay() {
		url := fmt.Sprintf("http://%s/board/", srv.Addr())
		openBrowser(url)
	}

	// Wait for signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Fprintln(os.Stderr, "\navenor board: shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "avenor board: server shutdown: %v\n", err)
	}
	return 0
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