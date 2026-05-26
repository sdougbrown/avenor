package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/digest"
)

// readCursor reads the byte offset stored in the cursor file at path.
// If the file does not exist, it returns (0, nil) — start from the beginning.
// If the file exists but is not parseable, it returns an error.
func readCursor(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read cursor file %s: %w", path, err)
	}
	s := strings.TrimRight(string(data), "\n")
	n, parseErr := strconv.ParseInt(s, 10, 64)
	if parseErr != nil {
		return 0, fmt.Errorf("invalid cursor file %s: %w", path, parseErr)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid cursor file %s: negative offset %d", path, n)
	}
	return n, nil
}

// validateCursorAgainstLog returns an error if the cursor offset is beyond the
// end of the log file (i.e. the log was rotated or truncated). An offset equal
// to the file size is valid (cursor points at EOF, nothing new to read).
func validateCursorAgainstLog(offset int64, info os.FileInfo) error {
	if offset > info.Size() {
		return fmt.Errorf("log %s shorter than cursor (offset %d > size %d)",
			info.Name(), offset, info.Size())
	}
	return nil
}

func runWatch(args []string) int {
	fs := flag.NewFlagSet("avenor watch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	follow := fs.Bool("follow", false, "poll and tail the log")
	format := fs.String("format", "plain", "output format: plain (EVENT lines) or json (pass-through NDJSON)")
	classify := fs.Bool("classify", false, "prefix each plain-format digest line with MILESTONE | FINDING | ACTIVITY; in json format, adds a top-level \"classify\" field")
	accumulate := fs.Bool("accumulate", false, "buffer consecutive thought/message chunks with the same session ID and event type, flushing them as a single human-readable line (plain format only)")
	pollInterval := fs.Duration("poll-interval", 250*time.Millisecond, "follow-mode sleep interval")
	sinceCursor := fs.String("since-cursor", "", "cursor file path: seek to saved offset before reading; rewrite offset on exit")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "avenor watch: <log> is required")
		return 2
	}
	if fs.NArg() > 1 {
		fmt.Fprintln(os.Stderr, "avenor watch: expected exactly one <log>")
		return 2
	}
	if *format != "plain" && *format != "json" {
		fmt.Fprintf(os.Stderr, "avenor watch: unsupported --format %q\n", *format)
		return 2
	}

	logPath := fs.Arg(0)

	file, err := os.Open(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "avenor watch: open %s: %v\n", logPath, err)
		return 2
	}
	defer file.Close()

	// Handle --since-cursor: read the saved offset, validate it against the
	// current log size, then seek the file to that position.
	var cursorStartOffset int64
	if *sinceCursor != "" {
		offset, cursorErr := readCursor(*sinceCursor)
		if cursorErr != nil {
			fmt.Fprintf(os.Stderr, "avenor watch: %v\n", cursorErr)
			return 2
		}

		// Sanity-check: if the log has been rotated/truncated the saved offset
		// may be beyond the end of the file.
		info, statErr := file.Stat()
		if statErr != nil {
			fmt.Fprintf(os.Stderr, "avenor watch: stat %s: %v\n", logPath, statErr)
			return 2
		}
		if err := validateCursorAgainstLog(offset, info); err != nil {
			fmt.Fprintf(os.Stderr, "avenor watch: %v (cursor %s)\n", err, *sinceCursor)
			return 2
		}

		if offset > 0 {
			if _, seekErr := file.Seek(offset, 0); seekErr != nil {
				fmt.Fprintf(os.Stderr, "avenor watch: seek %s: %v\n", logPath, seekErr)
				return 2
			}
		}
		cursorStartOffset = offset
	}

	out := bufio.NewWriter(os.Stdout)
	opts := digest.Options{
		Follow:            *follow,
		PollInterval:      *pollInterval,
		Format:            *format,
		Classify:          *classify,
		Accumulate:        *accumulate,
		CursorPath:        *sinceCursor,
		CursorStartOffset: cursorStartOffset,
	}

	if !*follow {
		if err := digest.Stream(file, out, opts); err != nil {
			_ = out.Flush()
			fmt.Fprintf(os.Stderr, "avenor watch: %v\n", err)
			return 1
		}
		if err := out.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "avenor watch: flush stdout: %v\n", err)
			return 1
		}
		return 0
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- digest.Stream(file, out, opts)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-errCh:
		if flushErr := out.Flush(); flushErr != nil {
			fmt.Fprintf(os.Stderr, "avenor watch: flush stdout: %v\n", flushErr)
			return 1
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "avenor watch: %v\n", err)
			return 1
		}
		return 0
	case <-sigCh:
		_ = file.Close()
		var err error
		select {
		case err = <-errCh:
		case <-time.After(time.Second):
		}
		if flushErr := out.Flush(); flushErr != nil {
			fmt.Fprintf(os.Stderr, "avenor watch: flush stdout: %v\n", flushErr)
			return 1
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "avenor watch: %v\n", err)
			return 1
		}
		return 0
	}
}
