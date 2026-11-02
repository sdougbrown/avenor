package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/digest"
	clicontract "github.com/sdougbrown/cli-contract"
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
	return runDeclared("watch", args, handleWatch)
}

// handleWatch digests one log file; follow mode owns its signal handling.
func handleWatch(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	follow := inv.Bools["follow"]
	format := orDefault(inv.Flags["format"], "plain")
	classify := inv.Bools["classify"]
	accumulate := inv.Bools["accumulate"]
	pollInterval := orDefault(inv.Durations["poll-interval"], 250*time.Millisecond)
	sinceCursor := inv.Flags["since-cursor"]

	if format != "plain" && format != "json" {
		return nil, usageErrorf("avenor watch: unsupported --format %q", format)
	}
	if len(inv.Args) != 1 {
		return nil, usageErrorf("avenor watch: expected exactly one <log>")
	}
	logPath := inv.Args[0]

	file, err := os.Open(logPath)
	if err != nil {
		return nil, usageErrorf("avenor watch: open %s: %v", logPath, err)
	}
	defer file.Close()

	// Handle --since-cursor: read the saved offset, validate it against the
	// current log size, then seek the file to that position.
	var cursorStartOffset int64
	if sinceCursor != "" {
		offset, cursorErr := readCursor(sinceCursor)
		if cursorErr != nil {
			return nil, usageErrorf("avenor watch: %v", cursorErr)
		}

		// Sanity-check: if the log has been rotated/truncated the saved offset
		// may be beyond the end of the file.
		info, statErr := file.Stat()
		if statErr != nil {
			return nil, usageErrorf("avenor watch: stat %s: %v", logPath, statErr)
		}
		if err := validateCursorAgainstLog(offset, info); err != nil {
			return nil, usageErrorf("avenor watch: %v (cursor %s)", err, sinceCursor)
		}

		if offset > 0 {
			if _, seekErr := file.Seek(offset, 0); seekErr != nil {
				return nil, usageErrorf("avenor watch: seek %s: %v", logPath, seekErr)
			}
		}
		cursorStartOffset = offset
	}

	out := bufio.NewWriter(os.Stdout)
	opts := digest.Options{
		Follow:            follow,
		PollInterval:      pollInterval,
		Format:            format,
		Classify:          classify,
		Accumulate:        accumulate,
		CursorPath:        sinceCursor,
		CursorStartOffset: cursorStartOffset,
	}

	if !follow {
		if err := digest.Stream(file, out, opts); err != nil {
			_ = out.Flush()
			return nil, fmt.Errorf("avenor watch: %w", err)
		}
		if err := out.Flush(); err != nil {
			return nil, fmt.Errorf("avenor watch: flush stdout: %w", err)
		}
		return nil, nil
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
			return nil, fmt.Errorf("avenor watch: flush stdout: %w", flushErr)
		}
		if err != nil {
			return nil, fmt.Errorf("avenor watch: %w", err)
		}
		return nil, nil
	case <-sigCh:
		_ = file.Close()
		var err error
		select {
		case err = <-errCh:
		case <-time.After(time.Second):
		}
		if flushErr := out.Flush(); flushErr != nil {
			return nil, fmt.Errorf("avenor watch: flush stdout: %w", flushErr)
		}
		if err != nil {
			return nil, fmt.Errorf("avenor watch: %w", err)
		}
		return nil, nil
	}
}
