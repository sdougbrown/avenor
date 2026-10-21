//go:build linux || darwin || freebsd

package terminal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func startTestPTY(t *testing.T, command string, answer bool) *PTYSession {
	t.Helper()
	session, err := (PTYLauncher{AnswerProgStatus: answer}).Start(context.Background(), StartOptions{Command: command, Cols: 120, Rows: 20})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	p := session.(*PTYSession)
	t.Cleanup(func() {
		_ = p.Kill(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Wait(ctx)
	})
	return p
}

func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func screenContains(p *PTYSession, s string) func() bool {
	return func() bool {
		out, _ := p.Capture(context.Background())
		return strings.Contains(out, s)
	}
}

// replyHex is progStatusReply as `od -An -tx1` prints it.
const replyHex = "1b 5d 37 35 30 31 3b 3f 1b 5c"

func TestPTYAnswersProgStatusQuerySplitAcrossReads(t *testing.T) {
	const script = `stty raw -echo; printf '\033]75'; sleep 0.2; printf '01;?\007'; head -c 10 | od -An -tx1; printf END; sleep 30`
	t.Run("answering", func(t *testing.T) {
		p := startTestPTY(t, script, true)
		waitUntil(t, 5*time.Second, "child to echo the reply", screenContains(p, "END"))
		if out, _ := p.Capture(context.Background()); !strings.Contains(strings.Join(strings.Fields(out), " "), replyHex+" END") {
			t.Fatalf("child received something other than the reply:\n%s", out)
		}
		if !p.ProgStatusAnswered() {
			t.Fatal("ProgStatusAnswered = false after the reply was written")
		}
	})
	t.Run("not answering", func(t *testing.T) {
		p := startTestPTY(t, script, false)
		time.Sleep(700 * time.Millisecond)
		if out, _ := p.Capture(context.Background()); strings.Contains(out, "END") {
			t.Fatalf("child received input although answering is off:\n%s", out)
		}
		if p.ProgStatusAnswered() {
			t.Fatal("ProgStatusAnswered = true with answering off")
		}
	})
}

func TestPTYReplyNeverLandsInsideAPaste(t *testing.T) {
	out := filepath.Join(t.TempDir(), "stdin")
	// The child asks for detection only once it has read part of the paste,
	// so the reply is queued while the paste is still being written.
	script := `stty raw -echo; printf READY; dd bs=1 count=1000 of='` + out + `' 2>/dev/null; printf '\033]7501;?\007'; exec cat >> '` + out + `'`
	p := startTestPTY(t, script, true)
	waitUntil(t, 5*time.Second, "child to enter raw mode", screenContains(p, "READY"))

	text := strings.Repeat("0123456789", 20000)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.PasteAndEnter(ctx, text); err != nil {
		t.Fatalf("PasteAndEnter: %v", err)
	}
	want := string(bracketedPaste(text)) + "\r" + progStatusReply
	waitUntil(t, 10*time.Second, "child to read paste and reply", func() bool {
		fi, err := os.Stat(out)
		return err == nil && fi.Size() >= int64(len(want))
	})
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		i := 0
		for i < len(got) && i < len(want) && got[i] == want[i] {
			i++
		}
		t.Fatalf("child input differs at byte %d of %d (got %d bytes): got %q, want %q",
			i, len(want), len(got), clip(string(got[i:])), clip(want[i:]))
	}
}

func clip(s string) string {
	if len(s) > 40 {
		return s[:40]
	}
	return s
}

func TestPTYProgStatusLeavesScreenUnchanged(t *testing.T) {
	plain := startTestPTY(t, `printf 'hello world!'; sleep 30`, false)
	reporting := startTestPTY(t, `printf 'hello\033]7501;state=idle:app=claude-code\007 world\033]7501;state=working:id=a1\033\\!'; sleep 30`, false)
	waitUntil(t, 5*time.Second, "plain output", screenContains(plain, "world!"))
	waitUntil(t, 5*time.Second, "reporting output", screenContains(reporting, "world!"))
	a, _ := plain.Capture(context.Background())
	b, _ := reporting.Capture(context.Background())
	if a != b {
		t.Fatalf("screen differs with OSC 7501 traffic:\nplain:\n%q\nreporting:\n%q", a, b)
	}
}

func TestPTYRetainsReportsSentBeforeAnyoneAsks(t *testing.T) {
	p := startTestPTY(t, `printf '\033]7501;state=idle:app=claude-code\007\033]7501;state=working:id=a1\007'; sleep 30`, false)
	waitUntil(t, 5*time.Second, "reports", func() bool {
		reports, _ := p.ProgStatusSince(0)
		return len(reports) == 2
	})
	select {
	case <-p.ProgStatusChanged():
	default:
		t.Fatal("no pending change notification for reports logged before subscribing")
	}
	root, ok := p.ProgStatusRoot()
	if !ok || root.State != ProgIdle || root.App != "claude-code" {
		t.Fatalf("root = %+v, %v; want idle from claude-code", root, ok)
	}
	reports, lost := p.ProgStatusSince(0)
	if lost || reports[0].Seq != 1 || reports[0].Status.ID != "" || reports[1].Status.ID != "a1" || reports[1].Status.State != ProgWorking {
		t.Fatalf("reports = %+v (lost=%v); want root idle then a1 working", reports, lost)
	}
	if later, _ := p.ProgStatusSince(reports[1].Seq); len(later) != 0 {
		t.Fatalf("ProgStatusSince(last) = %+v, want none", later)
	}
}

// floodScript never reads input and writes output, detection queries and
// reports as fast as it can.
const floodScript = `stty raw -echo; while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\033]7501;?\007\033]7501;state=working\007'; done`

func TestPTYReaderKeepsDrainingWhileAWriteIsBlocked(t *testing.T) {
	p := startTestPTY(t, floodScript, true)
	waitUntil(t, 5*time.Second, "reports to flow", func() bool {
		reports, _ := p.ProgStatusSince(0)
		return len(reports) > 0
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := p.PasteAndEnter(ctx, strings.Repeat("y", 1<<20))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PasteAndEnter to a child that never reads = %v, want deadline exceeded", err)
	}

	before := latestSeq(p)
	captured := make(chan struct{})
	go func() {
		_, _ = p.Capture(context.Background())
		close(captured)
	}()
	select {
	case <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("Capture blocked while a write was stuck")
	}
	waitUntil(t, 2*time.Second, "reader to keep consuming reports", func() bool { return latestSeq(p) > before })

	if err := p.Kill(context.Background()); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	assertWriterStops(t, p)
}

func TestPTYKillFailsPendingWrites(t *testing.T) {
	p := startTestPTY(t, `stty raw -echo; printf READY; exec sleep 30`, false)
	waitUntil(t, 5*time.Second, "child to enter raw mode", screenContains(p, "READY"))

	results := make(chan error, 2)
	go func() { results <- p.PasteAndEnter(context.Background(), strings.Repeat("y", 1<<20)) }()
	// Give the paste time to fill the input queue, then queue keys behind it.
	time.Sleep(200 * time.Millisecond)
	go func() { results <- p.SendKeys(context.Background(), KeyEnter) }()
	time.Sleep(100 * time.Millisecond)

	if err := p.Kill(context.Background()); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	for range 2 {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("pending write succeeded after Kill")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("pending write still blocked after Kill")
		}
	}
	assertWriterStops(t, p)
	if err := p.SendKeys(context.Background(), KeyEnter); err == nil {
		t.Fatal("SendKeys after Kill succeeded")
	}
}

func latestSeq(p *PTYSession) uint64 {
	reports, _ := p.ProgStatusSince(0)
	if len(reports) == 0 {
		return 0
	}
	return reports[len(reports)-1].Seq
}

// assertWriterStops checks that the writer goroutine returned, which needs its
// blocked write to have failed once the child's side of the pty went away.
func assertWriterStops(t *testing.T, p *PTYSession) {
	t.Helper()
	select {
	case <-p.writerDone:
	case <-time.After(3 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Logf("%s", buf[:n])
		t.Fatal("writer goroutine still running after Kill")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = p.Wait(ctx)
	if ctx.Err() != nil {
		t.Fatal("Wait did not return after Kill")
	}
}
