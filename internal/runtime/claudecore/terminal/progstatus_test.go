package terminal

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type progRecorder struct {
	events []string
}

func (r *progRecorder) hooks() ProgStatusHooks {
	return ProgStatusHooks{
		OnQuery: func() { r.events = append(r.events, "query") },
		OnReport: func(s ProgStatus) {
			r.events = append(r.events, fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s",
				s.State, s.ID, s.Kind, s.Progress, s.App, s.Title, s.Msg))
		},
		OnReset: func() { r.events = append(r.events, "reset") },
	}
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func osc(body string) string { return "\x1b]7501;" + body + "\x07" }

func feedAll(chunks ...string) (*progRecorder, *ProgStatusStore) {
	rec := &progRecorder{}
	store := NewProgStatusStore(0)
	p := NewProgStatusParser(store, rec.hooks())
	for _, c := range chunks {
		p.Feed([]byte(c))
	}
	return rec, store
}

func TestProgStatusParserSplitPositions(t *testing.T) {
	stream := "\x1b[>0q\x1b[?u" + "\x1b]7501;?\x07" + "\x1b[c" +
		"\x1b]0;\xe2\x8f\xb3 Claude\x07" +
		"\x1b]7501;state=idle:app=claude-code\x07" +
		"\x1b(B\x0f\x1b7\xe2\x9d\xaf\x1b8" +
		"\x1b]7501;state=working:app=claude-code:id=a1:title=" + b64("Reply") + "\x1b\\" +
		"\x1bP+q]7501;state=done\x07\x1b\\" +
		"\x1b]7501;state=blocked:kind=permission:msg=" + b64("approve ❯") + "\x1b\\" +
		"\x1b]7501;state=clear:id=a1\x07" + "\x1bc"
	want := []string{
		"query",
		"idle|||-1|claude-code||",
		"working|a1||-1|claude-code|Reply|",
		"blocked||permission|-1|||approve ❯",
		"clear|a1||-1|||",
		"reset",
	}

	check := func(name string, chunks ...string) {
		t.Helper()
		rec, store := feedAll(chunks...)
		if !reflect.DeepEqual(rec.events, want) {
			t.Fatalf("%s: events\n got %q\nwant %q", name, rec.events, want)
		}
		if store.Len() != 0 {
			t.Fatalf("%s: store not emptied by RIS: %d", name, store.Len())
		}
	}
	check("whole", stream)
	for i := 0; i <= len(stream); i++ {
		check(fmt.Sprintf("split@%d", i), stream[:i], stream[i:])
	}
	bytewise := make([]string, len(stream))
	for i := range stream {
		bytewise[i] = stream[i : i+1]
	}
	check("bytewise", bytewise...)
}

func TestProgStatusParserQueryThenDA1(t *testing.T) {
	rec, _ := feedAll("\x1b[>0q\x1b[?u\x1b]7501;?\x1b\\\x1b[c")
	if !reflect.DeepEqual(rec.events, []string{"query"}) {
		t.Fatalf("events = %q", rec.events)
	}
	rec, _ = feedAll("\x1b]7501;?\x07")
	if !reflect.DeepEqual(rec.events, []string{"query"}) {
		t.Fatalf("BEL events = %q", rec.events)
	}
}

func TestProgStatusParserDiscardsInvalidReports(t *testing.T) {
	ctrl := b64("a\x01b")
	cases := map[string]string{
		"bad base64":         "state=done:msg=ab=c",
		"bad base64 bytes":   "state=done:msg=!!!!",
		"base64 length 1":    "state=done:msg=Q",
		"control in msg":     "state=done:msg=" + ctrl,
		"C1 in msg":          "state=done:msg=" + b64("a\u0085b"),
		"DEL in title":       "state=done:title=" + b64("a\x7fb"),
		"invalid utf8":       "state=done:msg=" + base64.StdEncoding.EncodeToString([]byte{0xff, 0xfe}),
		"title too long enc": "state=done:title=" + strings.Repeat("A", 257),
		"title too long dec": "state=done:title=" + b64(strings.Repeat("x", 193)),
		"msg too long enc":   "state=done:msg=" + strings.Repeat("A", 2733),
		"msg too long dec":   "state=done:msg=" + b64(strings.Repeat("x", 2049)),
		"bad id char":        "state=done:id=a b!",
		"bad id char only":   "state=working:id=a$",
		"empty id":           "state=done:id=",
		"empty segment":      "state=done:id=a//b",
		"trailing slash":     "state=done:id=a/",
		"long segment":       "state=done:id=" + strings.Repeat("a", 33),
		"too deep":           "state=done:id=a/b/c/d/e/f/g/h/i",
		"id too long":        "state=done:id=" + strings.Repeat("abcdefgh/", 14) + "abcdefgh",
		"key too long":       "state=done:abcdefghijklmnopq=1",
		"unknown state":      "state=sleeping",
		"missing state":      "app=claude-code",
		"empty payload":      "",
	}
	for name, body := range cases {
		rec, store := feedAll(osc(body))
		if len(rec.events) != 0 || store.Len() != 0 {
			t.Errorf("%s: accepted: %q", name, rec.events)
		}
	}

	// Oversized sequence is discarded whole and the stream resynchronises.
	big := "\x1b]7501;state=done:msg=" + strings.Repeat("A", 4100) + "\x07" + osc("state=idle")
	rec, _ := feedAll(big)
	if !reflect.DeepEqual(rec.events, []string{"idle|||-1|||"}) {
		t.Errorf("oversize: %q", rec.events)
	}

	// Exactly 4096 bytes passes, one more is dropped (BEL and ST terminators).
	pad := func(n int) string { return "state=done:x=" + strings.Repeat("a", n) }
	for _, tc := range []struct {
		term string
		fill int // payload bytes available for padding
	}{{"\x07", 4096 - 2 - 5 - 1}, {"\x1b\\", 4096 - 2 - 5 - 2}} {
		base := len("state=done:x=")
		ok, _ := feedAll("\x1b]7501;" + pad(tc.fill-base) + tc.term)
		over, _ := feedAll("\x1b]7501;" + pad(tc.fill-base+1) + tc.term)
		if len(ok.events) != 1 || len(over.events) != 0 {
			t.Errorf("limit term %q: ok=%d over=%d", tc.term, len(ok.events), len(over.events))
		}
	}
}

func TestProgStatusParserFieldRules(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"malformed pairs skipped", "state=working: noequals :=x:ba d=1:Key=1:progress=5!:app=claude-code", "working|||-1|claude-code||"},
		{"unknown keys ignored", "state=done:extra=1:another=zz", "done|||-1|||"},
		{"last wins", "state=idle:state=error:app=a:app=b", "error|||-1|b||"},
		{"whitespace trimmed", " state = done : app = x ", "done|||-1|x||"},
		{"kind with blocked", "state=blocked:kind=auth", "blocked||auth|-1|||"},
		{"kind unknown", "state=blocked:kind=other", "blocked|||-1|||"},
		{"kind ignored unless blocked", "state=working:kind=permission", "working|||-1|||"},
		{"progress working", "state=working:progress=42", "working|||42|||"},
		{"progress blocked", "state=blocked:progress=100", "blocked|||100|||"},
		{"progress ignored when done", "state=done:progress=42", "done|||-1|||"},
		{"progress out of range", "state=working:progress=101", "working|||-1|||"},
		{"progress negative", "state=working:progress=-1", "working|||-1|||"},
		{"progress non numeric", "state=working:progress=1.5", "working|||-1|||"},
		{"app invalid absent", "state=done:app=" + strings.Repeat("a", 33), "done|||-1|||"},
		{"nested id", "state=working:id=a/b.c/d-1", "working|a/b.c/d-1||-1|||"},
		{"unpadded base64", "state=done:title=" + strings.TrimRight(b64("ab"), "="), "done|||-1||ab|"},
		{"padded base64", "state=done:title=" + b64("ab"), "done|||-1||ab|"},
		{"empty title", "state=done:title=", "done|||-1|||"},
		{"title at limit", "state=done:title=" + b64(strings.Repeat("x", 192)), "done|||-1||" + strings.Repeat("x", 192) + "|"},
	}
	for _, tc := range cases {
		rec, _ := feedAll(osc(tc.body))
		if !reflect.DeepEqual(rec.events, []string{tc.want}) {
			t.Errorf("%s: got %q want %q", tc.name, rec.events, tc.want)
		}
	}
}

func TestProgStatusParserSkipsStringSequences(t *testing.T) {
	for _, intro := range []string{"\x1bP", "\x1bX", "\x1b^", "\x1b_"} {
		rec, store := feedAll(intro + "body]7501;state=working\x07tail\x1b\\" + osc("state=idle"))
		if !reflect.DeepEqual(rec.events, []string{"idle|||-1|||"}) || store.Len() != 1 {
			t.Errorf("%q: %q", intro, rec.events)
		}
	}
}

func TestProgStatusParserUnterminatedStringDoesNotHideReports(t *testing.T) {
	for _, intro := range []string{"\x1bP", "\x1bX", "\x1b^", "\x1b_"} {
		rec, _ := feedAll(intro+"never terminated", osc("state=done"))
		if !reflect.DeepEqual(rec.events, []string{"done|||-1|||"}) {
			t.Errorf("%q: %q", intro, rec.events)
		}
	}
}

func TestProgStatusParserAbortsAndResync(t *testing.T) {
	cases := map[string]string{
		"CAN in OSC":         "\x1b]7501;state=working\x18" + "junk\x07",
		"SUB in OSC":         "\x1b]7501;state=working\x1a" + "junk\x07",
		"ESC not ST in OSC":  "\x1b]7501;state=working\x1bjunk\x07",
		"CAN in DCS":         "\x1bP\x18" + "\x1b]7501;state=working\x07",
		"CAN in CSI":         "\x1b[1;\x18" + "\x1b]7501;state=working\x07",
		"truncated by ESC ]": "\x1b]7501;state=work\x1b]0;t\x07",
	}
	for name, abort := range cases {
		rec, _ := feedAll(abort + osc("state=idle"))
		got := rec.events
		// The CAN-in-DCS/CSI cases leave the following OSC live; they are
		// checked below separately.
		if strings.HasPrefix(name, "CAN in DCS") || strings.HasPrefix(name, "CAN in CSI") {
			if !reflect.DeepEqual(got, []string{"working|||-1|||", "idle|||-1|||"}) {
				t.Errorf("%s: %q", name, got)
			}
			continue
		}
		if !reflect.DeepEqual(got, []string{"idle|||-1|||"}) {
			t.Errorf("%s: %q", name, got)
		}
	}
	// ESC aborting an OSC starts a new escape that can itself be an OSC 7501.
	rec, _ := feedAll("\x1b]0;title" + osc("state=idle"))
	if !reflect.DeepEqual(rec.events, []string{"idle|||-1|||"}) {
		t.Errorf("OSC 0 aborted by new OSC: %q", rec.events)
	}
}

func TestProgStatusParserUTF8NotC1(t *testing.T) {
	// 0x9D (C1 OSC) appears inside the UTF-8 encoding of the prompt glyph.
	rec, _ := feedAll("\xe2\x9d\xaf \x9d7501;state=working\x07 \xe2\x9d\xaf" + osc("state=idle") + "\x1b[1m\xe2\x9d\xaf")
	if !reflect.DeepEqual(rec.events, []string{"idle|||-1|||"}) {
		t.Errorf("events = %q", rec.events)
	}
}

func TestProgStatusParserNonProgOSCsAndCharsets(t *testing.T) {
	stream := "\x1b]0;title\x07\x1b]7502;x\x07\x1b]75011;state=done\x07\x1b]7501\x07" +
		"\x1b(B\x1b)0\x1b*B\x1b+B\x1b=\x1b>\x1b7\x1b8\x0f" + osc("state=done")
	rec, _ := feedAll(stream)
	if !reflect.DeepEqual(rec.events, []string{"done|||-1|||"}) {
		t.Errorf("events = %q", rec.events)
	}
	// A charset designator consumes the byte after it, even an 'c'.
	rec, _ = feedAll(osc("state=done") + "\x1b(c" + "\x1b)c")
	if !reflect.DeepEqual(rec.events, []string{"done|||-1|||"}) {
		t.Errorf("charset c treated as RIS: %q", rec.events)
	}
}

func TestProgStatusParserDoesNotMutateInput(t *testing.T) {
	in := []byte("\x1b]7501;state=done:msg=" + b64("hi") + "\x07\x1bc")
	orig := append([]byte(nil), in...)
	NewProgStatusParser(NewProgStatusStore(0), ProgStatusHooks{}).Feed(in)
	if string(in) != string(orig) {
		t.Fatal("input modified")
	}
}

func TestProgStatusParserNilStoreAndHooks(t *testing.T) {
	p := NewProgStatusParser(nil, ProgStatusHooks{})
	p.Feed([]byte("\x1b]7501;?\x07" + osc("state=done") + "\x1bc"))
}

func TestProgStatusStoreReplaceAndClear(t *testing.T) {
	_, s := feedAll(
		osc("state=working:app=claude-code:progress=10:title="+b64("t")),
		osc("state=working:id=a:app=x"),
		osc("state=working:id=a/b"),
		osc("state=working:id=a/b/c"),
		osc("state=working:id=ab"),
		osc("state=working:id=z"),
	)
	// Replacement is complete: fields not re-sent are dropped.
	_, s2 := feedAll(
		osc("state=working:progress=10:title="+b64("t")+":msg="+b64("m")),
		osc("state=done"),
	)
	r, ok := s2.Root()
	if !ok || r.State != ProgDone || r.Progress != -1 || r.Title != "" || r.Msg != "" {
		t.Fatalf("not replaced: %+v", r)
	}

	if s.Len() != 6 {
		t.Fatalf("len = %d", s.Len())
	}
	s.Clear("a")
	ids := idsOf(s)
	if !reflect.DeepEqual(ids, []string{"", "ab", "z"}) {
		t.Fatalf("after clear a: %q", ids)
	}
	s.Clear("missing")
	if s.Len() != 3 {
		t.Fatal("clearing a missing id changed the store")
	}
	rec, s3 := feedAll(osc("state=working"), osc("state=working:id=a"), osc("state=clear"))
	if s3.Len() != 0 || rec.events[2] != "clear|||-1|||" {
		t.Fatalf("clear-all: len=%d events=%q", s3.Len(), rec.events)
	}
	_, s4 := feedAll(osc("state=working"), osc("state=working:id=a"), "\x1b[!p")
	if s4.Len() != 2 {
		t.Fatal("DECSTR must not clear records")
	}
	_, s5 := feedAll(osc("state=working"), osc("state=working:id=a"), "\x1bc")
	if s5.Len() != 0 {
		t.Fatal("RIS must clear records")
	}
}

func TestProgStatusStoreAppInheritance(t *testing.T) {
	_, s := feedAll(
		osc("state=working:app=claude-code"),
		osc("state=working:id=a"),
		osc("state=working:id=a/b"),
		osc("state=working:id=a/b/c:app=other"),
		osc("state=working:id=a/b/c/d"),
		osc("state=working:id=solo/x"),
	)
	want := map[string]string{"": "claude-code", "a": "claude-code", "a/b": "claude-code",
		"a/b/c": "other", "a/b/c/d": "other", "solo/x": "claude-code"}
	for id, app := range want {
		r, ok := s.Get(id)
		if !ok || r.App != app {
			t.Errorf("Get(%q) = %+v, %v; want app %q", id, r, ok, app)
		}
	}
	for _, r := range s.Snapshot() {
		if r.App != want[r.ID] {
			t.Errorf("snapshot %q app = %q", r.ID, r.App)
		}
	}
	if _, ok := s.Get("nope"); ok {
		t.Error("Get of missing id succeeded")
	}
}

func TestProgStatusStoreEvictsLeastRecentlyUpdated(t *testing.T) {
	s := NewProgStatusStore(3)
	apply := func(id string) { s.Apply(ProgStatus{ID: id, State: ProgWorking, Progress: -1}) }
	apply("a")
	apply("b")
	apply("c")
	apply("a") // refresh a; b is now oldest
	apply("d")
	if got := idsOf(s); !reflect.DeepEqual(got, []string{"a", "c", "d"}) {
		t.Fatalf("ids = %q", got)
	}
	apply("c") // update in place never evicts
	if s.Len() != 3 {
		t.Fatalf("len = %d", s.Len())
	}

	big := NewProgStatusStore(0)
	for i := 0; i < DefaultMaxProgRecords+10; i++ {
		big.Apply(ProgStatus{ID: fmt.Sprintf("r%d", i), State: ProgIdle})
	}
	if big.Len() != DefaultMaxProgRecords {
		t.Fatalf("default cap: len = %d", big.Len())
	}
	if _, ok := big.Get("r0"); ok {
		t.Fatal("oldest survived")
	}
}

func TestProgStatusStoreDropOnExit(t *testing.T) {
	_, s := feedAll(
		osc("state=working"),
		osc("state=blocked:id=b"),
		osc("state=idle:id=i"),
		osc("state=done:id=d"),
		osc("state=error:id=e"),
	)
	s.DropOnExit()
	if got := idsOf(s); !reflect.DeepEqual(got, []string{"d", "e"}) {
		t.Fatalf("ids = %q", got)
	}
}

func idsOf(s *ProgStatusStore) []string {
	var ids []string
	for _, r := range s.Snapshot() {
		ids = append(ids, r.ID)
	}
	return ids
}

func TestProgStatusRealCaptures(t *testing.T) {
	const claude = "claude-code"
	cases := []struct {
		file string
		want []string
	}{
		{"turn-startup.bin", []string{
			"query",
			"idle|||-1|" + claude + "||",
			"working|||-1|" + claude + "||",
		}},
		{"turn-done.bin", []string{
			"done|||-1|" + claude + "||",
			"working|||-1|" + claude + "||",
		}},
		{"turn-clear.bin", []string{"clear|||-1|||"}},
		{"perm-blocked.bin", []string{
			"blocked||permission|-1|" + claude + "||approve Bash: echo git push --force",
		}},
		{"question-blocked.bin", []string{
			"blocked||question|-1|" + claude + "||answer: Do you prefer red or blue? (Red · Blue)",
		}},
		{"subagent-start.bin", []string{
			"working|aafef3a855645ab5a||-1|" + claude + "|Reply with OK|",
		}},
		{"subagent-clear.bin", []string{"clear|aafef3a855645ab5a||-1|||"}},
		{"cancel-idle.bin", []string{"idle|||-1|" + claude + "||"}},
	}
	for _, tc := range cases {
		data, err := os.ReadFile(filepath.Join("testdata", "osc7501", tc.file))
		if err != nil {
			t.Fatal(err)
		}
		whole := &progRecorder{}
		NewProgStatusParser(NewProgStatusStore(0), whole.hooks()).Feed(data)
		if !reflect.DeepEqual(whole.events, tc.want) {
			t.Errorf("%s:\n got %q\nwant %q", tc.file, whole.events, tc.want)
		}
		for i := 1; i < len(data); i++ {
			rec := &progRecorder{}
			p := NewProgStatusParser(NewProgStatusStore(0), rec.hooks())
			p.Feed(data[:i])
			p.Feed(data[i:])
			if !reflect.DeepEqual(rec.events, tc.want) {
				t.Fatalf("%s split@%d: %q", tc.file, i, rec.events)
			}
		}
	}
}
