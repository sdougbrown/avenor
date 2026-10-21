package terminal

import (
	"encoding/base64"
	"sort"
	"strings"
	"unicode/utf8"
)

// Program Status Protocol (OSC 7501).
//
// A program reports its state with `ESC ] 7501 ; key=value[:key=value...] ST`
// and probes for support with `ESC ] 7501 ; ? ST`, where ST is `ESC \` or BEL.
// ProgStatusParser observes terminal output without modifying it and keeps a
// ProgStatusStore up to date.

// ProgState is the reported state of a program or sub-task.
type ProgState string

const (
	ProgIdle    ProgState = "idle"
	ProgWorking ProgState = "working"
	ProgDone    ProgState = "done"
	ProgBlocked ProgState = "blocked"
	ProgError   ProgState = "error"
	// ProgClear never appears in a stored record; it removes records.
	ProgClear ProgState = "clear"
)

// ProgKind says why a program is blocked.
type ProgKind string

const (
	ProgKindNone       ProgKind = ""
	ProgKindPermission ProgKind = "permission"
	ProgKindQuestion   ProgKind = "question"
	ProgKindAuth       ProgKind = "auth"
)

const (
	// DefaultMaxProgRecords is the record cap used when none is given.
	DefaultMaxProgRecords = 256

	progMaxSequence = 4096 // ESC ] through the terminator
	// progMaxPayload is the longest buffered payload (after "7501;") that can
	// still fit in progMaxSequence with a one-byte terminator.
	progMaxPayload  = progMaxSequence - 2 - len(progOSCPrefix) - 1
	progMaxIDBytes  = 128
	progMaxIDDepth  = 8
	progMaxSegment  = 32
	progMaxKey      = 16
	progMaxTitleEnc = 256
	progMaxTitleDec = 192
	progMaxMsgEnc   = 2732
	progMaxMsgDec   = 2048
)

// ProgStatus is one validated report or stored record.
type ProgStatus struct {
	// ID is the slash-separated record path; empty addresses the root record.
	ID    string
	State ProgState
	Kind  ProgKind
	// Progress is 0..100, or -1 when absent.
	Progress int
	// App is the reporting application. In reports and in Store.Apply input it
	// is as sent; Store reads fill it from the nearest ancestor when empty.
	App   string
	Title string
	Msg   string
}

type progEntry struct {
	rec ProgStatus
	seq uint64
}

// ProgStatusStore holds the latest record per ID. It is not safe for
// concurrent use; the caller guards it.
type ProgStatusStore struct {
	max  int
	seq  uint64
	recs map[string]*progEntry
}

// NewProgStatusStore returns a store holding at most max records
// (DefaultMaxProgRecords when max <= 0).
func NewProgStatusStore(max int) *ProgStatusStore {
	if max <= 0 {
		max = DefaultMaxProgRecords
	}
	return &ProgStatusStore{max: max, recs: make(map[string]*progEntry)}
}

// Apply stores a report, replacing the record with the same ID. A ProgClear
// report removes the addressed record and its descendants (all records when ID
// is empty). Creating a record past the cap evicts the least recently updated.
func (s *ProgStatusStore) Apply(r ProgStatus) {
	if r.State == ProgClear {
		s.Clear(r.ID)
		return
	}
	s.seq++
	if e, ok := s.recs[r.ID]; ok {
		e.rec, e.seq = r, s.seq
		return
	}
	if len(s.recs) >= s.max {
		s.evictOldest()
	}
	s.recs[r.ID] = &progEntry{rec: r, seq: s.seq}
}

func (s *ProgStatusStore) evictOldest() {
	var victim string
	var oldest uint64
	first := true
	for id, e := range s.recs {
		if first || e.seq < oldest {
			victim, oldest, first = id, e.seq, false
		}
	}
	if !first {
		delete(s.recs, victim)
	}
}

// Clear removes the record id and all its descendants; an empty id removes
// every record.
func (s *ProgStatusStore) Clear(id string) {
	if id == "" {
		s.Reset()
		return
	}
	prefix := id + "/"
	for k := range s.recs {
		if k == id || strings.HasPrefix(k, prefix) {
			delete(s.recs, k)
		}
	}
}

// Reset removes every record.
func (s *ProgStatusStore) Reset() {
	clear(s.recs)
}

// DropOnExit applies process-exit semantics: working, blocked and idle records
// are dropped (the program is no longer doing or awaiting anything), while
// done and error records survive as the final outcome.
func (s *ProgStatusStore) DropOnExit() {
	for k, e := range s.recs {
		switch e.rec.State {
		case ProgDone, ProgError:
		default:
			delete(s.recs, k)
		}
	}
}

// Len reports the number of stored records.
func (s *ProgStatusStore) Len() int { return len(s.recs) }

// Get returns the record for id with App inherited from the nearest ancestor
// that has one.
func (s *ProgStatusStore) Get(id string) (ProgStatus, bool) {
	e, ok := s.recs[id]
	if !ok {
		return ProgStatus{}, false
	}
	r := e.rec
	if r.App == "" {
		r.App = s.inheritedApp(id)
	}
	return r, true
}

// Root returns the root (empty ID) record.
func (s *ProgStatusStore) Root() (ProgStatus, bool) { return s.Get("") }

// Snapshot returns all records ordered by ID (root first), with App inherited.
func (s *ProgStatusStore) Snapshot() []ProgStatus {
	out := make([]ProgStatus, 0, len(s.recs))
	for id := range s.recs {
		r, _ := s.Get(id)
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *ProgStatusStore) inheritedApp(id string) string {
	for id != "" {
		i := strings.LastIndexByte(id, '/')
		if i < 0 {
			id = ""
		} else {
			id = id[:i]
		}
		if e, ok := s.recs[id]; ok && e.rec.App != "" {
			return e.rec.App
		}
	}
	return ""
}

// ProgStatusHooks receives parser outputs in stream order. Any field may be
// nil. Hooks run inside Feed and must not block.
type ProgStatusHooks struct {
	// OnQuery fires for each feature-detection query (`OSC 7501 ; ? ST`).
	OnQuery func()
	// OnReport fires for each accepted report, after it was applied to the store.
	OnReport func(ProgStatus)
	// OnReset fires on RIS (`ESC c`), after the store was reset.
	OnReset func()
}

type progParseState uint8

const (
	psGround progParseState = iota
	psEsc
	psEscInter // one byte after an ESC intermediate (charset designators)
	psCSI
	psString    // DCS, SOS, PM, APC body
	psStringEsc // ESC seen inside a string body
	psOSC
	psOSCEsc
)

type progOSCMode uint8

const (
	oscPrefix progOSCMode = iota // matching "7501;"
	oscBuffer                    // collecting a 7501 payload
	oscSkip                      // other OSC, or an oversized 7501 payload
)

const progOSCPrefix = "7501;"

// ProgStatusParser is a streaming OSC 7501 recogniser. It reads the bytes
// given to Feed without altering them and holds no locks or goroutines;
// callers serialise Feed and any store access.
type ProgStatusParser struct {
	store *ProgStatusStore
	hooks ProgStatusHooks

	state    progParseState
	mode     progOSCMode
	prefixAt int
	overflow bool
	buf      []byte
}

// NewProgStatusParser returns a parser that applies accepted reports to store
// (which may be nil) and notifies hooks.
func NewProgStatusParser(store *ProgStatusStore, hooks ProgStatusHooks) *ProgStatusParser {
	return &ProgStatusParser{store: store, hooks: hooks}
}

// Feed consumes the next chunk of terminal output. Chunks may split anywhere.
func (p *ProgStatusParser) Feed(chunk []byte) {
	for _, b := range chunk {
		p.step(b)
	}
}

func (p *ProgStatusParser) step(b byte) {
	switch p.state {
	case psGround:
		if b == 0x1b {
			p.state = psEsc
		}
	case psEsc:
		p.escByte(b)
	case psEscInter:
		switch b {
		case 0x1b:
			p.state = psEsc
		default:
			p.state = psGround
		}
	case psCSI:
		switch {
		case b == 0x1b:
			p.state = psEsc
		case b == 0x18 || b == 0x1a:
			p.state = psGround
		case b >= 0x40 && b <= 0x7e:
			p.state = psGround
		case b >= 0x80:
			p.state = psGround
		}
	case psString:
		switch b {
		case 0x1b:
			p.state = psStringEsc
		case 0x18, 0x1a:
			p.state = psGround
		}
	case psStringEsc:
		switch b {
		case '\\', 0x18, 0x1a:
			p.state = psGround
		case 0x1b:
		default:
			p.state = psString
		}
	case psOSC:
		switch b {
		case 0x07:
			p.finishOSC(1)
		case 0x1b:
			p.state = psOSCEsc
		case 0x18, 0x1a:
			p.state = psGround
		default:
			p.oscByte(b)
		}
	case psOSCEsc:
		if b == '\\' {
			p.finishOSC(2)
			return
		}
		p.state = psEsc
		p.escByte(b)
	}
}

func (p *ProgStatusParser) escByte(b byte) {
	switch {
	case b == 0x1b:
	case b == 0x18 || b == 0x1a:
		p.state = psGround
	case b == '[':
		p.state = psCSI
	case b == ']':
		p.state = psOSC
		p.mode = oscPrefix
		p.prefixAt = 0
		p.overflow = false
		p.buf = p.buf[:0]
	case b == 'P' || b == 'X' || b == '^' || b == '_':
		p.state = psString
	case b >= 0x20 && b <= 0x2f:
		p.state = psEscInter
	case b == 'c':
		p.state = psGround
		p.reset()
	default:
		p.state = psGround
	}
}

func (p *ProgStatusParser) oscByte(b byte) {
	switch p.mode {
	case oscPrefix:
		if b != progOSCPrefix[p.prefixAt] {
			p.mode = oscSkip
			return
		}
		p.prefixAt++
		if p.prefixAt == len(progOSCPrefix) {
			p.mode = oscBuffer
		}
	case oscBuffer:
		if len(p.buf) >= progMaxPayload {
			p.overflow = true
			p.mode = oscSkip
			return
		}
		p.buf = append(p.buf, b)
	}
}

func (p *ProgStatusParser) finishOSC(termLen int) {
	p.state = psGround
	if p.mode != oscBuffer || p.overflow {
		return
	}
	if 2+len(progOSCPrefix)+len(p.buf)+termLen > progMaxSequence {
		return
	}
	if strings.TrimSpace(string(p.buf)) == "?" {
		if p.hooks.OnQuery != nil {
			p.hooks.OnQuery()
		}
		return
	}
	r, ok := parseProgReport(string(p.buf))
	if !ok {
		return
	}
	if p.store != nil {
		p.store.Apply(r)
	}
	if p.hooks.OnReport != nil {
		p.hooks.OnReport(r)
	}
}

func (p *ProgStatusParser) reset() {
	if p.store != nil {
		p.store.Reset()
	}
	if p.hooks.OnReset != nil {
		p.hooks.OnReset()
	}
}

// parseProgReport validates a `key=value:...` payload. ok is false when the
// whole report must be discarded. Pairs with a bad key, or a bad value on a
// key whose failure does not discard the report, are skipped; a value with
// bytes outside the value set on id, title or msg discards the report so a
// damaged id never falls back to the root record. A key over the length limit
// discards the report.
func parseProgReport(payload string) (ProgStatus, bool) {
	var state, id, kind, progress, app, title, msg string
	var hasID, hasTitle, hasMsg bool

	for _, pair := range strings.Split(payload, ":") {
		eq := strings.IndexByte(pair, '=')
		if eq < 0 {
			continue
		}
		key := strings.Trim(pair[:eq], " \t")
		val := strings.Trim(pair[eq+1:], " \t")
		if len(key) > progMaxKey {
			return ProgStatus{}, false
		}
		if !validProgKey(key) {
			continue
		}
		if !validProgValue(val) {
			if key == "id" || key == "title" || key == "msg" {
				return ProgStatus{}, false
			}
			continue
		}
		switch key {
		case "state":
			state = val
		case "id":
			id, hasID = val, true
		case "kind":
			kind = val
		case "progress":
			progress = val
		case "app":
			app = val
		case "title":
			title, hasTitle = val, true
		case "msg":
			msg, hasMsg = val, true
		}
	}

	r := ProgStatus{Progress: -1}
	switch ProgState(state) {
	case ProgIdle, ProgWorking, ProgDone, ProgBlocked, ProgError, ProgClear:
		r.State = ProgState(state)
	default:
		return ProgStatus{}, false
	}
	if hasID {
		if !validProgID(id) {
			return ProgStatus{}, false
		}
		r.ID = id
	}
	if hasTitle {
		t, ok := decodeProgText(title, progMaxTitleEnc, progMaxTitleDec)
		if !ok {
			return ProgStatus{}, false
		}
		r.Title = t
	}
	if hasMsg {
		m, ok := decodeProgText(msg, progMaxMsgEnc, progMaxMsgDec)
		if !ok {
			return ProgStatus{}, false
		}
		r.Msg = m
	}
	if r.State == ProgBlocked {
		switch k := ProgKind(kind); k {
		case ProgKindPermission, ProgKindQuestion, ProgKindAuth:
			r.Kind = k
		}
	}
	if r.State == ProgWorking || r.State == ProgBlocked {
		r.Progress = parseProgPercent(progress)
	}
	if validProgSegment(app) {
		r.App = app
	}
	return r, true
}

func validProgKey(k string) bool {
	if k == "" || len(k) > progMaxKey {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 'a' || k[i] > 'z' {
			return false
		}
	}
	return true
}

func validProgValue(v string) bool {
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case strings.IndexByte("_.,+/=-", c) >= 0:
		default:
			return false
		}
	}
	return true
}

func validProgSegment(s string) bool {
	if s == "" || len(s) > progMaxSegment {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '_' || c == '.' || c == '+' || c == '-':
		default:
			return false
		}
	}
	return true
}

func validProgID(id string) bool {
	if id == "" || len(id) > progMaxIDBytes {
		return false
	}
	segs := strings.Split(id, "/")
	if len(segs) > progMaxIDDepth {
		return false
	}
	for _, s := range segs {
		if !validProgSegment(s) {
			return false
		}
	}
	return true
}

func parseProgPercent(s string) int {
	if s == "" || len(s) > 3 {
		return -1
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return -1
		}
		n = n*10 + int(s[i]-'0')
	}
	if n > 100 {
		return -1
	}
	return n
}

// decodeProgText decodes unpadded-or-padded standard base64 holding UTF-8
// without control characters. The encoded length is checked before decoding.
func decodeProgText(enc string, maxEnc, maxDec int) (string, bool) {
	if len(enc) > maxEnc {
		return "", false
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(enc, "="))
	if err != nil || len(raw) > maxDec || !utf8.Valid(raw) {
		return "", false
	}
	for _, r := range string(raw) {
		if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
			return "", false
		}
	}
	return string(raw), true
}
