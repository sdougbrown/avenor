// Package room implements a small interactive coordinator where a human
// operator and N persistent coding-agent heads share one room record.
//
// The room rides on top of an avenor stable supervisor: each head is a parked
// runtime whose native session is preserved across turns, and every peer or
// operator message is injected as an append-only follow-up prompt. The room
// log (NDJSON in the workspace) is the authoritative record of what passed
// between participants; per-runtime event logs remain authoritative for what
// happened inside each head.
package room

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type Participant string

type Visibility string

const (
	VisibilityRoom Visibility = "room"
)

type RoomEventKind string

const (
	HumanInput       RoomEventKind = "human_input"
	HeadOutput       RoomEventKind = "head_output"
	Mutation         RoomEventKind = "mutation"
	GovernorDecision RoomEventKind = "governor"
	System           RoomEventKind = "system"
)

// RoomEvent is one record in the room log. Visibility states who may see the
// event later; it is deliberately separate from activation, which is a
// scheduler decision made by the Governor.
type RoomEvent struct {
	ID         string            `json:"id"`
	Seq        int64             `json:"seq"`
	Author     Participant       `json:"author"`
	Kind       RoomEventKind     `json:"kind"`
	Body       string            `json:"body"`
	Visibility Visibility        `json:"visibility"`
	Parents    []string          `json:"parents,omitempty"`
	Depth      int               `json:"depth"`
	Meta       map[string]string `json:"meta,omitempty"`
}

// Log is the append-only NDJSON room record.
type Log struct {
	mu     sync.Mutex
	f      *os.File
	seq    int64
	counts map[Participant]int
}

func OpenLog(path string) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{f: f, counts: map[Participant]int{}}
	// Recover per-author counters from any existing record so IDs stay stable
	// across room restarts.
	if rf, err := os.Open(path); err == nil {
		s := bufio.NewScanner(rf)
		for s.Scan() {
			var ev RoomEvent
			if err := json.Unmarshal(s.Bytes(), &ev); err == nil {
				if ev.Seq > l.seq {
					l.seq = ev.Seq
				}
				l.counts[ev.Author]++
			}
		}
		rf.Close()
	}
	return l, nil
}

// Append assigns ID and Seq and writes the event. IDs are per-author
// (H1, A2, ...) so causal references read like the turn labels operators
// naturally use.
func (l *Log) Append(author Participant, kind RoomEventKind, body string, visibility Visibility, parents []string, depth int) (RoomEvent, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	l.counts[author]++
	ev := RoomEvent{
		ID:         fmt.Sprintf("%s%d", author, l.counts[author]),
		Seq:        l.seq,
		Author:     author,
		Kind:       kind,
		Body:       body,
		Visibility: visibility,
		Parents:    parents,
		Depth:      depth,
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return ev, err
	}
	if _, err := l.f.WriteString(string(b) + "\n"); err != nil {
		return ev, err
	}
	return ev, nil
}

// Snapshot returns all events, oldest first. The spike keeps the log fully in
// memory alongside the NDJSON file; volumes are tiny.
func (l *Log) Snapshot() []RoomEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapshotLocked()
}

func (l *Log) snapshotLocked() []RoomEvent {
	events := readAllEvents(l.f)
	sort.Slice(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })
	return events
}

func readAllEvents(f *os.File) []RoomEvent {
	// Re-read from the durable file so the snapshot is always the record of
	// truth even across appends from other processes.
	name := f.Name()
	rf, err := os.Open(name)
	if err != nil {
		return nil
	}
	defer rf.Close()
	var events []RoomEvent
	s := bufio.NewScanner(rf)
	s.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for s.Scan() {
		var ev RoomEvent
		if err := json.Unmarshal(s.Bytes(), &ev); err == nil && ev.ID != "" {
			events = append(events, ev)
		}
	}
	return events
}

// TruncationMark is appended to bounded excerpts.
const TruncationMark = " […]"

// Bound truncates s to roughly limit characters on a rune boundary.
func Bound(s string, limit int) string {
	if limit <= 0 || len(s) <= limit {
		return s
	}
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + TruncationMark
}

// Normalizes whitespace in model output for digest rendering.
func OneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
