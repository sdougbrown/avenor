package terminal

import (
	"sync"
	"time"
)

// progReportLogSize bounds how many accepted reports a session retains for
// ProgStatusSince.
const progReportLogSize = 256

// progStatusLog is the ProgStatusSession state shared by the PTY session and
// the fake: the record store, a bounded log of accepted reports and a
// change notification. Writers never block.
type progStatusLog struct {
	psMu       sync.Mutex
	psStore    *ProgStatusStore
	psLog      []ProgReport
	psSeq      uint64
	psAnswered bool
	psChanged  chan struct{}
}

// psInit lazily allocates the log so zero-value sessions work. psMu is held.
func (l *progStatusLog) psInit() {
	if l.psStore == nil {
		l.psStore = NewProgStatusStore(0)
	}
	if l.psChanged == nil {
		l.psChanged = make(chan struct{}, 1)
	}
}

// ProgStatusAnswered reports whether a detection query has been answered.
func (l *progStatusLog) ProgStatusAnswered() bool {
	l.psMu.Lock()
	defer l.psMu.Unlock()
	return l.psAnswered
}

// ProgStatusRoot returns the current root record.
func (l *progStatusLog) ProgStatusRoot() (ProgStatus, bool) {
	l.psMu.Lock()
	defer l.psMu.Unlock()
	l.psInit()
	return l.psStore.Root()
}

// ProgStatusSince returns the retained reports with Seq greater than after,
// oldest first. lost is true when reports after `after` were already dropped
// from the bounded log.
func (l *progStatusLog) ProgStatusSince(after uint64) (reports []ProgReport, lost bool) {
	l.psMu.Lock()
	defer l.psMu.Unlock()
	if len(l.psLog) == 0 {
		return nil, false
	}
	lost = l.psLog[0].Seq > after+1
	for i, r := range l.psLog {
		if r.Seq > after {
			return append([]ProgReport(nil), l.psLog[i:]...), lost
		}
	}
	return nil, lost
}

// ProgStatusChanged returns a channel that receives a value after the log or
// store changes. One pending value covers any number of changes.
func (l *progStatusLog) ProgStatusChanged() <-chan struct{} {
	l.psMu.Lock()
	defer l.psMu.Unlock()
	l.psInit()
	return l.psChanged
}

func (l *progStatusLog) psRecord(r ProgStatus) {
	l.psMu.Lock()
	l.psInit()
	l.psStore.Apply(r)
	l.psAppend(r)
	l.psMu.Unlock()
}

// psReset handles RIS: every record is gone, which the log carries as a root
// clear.
func (l *progStatusLog) psReset() {
	l.psMu.Lock()
	l.psInit()
	l.psStore.Reset()
	l.psAppend(ProgStatus{State: ProgClear, Progress: -1})
	l.psMu.Unlock()
}

func (l *progStatusLog) psDropOnExit() {
	l.psMu.Lock()
	l.psInit()
	l.psStore.DropOnExit()
	l.psNotify()
	l.psMu.Unlock()
}

func (l *progStatusLog) psMarkAnswered() {
	l.psMu.Lock()
	l.psAnswered = true
	l.psMu.Unlock()
}

// psAppend logs r and notifies. psMu is held.
func (l *progStatusLog) psAppend(r ProgStatus) {
	l.psSeq++
	if len(l.psLog) >= progReportLogSize {
		n := copy(l.psLog, l.psLog[1:])
		l.psLog = l.psLog[:n]
	}
	l.psLog = append(l.psLog, ProgReport{Seq: l.psSeq, At: time.Now(), Status: r})
	l.psNotify()
}

func (l *progStatusLog) psNotify() {
	select {
	case l.psChanged <- struct{}{}:
	default:
	}
}
