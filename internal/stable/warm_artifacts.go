package stable

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runstate"
)

// admitWarmResume installs the next turn's artifacts before publishing its prompt.
// The runtime's turnMu serializes the fence with WithTurn, and writeMu
// serializes the swap with all event writes and writer teardown. Lock order:
// see childRuntime.turnMu.
func (s *Supervisor) admitWarmResume(child *childRuntime, p SpawnParams) (SpawnResult, bool, error) {
	child.turnMu.Lock()
	defer child.turnMu.Unlock()
	child.writeMu.Lock()
	for child.directAnswers > 0 {
		done := child.directAnswersDone
		child.writeMu.Unlock()
		<-done
		child.writeMu.Lock()
	}
	defer child.writeMu.Unlock()
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.shuttingDown {
		return SpawnResult{}, true, fmt.Errorf("supervisor is shutting down")
	}
	if p.resume.implicitLabel && s.broker != nil {
		if st := s.broker.GetRun(child.id); st != nil {
			st.Mu.Lock()
			if st.HarnessName != "" {
				p.Label = st.HarnessName
			}
			st.Mu.Unlock()
		}
	}
	if holder := s.labelHolderLocked(p.Label); holder != "" && holder != child.id {
		return SpawnResult{}, true, fmt.Errorf("label already in use: %s (runtime %s)", p.Label, holder)
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	if child.lc.isCompleted() || child.lc.isShuttingDown() || child.lc.isWriterClosing() {
		return SpawnResult{}, false, nil
	}
	if !child.lc.isParked() || child.warmResumePending || child.promptClaimed || len(child.promptQueue) != 0 {
		return SpawnResult{}, true, fmt.Errorf("runtime %q is busy", child.id)
	}
	if s.broker != nil {
		if st := s.broker.GetRun(child.id); st != nil {
			st.Mu.Lock()
			pending := len(st.ControlQueue) != 0
			st.Mu.Unlock()
			if pending {
				return SpawnResult{}, true, fmt.Errorf("runtime %q has pending broker messages", child.id)
			}
		}
	}

	if h := s.testHooks.warmAdmissionAfterCheck; h != nil {
		h()
	}
	var release func(bool)
	if s.broker != nil {
		var err error
		release, err = s.broker.LockWarmAdmission(child.id, p.Label)
		if err != nil {
			return SpawnResult{}, true, err
		}
		defer func() {
			if release != nil {
				release(false)
			}
		}()
	}
	if guard, ok := child.provider.(interface {
		LockWarmMessageAdmission(string) (func(), bool)
	}); ok {
		unlock, admitted := guard.LockWarmMessageAdmission(child.session.SessionID)
		if !admitted {
			return SpawnResult{}, true, fmt.Errorf("runtime %q has pending provider messages", child.id)
		}
		defer unlock()
	}

	onEvent, sentinel := p.OnEvent, p.SentinelFile
	var dir string
	if onEvent == "" || sentinel == "" {
		root := s.runtimeArtifactDir(child.id)
		if err := os.MkdirAll(root, 0700); err != nil {
			return SpawnResult{}, true, err
		}
		var err error
		dir, err = os.MkdirTemp(root, "turn-")
		if err != nil {
			return SpawnResult{}, true, err
		}
		if onEvent == "" {
			onEvent = filepath.Join(dir, "events.ndjson")
		}
		if sentinel == "" {
			sentinel = filepath.Join(dir, "sentinel.env")
		}
	}
	cleanup := func() {
		if dir != "" {
			_ = os.RemoveAll(dir)
		}
	}
	for _, pair := range [][2]string{{onEvent, sentinel}, {onEvent, child.onEvent}, {onEvent, child.sentinelFile}, {sentinel, child.onEvent}, {sentinel, child.sentinelFile}} {
		if sameArtifact(pair[0], pair[1]) {
			cleanup()
			return SpawnResult{}, true, fmt.Errorf("warm resume artifact paths must be distinct")
		}
	}
	if _, err := os.Lstat(sentinel); err == nil {
		cleanup()
		return SpawnResult{}, true, fmt.Errorf("sentinel path already exists: %s", sentinel)
	} else if !os.IsNotExist(err) {
		cleanup()
		return SpawnResult{}, true, err
	}
	// Reserve a fresh path before opening the writer: earlier turns' event
	// files must never be truncated, including hard links to those files.
	file, err := os.OpenFile(onEvent, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		cleanup()
		return SpawnResult{}, true, err
	}
	writer := &warmEventWriter{file: file, encoder: json.NewEncoder(file)}
	old := child.eventWriter
	child.eventWriter = writer
	if child.fanoutWriter != nil {
		child.fanoutWriter.base = writer
	}
	child.onEvent, child.sentinelFile = onEvent, sentinel
	if child.derivedFileHandler {
		fh, err := cli.ParsePermissionHandler("file:" + cli.DerivePermBase(sentinel))
		if err != nil {
			cleanup()
			return SpawnResult{}, true, fmt.Errorf("permission handler: %w", err)
		}
		child.fileHandler = fh
	}
	child.task, child.taskFile = p.Prompt, p.PromptFile
	child.lastStopReason = ""
	child.finalOutput, child.fullFinalOutput = "", ""
	child.finalOutputTruncated = false
	child.usage = nil
	child.pendingPermission = false
	child.permission = nil
	child.exitCode = 0
	child.phase, child.phaseLabel = runstate.Phase("working"), ""
	child.warmResumePending = true
	child.promptQueue = append(child.promptQueue, p.Prompt)
	if old != nil {
		_ = old.Close()
	}
	child.signalPrompt()
	if release != nil {
		release(true)
		release = nil
		if p.Label != "" {
			child.displayName = s.broker.DisplayName(child.id)
		}
	}
	return SpawnResult{RunID: p.resume.runID, RuntimeID: child.id, Label: p.Label, SessionID: child.session.SessionID, OnEvent: onEvent, SentinelFile: sentinel}, true, nil
}

type warmEventWriter struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
}

func (w *warmEventWriter) Write(event events.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.encoder.Encode(event)
}

func (w *warmEventWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// finishUnstartedWarmResume settles an admitted turn if shutdown prevented its
// attempt from producing a completion sentinel.
func (s *Supervisor) finishUnstartedWarmResume(child *childRuntime) {
	child.mu.Lock()
	pending, path, sessionID := child.warmResumePending, child.sentinelFile, child.session.SessionID
	child.mu.Unlock()
	if !pending || path == "" {
		return
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return
	}
	child.mu.Lock()
	child.exitCode = 1
	child.lastStopReason = "error"
	child.mu.Unlock()
	s.emitChildError(child, "admitted resume ended before its turn completed", "error")
	cli.WriteSentinel(path, 1, sessionID, "error", s.runID, s.stderrWriter())
}

func sameArtifact(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	aa, ea := filepath.Abs(a)
	bb, eb := filepath.Abs(b)
	if ea == nil && eb == nil && aa == bb {
		return true
	}
	ai, ea := os.Stat(a)
	bi, eb := os.Stat(b)
	return ea == nil && eb == nil && os.SameFile(ai, bi)
}
