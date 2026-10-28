package stable

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// warmResumeArtifacts holds everything a successful reservation produced for
// the next turn. None of it has been installed on the child yet.
type warmResumeArtifacts struct {
	onEvent     string
	sentinel    string
	writer      cli.EventSink
	fileHandler *permission.FileHandler
	// cleanup removes everything the reservation created after a failure:
	// the reserved directory when one was made, and the event file the
	// exclusive create made, even when its path was caller-supplied. It must
	// not be called once the turn has been rotated onto the reservation.
	cleanup func()
}

// admitWarmResume publishes the reserved artifacts and prompt as one turn.
// turnMu serializes admission with turn-bound operations; writeMu excludes
// event writes and writer teardown. Lock order: see childRuntime.turnMu.
//
// Reservation runs without controlMu, child.mu or the broker/provider guards.
// Admission rechecks supervisor, label, lifecycle and messages before rotation.
// Rejection leaves the child unchanged and removes the reservation.
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
	if err := s.gateWarmResume(child, p); err != nil {
		s.controlMu.Unlock()
		return SpawnResult{}, true, err
	}
	child.mu.Lock()
	warm, err := s.gateWarmResumeChild(child)
	child.mu.Unlock()
	s.controlMu.Unlock()
	if err != nil || !warm {
		return SpawnResult{}, warm, err
	}

	reserved, err := s.reserveWarmResumeArtifacts(child, p)
	if err != nil {
		return SpawnResult{}, true, err
	}
	installed := false
	var old cli.EventSink
	defer func() {
		if !installed {
			_ = reserved.writer.Close()
			reserved.cleanup()
		}
		if old != nil {
			_ = old.Close()
		}
	}()
	if h := s.testHooks.warmAdmissionAfterReservation; h != nil {
		h()
	}

	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if err := s.gateWarmResume(child, p); err != nil {
		return SpawnResult{}, true, err
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	if warm, err := s.gateWarmResumeChild(child); err != nil || !warm {
		return SpawnResult{}, warm, err
	}
	release, unlockProvider, err := s.lockWarmResumeAdmission(child, p.Label)
	if err != nil {
		return SpawnResult{}, true, err
	}
	defer func() {
		if release != nil {
			release(false)
		}
	}()
	if unlockProvider != nil {
		defer unlockProvider()
	}

	old = child.eventWriter
	result := s.rotateWarmResumeTurn(child, p, reserved)
	installed = true
	if release != nil {
		release(true)
		release = nil
		if p.Label != "" {
			child.displayName = s.broker.DisplayName(child.id)
		}
	}
	return result, true, nil
}

// gateWarmResume checks supervisor and label admission under s.controlMu,
// before taking child.mu so the label scan can lock each child itself.
func (s *Supervisor) gateWarmResume(child *childRuntime, p SpawnParams) error {
	if s.shuttingDown {
		return fmt.Errorf("supervisor is shutting down")
	}
	if holder := s.labelHolderLocked(p.Label); holder != "" && holder != child.id {
		return fmt.Errorf("label already in use: %s (runtime %s)", p.Label, holder)
	}
	return nil
}

// gateWarmResumeChild checks lifecycle admission under child.mu. A false, nil
// result permits a cold continuation. Broker and provider messages are checked
// under their admission locks.
func (s *Supervisor) gateWarmResumeChild(child *childRuntime) (bool, error) {
	if child.lc.isCompleted() || child.lc.isShuttingDown() || child.lc.isWriterClosing() {
		return false, nil
	}
	if !child.lc.isParked() || child.warmResumePending || child.promptClaimed || len(child.promptQueue) != 0 {
		return true, fmt.Errorf("runtime %q is busy", child.id)
	}
	return true, nil
}

// lockWarmResumeAdmission locks the broker queue and the provider's injection
// guard, if any. Non-injecting providers retain their unread inbox across turns.
// The caller holds controlMu and child.mu, releases with commit=true only after
// publication, and calls any provider unlock returned.
func (s *Supervisor) lockWarmResumeAdmission(child *childRuntime, label string) (release func(bool), unlockProvider func(), err error) {
	guard, injectsMessages := child.provider.(runtime.WarmMessageAdmission)
	if s.broker != nil {
		release, err = s.broker.LockWarmAdmission(child.id, label, injectsMessages)
		if err != nil {
			return nil, nil, err
		}
	}
	if injectsMessages {
		unlock, admitted := guard.LockWarmMessageAdmission(child.session.SessionID)
		if !admitted {
			if release != nil {
				release(false)
			}
			return nil, nil, fmt.Errorf("runtime %q has pending provider messages", child.id)
		}
		return release, unlock, nil
	}
	return release, nil, nil
}

// reserveWarmResumeArtifacts reserves distinct turn artifacts and derives the
// permission handler. Default paths use the verified artifact root. Failure
// closes the writer and removes the reservation without changing the child.
// Requires child.writeMu, which protects artifact paths from rotation and
// teardown. The handler derivation policy is immutable after spawn.
func (s *Supervisor) reserveWarmResumeArtifacts(child *childRuntime, p SpawnParams) (warmResumeArtifacts, error) {
	onEvent, sentinel := p.OnEvent, p.SentinelFile
	var dir string
	if onEvent == "" || sentinel == "" {
		root, err := createArtifactRunDir(s.runID, child.id)
		if err != nil {
			return warmResumeArtifacts{}, err
		}
		dir, err = os.MkdirTemp(root, "turn-")
		if err != nil {
			return warmResumeArtifacts{}, err
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
	paths := []string{onEvent, sentinel}
	previous := []string{child.onEvent, child.sentinelFile}
	if child.derivedFileHandler {
		request, response := permission.FilePaths(cli.DerivePermBase(sentinel))
		paths = append(paths, request, response)
		if child.sentinelFile != "" {
			request, response = permission.FilePaths(cli.DerivePermBase(child.sentinelFile))
			previous = append(previous, request, response)
		}
	}
	for _, path := range paths {
		for _, other := range previous {
			if sameArtifact(path, other) {
				cleanup()
				return warmResumeArtifacts{}, fmt.Errorf("warm resume artifact paths must be distinct")
			}
		}
		previous = append(previous, path)
	}
	if _, err := os.Lstat(sentinel); err == nil {
		cleanup()
		return warmResumeArtifacts{}, fmt.Errorf("sentinel path already exists: %s", sentinel)
	} else if !os.IsNotExist(err) {
		cleanup()
		return warmResumeArtifacts{}, err
	}
	// Reserve a fresh path before opening the writer: earlier turns' event
	// files must never be truncated, including hard links to those files.
	writer, err := cli.NewEventWriterExclusive(onEvent)
	if err != nil {
		cleanup()
		return warmResumeArtifacts{}, err
	}
	// The exclusive create made the event file; remove it too on a later
	// failure so a rejected reservation leaves nothing behind, including a
	// path the caller supplied.
	cleanup = func() {
		if dir != "" {
			_ = os.RemoveAll(dir)
		}
		_ = os.Remove(onEvent)
	}
	fileHandler, err := s.derivedPermissionHandler(child, sentinel)
	if err != nil {
		_ = writer.Close()
		cleanup()
		return warmResumeArtifacts{}, fmt.Errorf("permission handler: %w", err)
	}
	return warmResumeArtifacts{onEvent: onEvent, sentinel: sentinel, writer: writer, fileHandler: fileHandler, cleanup: cleanup}, nil
}

// derivedPermissionHandler parses the permission handler derived from the next
// sentinel path when the child derives its handler from the sentinel. The
// result is held for rotation; parsing must finish before any child state
// changes so a failure leaves the child untouched.
func (s *Supervisor) derivedPermissionHandler(child *childRuntime, sentinel string) (*permission.FileHandler, error) {
	if !child.derivedFileHandler {
		return nil, nil
	}
	if parse := s.testHooks.warmResumePermissionHandler; parse != nil {
		return parse(sentinel)
	}
	return cli.ParsePermissionHandler("file:" + cli.DerivePermBase(sentinel))
}

// rotateWarmResumeTurn installs a validated reservation and cannot fail.
// Requires child.mu and the admission locks. The caller closes the previous
// writer after releasing those locks, while still holding writeMu.
func (s *Supervisor) rotateWarmResumeTurn(child *childRuntime, p SpawnParams, reserved warmResumeArtifacts) SpawnResult {
	child.eventWriter = reserved.writer
	if child.fanoutWriter != nil {
		child.fanoutWriter.base = reserved.writer
	}
	child.onEvent, child.sentinelFile = reserved.onEvent, reserved.sentinel
	if child.derivedFileHandler {
		child.fileHandler = reserved.fileHandler
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
	child.signalPrompt()
	return SpawnResult{RunID: p.resume.runID, RuntimeID: child.id, Label: p.Label, SessionID: child.session.SessionID, OnEvent: reserved.onEvent, SentinelFile: reserved.sentinel}
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
	aa, ea := cli.ResolveArtifactPath(a)
	bb, eb := cli.ResolveArtifactPath(b)
	if ea == nil && eb == nil && aa == bb {
		return true
	}
	ai, ea := os.Stat(a)
	bi, eb := os.Stat(b)
	return ea == nil && eb == nil && os.SameFile(ai, bi)
}
