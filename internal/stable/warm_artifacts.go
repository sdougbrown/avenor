package stable

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/cli"
	"github.com/sdougbrown/avenor/internal/permission"
	"github.com/sdougbrown/avenor/internal/runstate"
	"github.com/sdougbrown/avenor/internal/runtime/warmqueue"
)

// warmResumeArtifacts holds everything a successful reservation produced for
// the next turn. None of it has been installed on the child yet.
type warmResumeArtifacts struct {
	onEvent     string
	sentinel    string
	writer      cli.EventSink
	fileHandler *permission.FileHandler
	// cleanup removes a reserved artifact directory after a failure; it does
	// nothing when the paths were caller-supplied and no directory was made.
	cleanup func()
}

// admitWarmResume installs the next turn's artifacts before publishing its
// prompt. The runtime's turnMu serializes the fence with turn-bound operations, and writeMu
// serializes the swap with all event writes and writer teardown. Lock order:
// see childRuntime.turnMu.
//
// The sequence is: gate on supervisor and child state, take the admission
// locks, reserve the next turn's artifacts, then rotate the child onto the
// reserved turn. Everything that can fail happens before rotation, so a
// rejection leaves the child exactly as it was.
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

	if warm, err := s.gateWarmResume(child, &p); err != nil || !warm {
		return SpawnResult{}, warm, err
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	if warm, err := s.gateWarmResumeChild(child); err != nil || !warm {
		return SpawnResult{}, warm, err
	}
	if h := s.testHooks.warmAdmissionAfterCheck; h != nil {
		h()
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

	reserved, err := s.reserveWarmResumeArtifacts(child, p)
	if err != nil {
		return SpawnResult{}, true, err
	}
	result := s.rotateWarmResumeTurn(child, p, reserved)
	if release != nil {
		release(true)
		release = nil
		if p.Label != "" {
			child.displayName = s.broker.DisplayName(child.id)
		}
	}
	return result, true, nil
}

// gateWarmResume rejects a warm resume while the supervisor is shutting down
// or while the requested label is owned by another runtime, and resolves an
// implicit label from the broker's harness name. It returns warm=false only
// for the lifecycle refusal, which is not an error. It runs before child.mu so
// the documented lock order is preserved.
func (s *Supervisor) gateWarmResume(child *childRuntime, p *SpawnParams) (bool, error) {
	if s.shuttingDown {
		return true, fmt.Errorf("supervisor is shutting down")
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
		return true, fmt.Errorf("label already in use: %s (runtime %s)", p.Label, holder)
	}
	return true, nil
}

// gateWarmResumeChild rejects a warm resume for a child that is completed,
// shutting down, still closing its writer, or otherwise busy, and for a child
// with pending broker messages. It returns warm=false only for the lifecycle
// refusal, which is not an error. It requires child.mu.
func (s *Supervisor) gateWarmResumeChild(child *childRuntime) (bool, error) {
	if child.lc.isCompleted() || child.lc.isShuttingDown() || child.lc.isWriterClosing() {
		return false, nil
	}
	if !child.lc.isParked() || child.warmResumePending || child.promptClaimed || len(child.promptQueue) != 0 {
		return true, fmt.Errorf("runtime %q is busy", child.id)
	}
	if s.broker != nil {
		if st := s.broker.GetRun(child.id); st != nil {
			st.Mu.Lock()
			pending := len(st.ControlQueue) != 0
			st.Mu.Unlock()
			if pending {
				return true, fmt.Errorf("runtime %q has pending broker messages", child.id)
			}
		}
	}
	return true, nil
}

// lockWarmResumeAdmission takes the broker's warm-admission lock and the
// provider's warm-message guard for the session. The caller must pass the
// broker release commit=true only after the turn is published (false releases
// it without committing), and must always call the returned provider unlock
// when it is non-nil. Both run while child.mu is held, after s.controlMu, in
// the documented lock order.
func (s *Supervisor) lockWarmResumeAdmission(child *childRuntime, label string) (release func(bool), unlockProvider func(), err error) {
	if s.broker != nil {
		release, err = s.broker.LockWarmAdmission(child.id, label)
		if err != nil {
			return nil, nil, err
		}
	}
	if guard, ok := child.provider.(warmqueue.WarmMessageAdmission); ok {
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

// reserveWarmResumeArtifacts picks the next turn's event and sentinel paths,
// rejects path collisions, exclusively creates the event file, and parses the
// derived permission handler. Default paths live under the shared artifact
// root, so their directory is created through the same verified-root path a
// cold spawn uses. Every fallible step happens here, so a failure
// releases the whole reservation — the new writer is closed and reserved
// directories removed — and the caller's child state is untouched. Requires
// child.mu (the collision checks compare against the child's current paths).
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
	for _, pair := range [][2]string{{onEvent, sentinel}, {onEvent, child.onEvent}, {onEvent, child.sentinelFile}, {sentinel, child.onEvent}, {sentinel, child.sentinelFile}} {
		if sameArtifact(pair[0], pair[1]) {
			cleanup()
			return warmResumeArtifacts{}, fmt.Errorf("warm resume artifact paths must be distinct")
		}
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

// rotateWarmResumeTurn swaps the child onto the reserved turn: it installs the
// new writer and derived handler, swaps the artifact paths, resets the turn's
// output state, queues the prompt, closes the previous writer, and signals the
// prompt. It cannot fail — every fallible step was validated by the
// reservation — and it requires child.mu and the held admission locks.
func (s *Supervisor) rotateWarmResumeTurn(child *childRuntime, p SpawnParams, reserved warmResumeArtifacts) SpawnResult {
	old := child.eventWriter
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
	if old != nil {
		_ = old.Close()
	}
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
	aa, ea := filepath.Abs(a)
	bb, eb := filepath.Abs(b)
	if ea == nil && eb == nil && aa == bb {
		return true
	}
	ai, ea := os.Stat(a)
	bi, eb := os.Stat(b)
	return ea == nil && eb == nil && os.SameFile(ai, bi)
}
