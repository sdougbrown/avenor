import type { Hooks, ToolContext } from '@opencode-ai/plugin'
import { tool } from '@opencode-ai/plugin'

import * as readline from 'node:readline'
import { hostname as osHostname } from 'node:os'
import type { ChildProcess } from 'node:child_process'
import { spawnAvenorProcess as defaultSpawnProcess, runAvenorCommand as defaultRunCommand } from './process.js'
import {
  formatAnswerPermissionOutput,
  formatEventsOutput,
  formatInspectOutput,
  formatMessageOutput,
  formatPeersOutput,
  formatResultOutput,
  formatShutdownOutput,
  formatKillOutput,
  formatStatusOutput,
  formatWorkflowControllerStatusOutput,
} from './render.js'
import {
  answerPermissionTool,
  brokerIsLocal,
  boundFinalOutput,
  BrokerHttpError,
  createBrokerChildBroker,
  createChildMessageProvider,
  createHostMessageProvider,
  createRunSnapshot,
  defaultHostLabel,
  dial,
  eventsTool,
  extractTurnText,
  findAvenorBinary,
  getSupervisorClient,
  gitToplevel,
  inspectTool,
  isTerminalStatus,
  messageTool,
  normalizeTurnStopReason,
  isInvalidNameRefusal,
  observeRun,
  PEER_MESSAGE_NOTICE,
  peersTool,
  postBroker,
  publishHarnessName,
  publishLastTurn,
  resultTool,
  sanitize,
  takenLabelsFromListing,
  type MessageProvider,
  type ProviderResult,
  type RunObserver,
  type RunSnapshot,
  type RunSnapshotView,
  shutdownTool,
  killTool,
  spawnTool,
  statusTool,
  Supervisor,
  ToolError,
  type StatusResult,
  THINKING_LEVELS,
  validateSpawnSelection,
  runWorkflowTool,
} from '@dougbots/avenor-core'

type TrackedRun = {
  runId: string
  runtimeId?: string
  sessionId?: string
  agent?: string
  model?: string
  backend?: string
  rosterFile?: string
  rosterEntry?: string
  effectiveAgent?: string
  effectiveModel?: string
  effectiveBackend?: string
  orchestratorSessionId: string
  label: string
  supervisorId?: string
  monitoring: boolean
  latestSeq?: number
  lastSnapshot?: RunSnapshotView
}

/** The owner connection's provider: execution and resume authority only. */
type OwnerBinding = {
  provider: MessageProvider
  supervisorId: string
  generation: string
}

/**
 * One bound conversation: the broker run this conversation acts as, with the
 * credentials its messaging tools and its one inbound consumer authenticate
 * with. The only identity a conversation ever holds is the one its own bind
 * minted.
 */
type ConversationBinding = {
  sessionId: string
  supervisorId: string
  brokerUrl: string
  runId: string
  runToken: string
  generation: string
}

/** The one `avenor feed broker` consumer a bound conversation runs. */
type FeedProcess = {
  child: ChildProcess
  runId: string
  brokerUrl: string
  /**
   * The consumer's inbox lock is released: its process closed, or it never
   * started. A replacement may claim the inbox only after this, because the
   * feed's lock makes a second consumer exit as a duplicate.
   */
  released: boolean
  /**
   * Retire this consumer: no record it has read is delivered and no further
   * record is read. The lines it had queued belong to the identity it served,
   * so they are dropped rather than handed to whatever consumer replaced it.
   */
  retire(): void
  /**
   * Hand the session's inbox to the replacement waiting behind this consumer,
   * but only once this process is gone and — unless it was retired — the record
   * it was delivering has landed. Safe to call repeatedly; it acts only while
   * this consumer still owns the slot or is the one the session waits on.
   */
  settle(): void
}

/** The process seams a test can replace; production uses node's own. */
export interface AvenorCoreOptions {
  /** Start one long-lived child; defaults to node:child_process spawn. */
  spawnProcess?: (argv: string[], env: NodeJS.ProcessEnv) => ChildProcess
  /** Run one short-lived CLI call to completion; defaults to spawn + collect. */
  runCommand?: (
    argv: string[],
    env: NodeJS.ProcessEnv,
    signal?: AbortSignal,
    stdin?: string,
  ) => Promise<{ code: number | null; stdout: string; stderr: string }>
  /** Read the git top-level of a directory; defaults to the shared git reader,
   * which answers an empty string outside a repository. Injectable so tests can
   * place the conversation in a repository without running git. */
  gitToplevel?: (dir: string) => Promise<string>
  /** The machine's name for the default label, read only when the broker is on
   * another machine; injectable so tests can name the machine. */
  hostname?: () => string
}

type MonitorStopReason = 'permission-routed' | 'shutdown' | 'abort' | 'result'

type MonitorOutcome = {
  kind: 'terminal' | 'waiting' | 'aborted' | 'shutdown' | 'stopped' | 'error'
  snapshot: RunSnapshotView
  notified?: boolean
  error?: string
}

type ActiveMonitor = {
  observer?: RunObserver
  stopReason?: MonitorStopReason
  promise: Promise<MonitorOutcome>
}

const FINAL_OUTPUT_CHARS = 1_500
const METADATA_TEXT_CHARS = 400
const TRANSCRIPT_PREVIEW_ENTRIES = 3

function clipTail(text: string | undefined, limit: number): string | undefined {
  if (!text) return undefined
  if (text.length <= limit) return text
  return text.slice(text.length - limit)
}

export function terminalStatus(snapshot: RunSnapshotView, fallback?: StatusResult): string {
  if (fallback?.status) return fallback.status
  if (snapshot.phase && isTerminalStatus(snapshot.phase)) return snapshot.phase
  return snapshot.stop_reason ?? 'done'
}

function formatLiveTool(snapshot: RunSnapshotView): string | undefined {
  const tool = snapshot.live_tools[snapshot.live_tools.length - 1]
  if (!tool) return undefined
  const base = tool.title ?? tool.kind ?? tool.id ?? tool.key
  const status = tool.status ? ` (${tool.status})` : ''
  const preview = clipTail(tool.preview, 120)
  return preview ? `${base}${status}: ${preview}` : `${base}${status}`
}

function formatTranscriptEntry(entry: RunSnapshot['transcript'][number]): string | undefined {
  const text = clipTail(entry.text?.trim(), 120)
  switch (entry.kind) {
    case 'assistant':
      return text ? `assistant: ${text}` : undefined
    case 'thought':
      return text ? `thought: ${text}` : undefined
    case 'user':
      return text ? `user: ${text}` : undefined
    case 'tool': {
      const title = entry.title ?? 'tool'
      const status = entry.status ? ` (${entry.status})` : ''
      return text ? `${title}${status}: ${text}` : `${title}${status}`
    }
    case 'permission':
      return text ? `permission: ${text}` : undefined
    case 'status':
      return text ? `status: ${text}` : undefined
    case 'session':
      return text ? `session: ${text}` : undefined
  }
}

function formatTranscriptPreview(snapshot: RunSnapshotView): string | undefined {
  const lines = snapshot.transcript
    .slice(-TRANSCRIPT_PREVIEW_ENTRIES)
    .map(formatTranscriptEntry)
    .filter((line): line is string => Boolean(line))
  if (lines.length === 0) return undefined
  return clipTail(lines.join('\n'), METADATA_TEXT_CHARS)
}

type HostIdentityMetadataInput = Pick<TrackedRun, 'agent' | 'model' | 'backend' | 'rosterFile' | 'rosterEntry' | 'effectiveAgent' | 'effectiveModel' | 'effectiveBackend'>

type HostIdentityStatus = Pick<StatusResult, 'roster_file' | 'roster_entry' | 'agent' | 'model' | 'backend' | 'effective_agent' | 'effective_model' | 'effective_backend'>

/** Project shared spawn identity into OpenCode metadata without resolving rosters here. */
export function hostIdentityMetadata(
  run: HostIdentityMetadataInput = {},
  status?: HostIdentityStatus,
): Record<string, unknown> {
  const metadata: Record<string, unknown> = {}
  const add = (key: string, value: unknown) => {
    if (typeof value === 'string' && value.length > 0) metadata[key] = value
  }

  add('roster_file', status?.roster_file ?? run.rosterFile)
  add('roster_entry', status?.roster_entry ?? run.rosterEntry)
  add('effective_agent', status?.effective_agent ?? status?.agent ?? run.effectiveAgent)
  add('effective_model', status?.effective_model ?? status?.model ?? run.effectiveModel)
  add('effective_backend', status?.effective_backend ?? status?.backend ?? run.effectiveBackend)
  return metadata
}

export function buildSnapshotMetadata(
  snapshot: RunSnapshotView,
  run?: HostIdentityMetadataInput,
  status?: HostIdentityStatus,
): Record<string, unknown> {
  const metadata: Record<string, unknown> = {}
  if (snapshot.phase) metadata.phase = snapshot.phase
  if (snapshot.latest_seq !== undefined) metadata.latest_seq = snapshot.latest_seq
  const liveTool = formatLiveTool(snapshot)
  if (liveTool) metadata.live_tool = liveTool
  const transcriptPreview = formatTranscriptPreview(snapshot)
  if (transcriptPreview) metadata.transcript_preview = transcriptPreview
  const pendingPermission = clipTail(
    snapshot.pending_permission?.description
      ?? snapshot.pending_permission?.question
      ?? snapshot.phase_label,
    METADATA_TEXT_CHARS,
  )
  if (pendingPermission) metadata.pending_permission = pendingPermission
  Object.assign(metadata, hostIdentityMetadata(run, status))
  return metadata
}

export function buildWaitingText(run: TrackedRun, snapshot: RunSnapshotView): string {
  const permission = snapshot.pending_permission
  const lines = [
    `Sub-agent "${run.label}" is waiting for input.`,
  ]

  if (permission) {
    lines.push(
      `Permission request: ${permission.description ?? permission.question ?? permission.tool ?? 'permission requested'}`,
      `To answer: call \`avenor_answer_permission\` with run_id "${run.runId}", option_id "allow_once" | "allow_always" | "deny"`,
    )
  } else if (snapshot.phase_label) {
    lines.push(`Question: ${snapshot.phase_label}`)
  }

  const transcriptPreview = formatTranscriptPreview(snapshot)
  if (transcriptPreview) {
    lines.push('', `Recent context:\n${transcriptPreview}`)
  }

  if (!permission) {
    lines.push(
      `Use \`avenor_message\` with action "resume" and target "${run.runId}" to respond, or \`avenor_status\` to re-check.`,
    )
  }

  return lines.join('\n')
}

function buildCompletionText(
  run: TrackedRun,
  snapshot: RunSnapshotView,
  status?: StatusResult,
  finalOutput?: string,
): string {
  const lines = [
    `Sub-agent "${run.label}" finished with status **${terminalStatus(snapshot, status)}**.`,
  ]

  const stopReason = status?.stop_reason ?? snapshot.stop_reason
  if (stopReason && stopReason.toUpperCase() !== terminalStatus(snapshot, status).toUpperCase()) {
    lines.push(`Stop reason: ${stopReason}`)
  }

  const sessionId = status?.session_id ?? snapshot.identity.session_id
  if (sessionId) {
    lines.push(`Session: \`${sessionId}\``)
  }

  const boundedOutput = clipTail(finalOutput ?? snapshot.final_output, FINAL_OUTPUT_CHARS)
  if (boundedOutput) {
    lines.push('', 'Final output:', '```text', boundedOutput, '```')
  }

  lines.push(
    '',
    `Call \`avenor_events\` with \`run_id: "${run.runId}"\` for raw events, or \`avenor_message\` with action "resume" to iterate.`,
  )

  return lines.join('\n')
}

// Channel block pattern: <channel source="agent-reviewer" from_run_id="abc123" from_role="reviewer">content</channel>
const CHANNEL_RE = /<channel\s+source="([^"]*)"(?:\s+from_run_id="([^"]*)")?(?:\s+from_role="([^"]*)")?[^>]*>([\s\S]*?)<\/channel>/g

/**
 * One inbound message's frame. The text below the header is a peer's words, so
 * it is marked as such and never read as an operator instruction. The feed's
 * trailing note for an ask names poo's call_cli transport; the preface maps
 * that instruction onto this host's own avenor_message tool without parsing
 * the peer's text, and the feed's own refusal wording passes through intact.
 */
function formatAgentMessage(source: string, fromRunId: string, content: string, messageId: string): string {
  const label = sanitize(source.replace(/^agent-/, '') || source)
  const shortId = sanitize(fromRunId ? fromRunId.slice(0, 8) : '')
  // The id arrives from the feed record, so every mention of it in this
  // trusted frame is a JSON string literal: quotes, backslashes and newlines
  // in a crafted id cannot break out of the instruction it rides in.
  const quotedId = JSON.stringify(messageId)
  const lines = [
    `📨 ${label} (${shortId})`,
    '',
    PEER_MESSAGE_NOTICE,
    '',
    ...content.trim().split('\n').map(l => `  ${l}`),
  ]
  if (messageId) {
    lines.push(
      '',
      `This message arrived through the avenor inbox (message id ${quotedId}). When its trailing note tells you to answer — it names call_cli with an avenor_message call — answer here with the avenor_message tool instead, passing the same arguments (action "reply", reply_to ${quotedId}, message "..."). A trailing note that says the message cannot be answered here means no answer is possible on this conversation.`,
    )
  }
  return lines.join('\n') + '\n'
}

function renderChannelMessage(match: string, source: string, fromRunId: string, fromRole: string, content: string): string {
  void match
  void fromRole
  return formatAgentMessage(source, fromRunId, content, '')
}

export function formatChannelBlocks(text: string): string {
  CHANNEL_RE.lastIndex = 0
  return text.replace(CHANNEL_RE, (...args: Parameters<typeof renderChannelMessage>) => {
    return renderChannelMessage(
      args[0] as string,
      args[1] as string,
      args[2] as string,
      args[3] as string,
      args[4] as string,
    )
  })
}
/**
 * The two host calls the core needs, supplied per OpenCode generation. V1 and
 * V2 adapters wrap their own client shapes onto these; everything else in the
 * plugin is shared.
 */
export interface PluginHost {
  /**
   * Deliver a user prompt into a session without waiting on the reply, and
   * reject when the host refuses it: the core owns what a refusal means, so an
   * adapter never swallows one.
   */
  promptAsync(sessionId: string, text: string): Promise<void>
  /** Read one settled message; resolve undefined when it cannot be read. */
  getMessage(
    sessionId: string,
    messageId: string,
  ): Promise<{ parts?: unknown[]; content?: unknown[] } | undefined>
  /**
   * Read the session's current native title, or undefined when it cannot be
   * read. Binding a conversation reads it once, so sessions named before the
   * plugin bound them publish their names.
   */
  getSessionTitle?(sessionId: string): Promise<string | undefined>
}

export type PermissionAsk = {
  sessionID?: string
  type?: string
  pattern?: string | string[]
  id?: string
}

export type StepEndedProjection = {
  info: Record<string, unknown>
  settledText: string[] | undefined
}

/**
 * Map a V2 structured error type onto the error-name spellings stopReasonOf
 * recognizes, so a V2 turn ends with the same canonical stop reason a V1 turn
 * would: aborted turns cancel, length errors cap, everything else is an error.
 */
function v2ErrorName(type: string): string {
  if (/abort|cancel|interrupt/i.test(type)) return 'MessageAbortedError'
  if (/length/i.test(type)) return 'MessageOutputLengthError'
  return type
}

/**
 * The text parts of one settled message, in either the V1 parts or the V2
 * content shape. Module-level so both the V1 read path and the V2 projection
 * share one extraction.
 */
export function extractTextParts(raw: unknown): string[] {
  if (!Array.isArray(raw)) return []
  return raw
    .filter((part): part is { type?: string; text?: string } => part !== null && typeof part === 'object')
    .filter(part => part.type === 'text')
    .map(part => part.text ?? '')
}

/**
 * Project one settled message onto the info shape the V1 message.updated event
 * carried. The message must be an assistant message with a completed time, or
 * the projection is undefined and nothing publishes.
 */
export function stepEndedInfo(
  message: { parts?: unknown[]; content?: unknown[] } | undefined,
  sessionID: string,
  messageId: string,
): StepEndedProjection | undefined {
  if (!message) return undefined
  const record = message as Record<string, unknown>
  const time = record.time as { created?: number; completed?: number } | undefined
  if (typeof time?.created !== 'number' || typeof time?.completed !== 'number') return undefined
  const error = record.error as { type?: string } | undefined
  const settledText = extractTextParts(message.parts ?? message.content)
  return {
    info: {
      role: 'assistant',
      sessionID,
      id: messageId,
      // The publish gate skips compaction summaries; forward the message's own
      // flag rather than assuming every step is an ordinary answer.
      summary: record.summary === true,
      time: { created: time.created, completed: time.completed },
      finish: typeof record.finish === 'string' ? record.finish : undefined,
      ...(error?.type ? { error: { name: v2ErrorName(error.type) } } : {}),
    },
    settledText,
  }
}

/**
 * The version-neutral plugin core: the V1 hooks plus the handlers the V2
 * adapter registers through its own domains.
 */
export type AvenorCore = Hooks & {
  onSessionIdle(sessionID: string): Promise<void>
  onSessionAppeared(sessionID: string, directory?: string): Promise<void>
  onSessionRenamed(sessionID: string, title: string | undefined): Promise<void>
  onMessageUpdated(info: Record<string, unknown>): Promise<void>
  onPermissionAsk(permission: PermissionAsk): Promise<void>
  onStepEnded(sessionID: string, messageId: string): Promise<void>
  onChatMessage(message: { parts?: Array<{ type?: string; text?: string }> } | undefined): void
  onTextComplete(output: { text: string }): void
  formatChannelBlocks(text: string): string
}

export function createAvenorCore(host: PluginHost, options: AvenorCoreOptions = {}): AvenorCore {
  const spawnAvenorProcess = options.spawnProcess ?? defaultSpawnProcess
  const runAvenorCommand = options.runCommand ?? defaultRunCommand
  const readGitToplevel = options.gitToplevel ?? gitToplevel
  const trackedRuns = new Map<string, TrackedRun>()
  const activeMonitors = new Map<string, ActiveMonitor>()
  const sessionIdToRunId = new Map<string, string>()
  const runtimeIdToRunId = new Map<string, string>()

  function unregisterRun(runId: string): void {
    trackedRuns.delete(runId)
    for (const [sessionId, mappedRunId] of [...sessionIdToRunId.entries()]) {
      if (mappedRunId === runId) sessionIdToRunId.delete(sessionId)
    }
    for (const [runtimeId, mappedRunId] of [...runtimeIdToRunId.entries()]) {
      if (mappedRunId === runId) runtimeIdToRunId.delete(runtimeId)
    }
  }

  function registerSessionId(sessionId: string | undefined, runtimeId: string | undefined, runId: string): void {
    const run = trackedRuns.get(runId)
    if (run) {
      if (sessionId) run.sessionId = sessionId
      if (runtimeId) run.runtimeId = runtimeId
    }
    if (sessionId && !sessionIdToRunId.has(sessionId)) {
      sessionIdToRunId.set(sessionId, runId)
    }
    if (runtimeId && !runtimeIdToRunId.has(runtimeId)) {
      runtimeIdToRunId.set(runtimeId, runId)
    }
  }

  function syncRunSnapshot(run: TrackedRun, snapshot: RunSnapshotView): void {
    run.lastSnapshot = snapshot
    if (snapshot.latest_seq !== undefined) run.latestSeq = snapshot.latest_seq
    registerSessionId(snapshot.identity.session_id, snapshot.identity.runtime_id, run.runId)
  }

  function updateRunIdentity(run: TrackedRun, status: StatusResult): void {
    run.rosterFile = status.roster_file ?? run.rosterFile
    run.rosterEntry = status.roster_entry ?? run.rosterEntry
    run.effectiveAgent = status.effective_agent ?? status.agent ?? run.effectiveAgent
    run.effectiveModel = status.effective_model ?? status.model ?? run.effectiveModel
    run.effectiveBackend = status.effective_backend ?? status.backend ?? run.effectiveBackend
    run.agent = run.effectiveAgent ?? run.agent
    run.model = run.effectiveModel ?? run.model
    run.backend = run.effectiveBackend ?? run.backend
  }

  async function primeTrackedRun(run: TrackedRun, options: { captureLatestSeq?: boolean } = {}): Promise<StatusResult | undefined> {
    try {
      const raw = await statusTool({ runId: run.runId, supervisorId: run.supervisorId })
      const status = Array.isArray(raw) ? raw[0] : raw
      if (!status) return undefined
      registerSessionId(status.session_id, status.runtime_id, run.runId)
      updateRunIdentity(run, status)
      if (status.latest_seq !== undefined && options.captureLatestSeq) {
        run.latestSeq = status.latest_seq
      }
      return status
    } catch {
      return undefined
    }
  }

  async function createObserver(run: TrackedRun): Promise<RunObserver> {
    if (!run.supervisorId) {
      run.supervisorId = Supervisor.currentInstance()?.supervisorId
    }
    if (!run.runtimeId) {
      await primeTrackedRun(run)
    }
    if (!run.supervisorId || !run.runtimeId) {
      throw new Error(`unable to observe run ${run.runId}`)
    }
    const client = await dial(run.supervisorId)
    return observeRun(client, {
      runtime_id: run.runtimeId,
      replay: true,
      ...(run.latestSeq !== undefined ? { after_seq: run.latestSeq } : {}),
      close_client: true,
    })
  }

  async function stopMonitor(runId: string, reason: MonitorStopReason): Promise<void> {
    const active = activeMonitors.get(runId)
    if (!active) return
    active.stopReason = active.stopReason ?? reason
    await active.observer?.close().catch(() => {})
  }

  async function loadInspection(run: TrackedRun): Promise<Awaited<ReturnType<typeof inspectTool>> | undefined> {
    try {
      const inspection = await inspectTool({
        runId: run.runId,
        supervisorId: run.supervisorId,
      })
      updateRunIdentity(run, inspection.status)
      return inspection
    } catch {
      return undefined
    }
  }

  async function observeTrackedRun(
    run: TrackedRun,
    options: {
      abort?: AbortSignal
      onSnapshot?: (snapshot: RunSnapshotView) => void
    } = {},
  ): Promise<MonitorOutcome> {
    const existing = activeMonitors.get(run.runId)
    if (existing) return existing.promise

    run.monitoring = true
    const handle: ActiveMonitor = { promise: Promise.resolve({ kind: 'stopped', snapshot: run.lastSnapshot ?? createRunSnapshot() }) }

    handle.promise = (async () => {
      let observer: RunObserver | undefined
      let unsubscribe: (() => void) | undefined
      let abortListener: (() => void) | undefined

      try {
        observer = await createObserver(run)
        handle.observer = observer

        if (handle.stopReason) {
          await observer.close()
          const current = observer.snapshot()
          syncRunSnapshot(run, current)
          if (handle.stopReason === 'permission-routed') {
            return { kind: 'waiting', snapshot: current, notified: true }
          }
          if (handle.stopReason === 'abort') {
            return { kind: 'aborted', snapshot: current }
          }
          if (handle.stopReason === 'result') {
            return { kind: 'stopped', snapshot: current }
          }
          return { kind: 'shutdown', snapshot: current }
        }

        if (options.abort) {
          const onAbort = () => {
            void stopMonitor(run.runId, 'abort')
          }
          options.abort.addEventListener('abort', onAbort, { once: true })
          abortListener = () => options.abort?.removeEventListener('abort', onAbort)
          if (options.abort.aborted) {
            await stopMonitor(run.runId, 'abort')
          }
        }

        unsubscribe = observer.subscribe((snapshot) => {
          syncRunSnapshot(run, snapshot)
          options.onSnapshot?.(snapshot)
        })

        while (true) {
          const snapshot = await observer.next()
          if (!snapshot) {
            const current = observer.snapshot()
            syncRunSnapshot(run, current)
            if (handle.stopReason === 'permission-routed') {
              return { kind: 'waiting', snapshot: current, notified: true }
            }
            if (handle.stopReason === 'abort') {
              return { kind: 'aborted', snapshot: current }
            }
            if (handle.stopReason === 'shutdown') {
              return { kind: 'shutdown', snapshot: current }
            }
            if (handle.stopReason === 'result') {
              return { kind: 'stopped', snapshot: current }
            }
            if (current.pending_permission || current.phase === 'waiting') {
              return { kind: 'waiting', snapshot: current }
            }
            if (current.ended) {
              return { kind: 'terminal', snapshot: current }
            }
            return { kind: 'stopped', snapshot: current }
          }

          syncRunSnapshot(run, snapshot)
          if (snapshot.pending_permission || snapshot.phase === 'waiting') {
            return { kind: 'waiting', snapshot }
          }
          if (snapshot.ended) {
            return { kind: 'terminal', snapshot }
          }
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        const snapshot = observer?.snapshot() ?? run.lastSnapshot ?? {
          ...createRunSnapshot(),
          identity: {
            run_id: run.runId,
            run_label: run.label,
            runtime_id: run.runtimeId,
            agent: run.agent,
          },
        }
        syncRunSnapshot(run, snapshot)
        return { kind: 'error', snapshot, error: message }
      } finally {
        abortListener?.()
        unsubscribe?.()
        await observer?.close().catch(() => {})
        if (activeMonitors.get(run.runId) === handle) {
          activeMonitors.delete(run.runId)
        }
        run.monitoring = false
      }
    })()

    activeMonitors.set(run.runId, handle)
    return handle.promise
  }

  /**
   * The reason a failed prompt names. A host can reject with an error whose
   * message is empty, so a named error falls back to its name and anything
   * else to a fixed description rather than logging a bare colon.
   */
  function promptFailureText(error: unknown): string {
    if (error instanceof Error) {
      if (error.message) return error.message
      if (error.name && error.name !== 'Error') return error.name
      return 'the host refused the prompt'
    }
    return typeof error === 'string' && error ? error : 'the host refused the prompt'
  }

  /**
   * A notification prompt is best effort: a host that refuses it is reported
   * and its caller carries on, because a failed notice must not change what
   * the tracking state does next. Feed delivery is a different path — a record
   * nobody accepted stops the feed that produced it.
   */
  async function notifySession(sessionId: string, text: string): Promise<void> {
    try {
      await host.promptAsync(sessionId, text)
    } catch (error) {
      console.error(`[avenor] notification failed for session ${sessionId}:`, promptFailureText(error))
    }
  }

  async function monitorRun(run: TrackedRun): Promise<void> {
    const outcome = await observeTrackedRun(run)

    if (outcome.kind === 'error') {
      await notifySession(
        run.orchestratorSessionId,
        `Monitoring of "${run.label}" (run_id: ${run.runId}) failed: ${outcome.error ?? 'unknown error'}. The run may still be active — use avenor_status to check.`,
      )
      return
    }

    if (outcome.kind === 'waiting') {
      if (!outcome.notified) {
        await notifySession(run.orchestratorSessionId, buildWaitingText(run, outcome.snapshot))
      }
      return
    }

    if (outcome.kind === 'terminal') {
      const inspection = await loadInspection(run)
      const snapshot = inspection?.snapshot ?? outcome.snapshot
      await notifySession(
        run.orchestratorSessionId,
        buildCompletionText(run, snapshot, inspection?.status, inspection?.final_output),
      )
      unregisterRun(run.runId)
      return
    }

    if (outcome.kind === 'aborted' || outcome.kind === 'shutdown') {
      unregisterRun(run.runId)
    }
  }

  async function stopTrackedRuns(predicate: (run: TrackedRun) => boolean): Promise<void> {
    const runs = [...trackedRuns.values()].filter(predicate)
    await Promise.all(runs.map(async (run) => {
      await stopMonitor(run.runId, 'shutdown')
      unregisterRun(run.runId)
    }))
  }

  // One broker run per conversation: the run its own bind minted. Messaging
  // and the one inbound consumer act as that run and nothing else.
  const conversationBindings = new Map<string, ConversationBinding>()
  // The per-conversation messaging provider: built at bind time around the
  // shared broker adapter, so tell, ask, withdraw, peers, rename and last all
  // speak as this conversation. Replies are absent from it on purpose: a
  // reply's authorization is the feed's Go claim store, so the reply action
  // runs the `avenor message` CLI instead of this provider.
  const conversationProviders = new Map<string, MessageProvider>()
  const bindInFlight = new Map<string, Promise<ConversationBinding>>()
  const conversationFeeds = new Map<string, FeedProcess>()
  // The consumer a session may not replace yet: its process still holds the
  // feed's lock, or the records it already read are still delivering. A
  // replacement that started before either finished would take the inbox as a
  // duplicate or overtake the older record it had not delivered.
  const handedOffFeeds = new Map<string, FeedProcess>()
  // The consumer a session is waiting to start at that close. One per session:
  // a later bind retargets it rather than queueing a second.
  const pendingFeeds = new Map<string, { binding: ConversationBinding; epoch: number }>()
  // The owner client the current bindings were minted under. A different
  // client means a fresh supervisor singleton, so every binding names a run
  // on a broker that no longer exists and must not be reused.
  let bindingsClient: unknown | undefined
  // Bumped whenever every binding is dropped. A bind or a handoff that started
  // under an older epoch is stale even when its client object is unchanged, so
  // a late answer can never revive a stopped feed.
  let bindEpoch = 0

  /**
   * Retire one consumer, and free its slot only while the slot still names it.
   * A named feed is retired even when a replacement already owns the slot: its
   * queued rows must not land after its own delivery was refused. Its
   * replacement is never killed and never waited on by this path.
   */
  function retireFeed(sessionId: string, feed: FeedProcess): void {
    feed.retire()
    if (conversationFeeds.get(sessionId) === feed) {
      conversationFeeds.delete(sessionId)
      feed.child.kill()
      // The replacement waits for this exact process's close: until then the
      // dead consumer still holds the inbox lock.
      if (!feed.released) handedOffFeeds.set(sessionId, feed)
    }
    // A retired consumer's queued rows are already dropped, so only its close
    // is still owed; a close that already landed releases the inbox now.
    feed.settle()
  }

  function stopFeed(sessionId: string, feed?: FeedProcess): void {
    const target = feed ?? conversationFeeds.get(sessionId)
    if (!target) return
    retireFeed(sessionId, target)
  }

  /**
   * Drop every conversation binding and its consumer. The supervisor the
   * bindings were minted under is gone, so each run's credential is dead and
   * nothing of it — provider, feed, inbox — may serve another turn.
   */
  function resetConversationBindings(): void {
    // Every bind or handoff still in flight belongs to the identity dropped
    // here, so it is stale from this point on whatever client object it holds.
    bindEpoch++
    // Nothing queued for the dropped identity may start, even from a feed whose
    // close is still outstanding.
    pendingFeeds.clear()
    for (const sessionId of [...conversationFeeds.keys()]) stopFeed(sessionId)
    // A consumer whose process already closed still waiting on its last
    // delivery is nobody's slot any more, but it belongs to the same dead
    // identity: drop its queue too. Its close (or the close already seen) is
    // still what frees the inbox for whatever binds next.
    for (const feed of [...handedOffFeeds.values()]) {
      feed.retire()
      feed.child.kill()
      feed.settle()
    }
    conversationBindings.clear()
    conversationProviders.clear()
    bindInFlight.clear()
    conversationTurns.clear()
    publishedTitles.clear()
    refusedTitles.clear()
  }

  /** The per-conversation messaging provider over the shared broker adapter. */
  function buildConversationProvider(binding: ConversationBinding): MessageProvider {
    return createChildMessageProvider({
      surface: 'opencode',
      runId: binding.runId,
      broker: createBrokerChildBroker({
        brokerUrl: binding.brokerUrl,
        runId: binding.runId,
        token: binding.runToken,
      }),
    })
  }

  async function bindConversation(sessionId: string, opts: { reanswer?: boolean } = {}): Promise<ConversationBinding> {
    // The identity this call was made under. A reset that lands while the owner
    // client lookup is in flight drops every binding that call could mint, and
    // the answer may name the same client object, so only this epoch proves it
    // is stale.
    const lookupEpoch = bindEpoch
    // Resolving the owner client here is the safe local bootstrap: the same
    // lazily started singleton every avenor tool uses. It also carries the
    // restart signal: a different client means a fresh singleton whose broker
    // no longer holds the old runs, so every stale binding must go.
    const { client, supervisorId } = await getSupervisorClient(undefined, { owner: true })
    // A reset raced even the lookup: the shutdown already dropped this
    // conversation, so its late answer must not bind a run or revive a feed.
    if (lookupEpoch !== bindEpoch) {
      throw new Error('conversation bind superseded by a supervisor restart')
    }
    // A lookup that itself discovered a fresh singleton dropped the stale
    // bindings above, so the bind it is about to mint belongs to the new epoch.
    if (bindingsClient !== undefined && bindingsClient !== client) resetConversationBindings()
    bindingsClient = client
    if (!opts.reanswer) {
      const cached = conversationBindings.get(sessionId)
      if (cached) return cached
    }
    const inFlight = bindInFlight.get(sessionId)
    if (inFlight) return inFlight
    // The binding this rebind replaces, if any: a re-answer that lands on a
    // different run is a new identity, and the old run's per-session records
    // must not carry over.
    const previous = conversationBindings.get(sessionId)
    const epoch = bindEpoch
    const promise = (async () => {
      const bound = await client.messageBind({ host: 'opencode', session_id: sessionId })
      // A reset raced this bind: the dropped bindings win even though this call
      // still holds the same client object, so the stale credentials are
      // refused, never served, cached or used to start a consumer.
      if (bindingsClient !== client || epoch !== bindEpoch) {
        throw new Error('conversation bind superseded by a supervisor restart')
      }
      const binding: ConversationBinding = {
        sessionId,
        supervisorId,
        brokerUrl: bound.broker_url,
        runId: bound.run_id,
        runToken: bound.run_token,
        generation: bound.generation,
      }
      if (previous && previous.runId !== binding.runId) {
        // A different run is a new identity: the old run's turn record and
        // name do not carry over, and the fresh run is named and claimed from
        // scratch. A same-run re-answer keeps both.
        conversationTurns.delete(sessionId)
        publishedTitles.delete(sessionId)
        refusedTitles.delete(sessionId)
      }
      conversationBindings.set(sessionId, binding)
      conversationProviders.set(sessionId, buildConversationProvider(binding))
      ensureFeed(sessionId, binding, epoch)
      void publishCurrentTitle(sessionId)
      return binding
    })()
    bindInFlight.set(sessionId, promise)
    void promise.catch(() => {}).then(() => {
      if (bindInFlight.get(sessionId) === promise) bindInFlight.delete(sessionId)
    })
    return promise
  }

  /**
   * Re-bind an already-bound conversation whose feed has died: the bind is
   * re-answered (or re-minted, after a supervisor restart), the credentials
   * switch only when the run changed, and the feed starts again. A session
   * that never opted into messaging never binds here, so an idle alone mints
   * nothing. A bind failure is logged and retried at the next idle: a
   * conversation without a bind has no inbox and never falls back to another
   * run's identity.
   */
  async function restartConversationFeed(sessionId: string): Promise<void> {
    if (!conversationBindings.has(sessionId) || conversationFeeds.has(sessionId) || pendingFeeds.has(sessionId)) return
    // A dead consumer forces a re-answer of the bind, so a reclaimed or
    // re-minted run is noticed and its new identity takes over; a same-run
    // re-answer keeps the session's turn and name records.
    await bindConversation(sessionId, { reanswer: true }).catch(error => {
      console.error(`[avenor] conversation rebind failed for session ${sessionId}:`, error instanceof Error ? error.message : error)
    })
  }

  /**
   * The provider a messaging or peers call routes through: the bound
   * conversation speaks as its own run for every such action. A bind that
   * cannot land leaves the conversation with no participant, so the tool
   * fails closed with the shared refusal instead of falling back to another
   * run's identity.
   */
  async function conversationToolProvider(sessionId: string): Promise<MessageProvider> {
    try {
      await bindConversation(sessionId)
    } catch (error) {
      throw new ToolError(
        'refused',
        `the conversation's broker binding is unavailable: ${error instanceof Error ? error.message : String(error)}`,
      )
    }
    // A reset that raced the bind leaves no provider for this session; the
    // call fails closed rather than building one around stale credentials.
    const provider = conversationProviders.get(sessionId)
    if (!provider) {
      throw new ToolError('refused', "the conversation's broker binding is unavailable")
    }
    return provider
  }

  /** The conversation run's delivery environment, or undefined before a bind. */
  function conversationDelivery(sessionId: string): { brokerUrl: string; runId: string; token: string } | undefined {
    const binding = conversationBindings.get(sessionId)
    if (!binding) return undefined
    return { brokerUrl: binding.brokerUrl, runId: binding.runId, token: binding.runToken }
  }

  /**
   * Whether a spawn is routed to the supervisor that holds this conversation's
   * bound run — the only broker where naming that run as the spawn parent is
   * meaningful. Identity is the supervisor's socket path, canonicalized and
   * compared by the shared supervisor class, which is the same test the shared
   * spawn tool makes before reusing the live singleton; a label or a caller's
   * spelling of a path never matches. A spawn routed anywhere else runs
   * without a parent route: that broker would mint a parent record nothing
   * serves, so the child is told no parent it could never reach.
   */
  function spawnSharesConversationSupervisor(
    binding: ConversationBinding,
    requestedSupervisorId: string | undefined,
  ): boolean {
    if (requestedSupervisorId === undefined) return true
    if (!Supervisor.isCurrentInstance(requestedSupervisorId)) return false
    return Supervisor.currentInstance()?.supervisorId === binding.supervisorId
  }

  /**
   * Whether a shutdown names the supervisor this plugin's conversation bindings
   * were minted on. Identity is the canonical socket path compared by the
   * shared supervisor class, never the caller's spelling or a label; a shutdown
   * aimed elsewhere says nothing about this plugin's conversations.
   */
  function shutdownDropsConversations(requestedSupervisorId: string | undefined): boolean {
    if (requestedSupervisorId === undefined) return true
    const live = Supervisor.currentInstance()
    if (!live) return false
    if (!Supervisor.isCurrentInstance(requestedSupervisorId)) return false
    for (const binding of conversationBindings.values()) {
      if (binding.supervisorId !== live.supervisorId) return false
    }
    return true
  }

  /** The credentials a spawned feed or `avenor message` call authenticates with.
   *
   * Every inherited AVENOR_* variable — including unrelated owner or
   * supervisor credentials the host process may carry — is scrubbed: the child
   * authenticates only as its own bound run through the trio set here. All
   * other variables are preserved, so noncredential wiring like
   * XDG_STATE_HOME still puts the feed's claim records in the same default
   * root the reply path reads.
   */
  function childEnv(delivery: { brokerUrl: string; runId: string; token: string }): NodeJS.ProcessEnv {
    const env: NodeJS.ProcessEnv = {}
    for (const [key, value] of Object.entries(process.env)) {
      if (key.startsWith('AVENOR_')) continue
      env[key] = value
    }
    env.AVENOR_BROKER_URL = delivery.brokerUrl
    env.AVENOR_RUN_ID = delivery.runId
    env.AVENOR_BROKER_TOKEN = delivery.token
    return env
  }

  /**
   * Run one `avenor message` reply call as the bound conversation. The CLI's
   * router claim-guards the reply against the feed's Go claim store, so a
   * reply is answerable exactly when the feed delivered the ask here. The
   * token travels in the child's environment and the reply text on its stdin,
   * so neither is readable in the child's argv.
   * An interrupted call is an uncertainty, not an absence: the edge may have
   * completed, so the result reports possibly-pending.
   */
  async function replyViaCli(
    sessionId: string,
    args: { reply_to?: unknown; message?: unknown },
    signal?: AbortSignal,
  ): Promise<ProviderResult> {
    const delivery = conversationDelivery(sessionId)
    if (!delivery) {
      // conversationToolProvider already bound this session for any call that
      // reaches here, so this is a race with a reset, not a missing bind.
      throw new ToolError('refused', "the conversation's broker binding is unavailable")
    }
    // A call that arrived already cancelled starts no process and sends
    // nothing: the ask edge was never created, so not-sent is the honest
    // report rather than an uncertainty about a send that never happened.
    if (signal?.aborted) {
      return { action: 'reply', outcome: 'not-sent', messageId: String(args.reply_to ?? '') }
    }
    const argv = [
      findAvenorBinary(),
      'message',
      '--action=reply',
      `--reply-to=${String(args.reply_to ?? '')}`,
      // The text itself never appears here: the CLI reads it from the child's
      // stdin, which keeps it out of argv for every local process to read.
      '--message-stdin',
    ]
    const text = typeof args.message === 'string' ? args.message : ''
    const { code, stdout } = await runAvenorCommand(argv, childEnv(delivery), signal, text)
    let parsed: Record<string, unknown> = {}
    try { parsed = JSON.parse(stdout) as Record<string, unknown> } catch { parsed = {} }
    if (code === null) {
      return { action: 'reply', outcome: 'possibly-pending', messageId: String(args.reply_to ?? '') }
    }
    if (code !== 0) {
      const codeName = typeof parsed.code === 'string' && parsed.code ? parsed.code : 'refused'
      const text = typeof parsed.text === 'string' && parsed.text ? parsed.text : 'the reply was refused'
      throw new ToolError(codeName, text)
    }
    return {
      action: 'reply',
      outcome: (typeof parsed.outcome === 'string' ? parsed.outcome : 'unknown') as ProviderResult['outcome'],
      messageId: typeof parsed.message_id === 'string' && parsed.message_id ? parsed.message_id : String(args.reply_to ?? ''),
      ...(typeof parsed.reply === 'string' ? { reply: parsed.reply } : {}),
      ...(typeof parsed.reply_id === 'string' ? { replyId: parsed.reply_id } : {}),
      ...(typeof parsed.text === 'string' ? { text: parsed.text } : {}),
    }
  }

  /**
   * The one consumer of a conversation's inbox: the real `avenor feed broker`
   * child, authenticated as the conversation's own run. Its lock keeps a
   * second consumer out, its parent-loss detection ends it with the host, and
   * its records arrive as JSON lines on stdout. A feed that exits is logged;
   * the next idle rebinds explicitly rather than respawning in a loop.
   */
  function ensureFeed(sessionId: string, binding: ConversationBinding, epoch: number): void {
    if (epoch !== bindEpoch) return
    const existing = conversationFeeds.get(sessionId)
    if (existing) {
      if (existing.runId === binding.runId && existing.brokerUrl === binding.brokerUrl) return
      stopFeed(sessionId, existing)
    }
    // A previous consumer's process is still closing and holds the inbox lock:
    // the replacement is handed off to exactly that close, and a later bind
    // retargets the one consumer waiting there.
    if (handedOffFeeds.has(sessionId)) {
      pendingFeeds.set(sessionId, { binding, epoch })
      return
    }
    spawnFeed(sessionId, binding, epoch)
  }

  /**
   * Start the consumer a session is waiting to run once the previous one is
   * gone and its last delivery landed. The handoff is one close event and one
   * queued consumer: nothing polls, and a reset drops the queue before it can
   * start.
   */
  function startPendingFeed(sessionId: string): void {
    if (handedOffFeeds.has(sessionId)) return
    const pending = pendingFeeds.get(sessionId)
    if (!pending) return
    pendingFeeds.delete(sessionId)
    spawnFeed(sessionId, pending.binding, pending.epoch)
  }

  /** Start one consumer, or do nothing when the binding it serves is stale. */
  function spawnFeed(sessionId: string, binding: ConversationBinding, epoch: number): void {
    if (epoch !== bindEpoch) return
    const child = spawnAvenorProcess(
      [findAvenorBinary(), 'feed', 'broker'],
      childEnv({ brokerUrl: binding.brokerUrl, runId: binding.runId, token: binding.runToken }),
    )
    // Records are delivered one at a time in arrival order: the reader pauses
    // the feed's stdout for the duration of a delivery, so the next record is
    // never offered to the host while the previous prompt is still open and
    // the lines waiting are only those a single read chunk already carried.
    // Nothing polls here — the paused stream is the backpressure.
    const pending: string[] = []
    let retired = false
    let draining = false
    let readerClosed = false
    const reader = child.stdout ? readline.createInterface({ input: child.stdout }) : undefined
    const settle = (): void => {
      // A process that has not closed still holds the inbox lock.
      if (!feed.released) return
      // A consumer that ended on its own keeps the inbox until the record it
      // was delivering lands: its replacement's newer record must not overtake
      // it. A retired consumer's queue is already dropped, so it owes only the
      // close above.
      if (!retired && draining) return
      const ownsSlot = conversationFeeds.get(sessionId) === feed
      const handedOff = handedOffFeeds.get(sessionId) === feed
      // A consumer a replacement already superseded owns neither.
      if (!ownsSlot && !handedOff) return
      if (ownsSlot) conversationFeeds.delete(sessionId)
      if (handedOff) handedOffFeeds.delete(sessionId)
      startPendingFeed(sessionId)
    }
    const feed: FeedProcess = {
      child,
      runId: binding.runId,
      brokerUrl: binding.brokerUrl,
      released: false,
      retire() {
        retired = true
        pending.length = 0
        reader?.close()
      },
      settle,
    }
    reader?.on('close', () => {
      readerClosed = true
    })
    conversationFeeds.set(sessionId, feed)
    /** A consumer that already owns the slot is this one's replacement. */
    const superseded = (): boolean => {
      const owned = conversationFeeds.get(sessionId)
      return owned !== undefined && owned !== feed
    }
    const drain = async (): Promise<void> => {
      if (draining) return
      draining = true
      try {
        while (!retired && !superseded() && pending.length > 0) {
          await deliverFeedRecord(sessionId, feed, pending.shift() ?? '')
        }
      } finally {
        draining = false
        // A retired or replaced feed stays paused: nothing of it may be read
        // or delivered any more. A reader already closed by its process ending
        // has no line left to resume, so asking it to would throw.
        if (!retired && !superseded() && !readerClosed) reader?.resume()
        // The old process is closed and its last queued record has landed: the
        // replacement waiting behind it may now claim the inbox in order.
        settle()
      }
    }
    reader?.on('line', line => {
      pending.push(line)
      reader.pause()
      // A delivery that throws out of its own handling stops this consumer, as
      // a refused prompt does: the feed it came from is the one at fault.
      void drain().catch(error => {
        console.error(`[avenor] feed delivery failed for session ${sessionId}:`, error instanceof Error ? error.message : error)
        stopFeed(sessionId, feed)
      })
    })
    // A spawn that fails to start (a missing or non-executable binary, for
    // instance) must not reach the host as an unhandled 'error' event; the
    // slot is freed like any other dead consumer so the next idle rebinds. A
    // process that never started holds no lock, so no replacement waits on it.
    child.on('error', error => {
      console.error(`[avenor] feed ${binding.runId} failed to start:`, error instanceof Error ? error.message : error)
      feed.released = true
      stopFeed(sessionId, feed)
      // Nothing was ever launched, so no lock is held and a waiting replacement
      // does not need the close event this process may never emit.
      startPendingFeed(sessionId)
    })
    // Feed diagnostics arrive as lines, possibly split across chunks, so they
    // are buffered here and one complete line is handled at a time. A terminal
    // diagnostic split across chunks is still recognized.
    let stderrPending = ''
    child.stderr?.on('data', chunk => {
      stderrPending += String(chunk)
      const complete = stderrPending.split('\n')
      stderrPending = complete.pop() ?? ''
      for (const raw of complete) {
        handleFeedDiagnostic(sessionId, feed, binding.runId, raw)
      }
    })
    child.on('close', () => {
      if (stderrPending.trim()) handleFeedDiagnostic(sessionId, feed, binding.runId, stderrPending)
      stderrPending = ''
      // The process is gone and its inbox lock released. The rows it already
      // read may still be delivering, so the session waits on this consumer
      // until both are done rather than letting a replacement overtake them.
      feed.released = true
      if (conversationFeeds.get(sessionId) === feed) {
        conversationFeeds.delete(sessionId)
        handedOffFeeds.set(sessionId, feed)
      }
      settle()
    })
    child.on('exit', (code, signalName) => {
      // Only the feed this slot still names logs its exit: a replaced or reset
      // consumer's death is its replacement's business.
      if (conversationFeeds.get(sessionId) === feed) {
        conversationFeeds.delete(sessionId)
        console.error(`[avenor] feed for session ${sessionId} exited (code ${code ?? 'null'}${signalName ? ` signal ${signalName}` : ''}); the next idle rebinds`)
      }
      // Exit is not EOF: the reader stays open so a record already written to
      // the feed's stdout is still read and delivered. The process's close is
      // what releases the inbox for a replacement.
      if (!feed.released) handedOffFeeds.set(sessionId, feed)
    })
  }

  /**
   * One buffered feed diagnostic line. The feed's terminal diagnostic is
   * authoritative: the broker refused this consumer for good and the feed
   * parks without polling again, so leaving it running would advertise an
   * identity no consumer serves. Stop exactly this owned feed and let the
   * next session activity rebind. Matching is the stable prefix the feed
   * itself writes, never parsed peer text, and a replacement's slot is never
   * touched.
   */
  function handleFeedDiagnostic(sessionId: string, feed: FeedProcess, runId: string, raw: string): void {
    const line = raw.trim()
    if (!line) return
    if (line.startsWith('feed broker: terminal')) {
      if (conversationFeeds.get(sessionId) === feed) {
        console.error(`[avenor] feed ${runId} retired: ${line}; the next idle rebinds`)
        stopFeed(sessionId, feed)
      }
      return
    }
    console.error(`[avenor] feed ${runId}: ${line}`)
  }

  /** Deliver one feed record into its session, rendered as an inbound frame. */
  async function deliverFeedRecord(sessionId: string, feed: FeedProcess, line: string): Promise<void> {
    let record: { text?: unknown; from?: unknown; id?: unknown }
    try { record = JSON.parse(line) as typeof record } catch { return }
    const text = typeof record.text === 'string' ? record.text : ''
    const id = typeof record.id === 'string' ? record.id : ''
    const from = typeof record.from === 'string' && record.from ? record.from : 'unknown'
    if (!text || !id) return
    // Only a run this host tracks supplies a name it knows; the row's sender
    // field is the broker's stamp, but a peer must not be able to name itself
    // in the header the model reads.
    const resolvedRunId = trackedRuns.has(from) ? from
      : sessionIdToRunId.get(from)
      ?? runtimeIdToRunId.get(from)
    const runInfo = resolvedRunId ? trackedRuns.get(resolvedRunId) : undefined
    const frame = formatAgentMessage(runInfo?.agent ?? 'agent', from, text, id)
    try {
      await host.promptAsync(sessionId, frame)
    } catch (error) {
      // A delivery failure stops the feed it arrived from, and only that one:
      // a stale feed's late failure must never stop its replacement.
      console.error(`[avenor] prompt failed for session ${sessionId}; stopping its feed:`, error instanceof Error ? error.message : error)
      stopFeed(sessionId, feed)
    }
  }

  // The owner authority: one provider over this plugin's owner connection,
  // rebuilt when the supervisor's client changes (a shutdown followed by a
  // fresh singleton, or a closed socket), so a revoked generation is never
  // reused. It serves only owned execution and resume; messaging and peers
  // ride the bound conversation's own identity instead.
  let ownerProvider: { client: unknown; binding: OwnerBinding } | undefined
  // Bumped by every owner bind that has to preflight. A bind whose sequence is
  // stale by the time its preflight resolves raced a later one, and its client
  // may name a generation the supervisor has already revoked.
  let ownerBindSeq = 0

  async function ownerHostProvider(): Promise<OwnerBinding> {
    const { client, supervisorId } = await getSupervisorClient(undefined, { owner: true })
    if (ownerProvider && ownerProvider.client === client) return ownerProvider.binding
    const seq = ++ownerBindSeq
    const preflight = await client.messagePreflight()
    // A later bind started while this one preflighted, so this client may name
    // a generation the supervisor already revoked. The winner's binding is what
    // every later call must use; with no winner cached yet, the binding below
    // serves only this call and is never cached, so the client cannot revive.
    const stale = seq !== ownerBindSeq
    if (stale && ownerProvider) return ownerProvider.binding
    const binding: OwnerBinding = {
      provider: createHostMessageProvider({
        surface: 'opencode',
        supervisorId,
        generation: preflight.generation,
        client,
      }),
      supervisorId,
      generation: preflight.generation,
    }
    if (stale) return binding
    ownerProvider = { client, binding }
    return binding
  }

  /**
   * The provider a resume routes through: resume keeps the owner authority —
   * a broker-authenticated run may continue only a run it spawned, and the
   * bound conversation holds messaging credentials only. The resumed run
   * belongs to whichever session resumed it, so its monitor prompts that
   * session rather than any host-bound one.
   */
  async function resumeProvider(sessionId: string): Promise<MessageProvider> {
    const owner = await ownerHostProvider()
    return {
      ...owner.provider,
      onResumed: (result: ProviderResult) => trackResumed(sessionId, owner.supervisorId, result),
    }
  }

  async function trackResumed(sessionId: string, supervisorId: string, result: ProviderResult): Promise<void> {
    const run: TrackedRun = {
      runId: result.runId ?? '',
      orchestratorSessionId: sessionId,
      label: result.label ?? result.runId ?? '',
      supervisorId,
      monitoring: false,
    }
    trackedRuns.set(run.runId, run)
    await primeTrackedRun(run)
    void monitorRun(run).catch(console.error)
  }

  // The conversation's own turn producer: each session publishes its settled
  // assistant turns as its own bound run through the broker's peer publish
  // route, ordered by a per-conversation sequence, so peers read last turns of
  // the conversation they name.
  type TurnState = {
    sequence: number
    chain: Promise<void>
    publishedTurn?: { created: number; id: string }
  }
  const conversationTurns = new Map<string, TurnState>()

  function turnState(sessionId: string): TurnState {
    let state = conversationTurns.get(sessionId)
    if (!state) {
      state = { sequence: 0, chain: Promise.resolve() }
      conversationTurns.set(sessionId, state)
    }
    return state
  }

  function isNewerTurn(published: { created: number; id: string }, created: number, id: string): boolean {
    if (created !== published.created) return created > published.created
    return id > published.id
  }

  /** The raw stop reason one settled OpenCode assistant message reports. */
  function stopReasonOf(info: Record<string, unknown>): string {
    const error = info.error
    if (error !== null && typeof error === 'object') {
      const name = (error as Record<string, unknown>).name
      if (name === 'MessageAbortedError') return 'aborted'
      if (name === 'MessageOutputLengthError') return 'length'
      return 'error'
    }
    const finish = typeof info.finish === 'string' ? info.finish.trim() : ''
    return finish === '' ? 'end_turn' : finish
  }

  /**
   * Translate the ai-sdk finish spellings onto the words the shared normaliser
   * knows. They are adapter vocabulary, not pi spellings, so they are mapped
   * here rather than in the shared table a Pi child's session.end also uses.
   */
  function normalizeFinish(stopReason: string): string {
    switch (stopReason) {
      case 'tool-calls':
      case 'tool_calls':
      case 'toolUse':
        return 'tool_use'
      case 'end-turn':
        return 'end_of_turn'
      default:
        return stopReason
    }
  }

  /**
   * The text parts of one assistant message, read by id. The read is bounded to
   * the message the event carried, never the session's whole history.
   */
  async function settledTurnText(sessionId: string, messageId: string): Promise<string[]> {
    const message = await host.getMessage(sessionId, messageId)
    const raw = message?.parts ?? message?.content
    return extractTextParts(raw)
  }

  // The conversation's own last-turn producer. It publishes the turn the event
  // carries for any bound session, only once the assistant message has
  // completed. A failure is logged, never rethrown.
  async function publishHostTurn(info: Record<string, unknown>, settledText?: string[]): Promise<void> {
    if (info.role !== 'assistant') return
    // A compaction summary is an assistant message on the same session that
    // also completes; it is the context OpenCode folded away, not an answer.
    if (info.summary === true) return
    const time = info.time as Record<string, unknown> | undefined
    if (typeof time?.completed !== 'number') return
    const sessionId = typeof info.sessionID === 'string' ? info.sessionID : ''
    if (sessionId === '') return
    const created = typeof time.created === 'number' ? time.created : 0
    const messageId = typeof info.id === 'string' ? info.id : ''
    if (messageId === '') return
    const binding = conversationBindings.get(sessionId)
    // A turn before the session's bind lands is skipped quietly: the idle
    // that follows binds the session, and later turns publish.
    if (!binding) return
    const state = turnState(sessionId)
    const publish = state.chain.then(() =>
      publishTurn(binding, state, info, created, messageId, settledText))
    // A failed publish must not stop every later one, so the chain keeps a
    // resolved tail while the caller still sees the rejection.
    state.chain = publish.catch(() => {})
    await publish
  }

  /**
   * One queued publish. The newest-turn gate is read here rather than before
   * queueing, so it sees the turn the publish ahead of it claimed.
   */
  async function publishTurn(
    binding: ConversationBinding,
    state: TurnState,
    info: Record<string, unknown>,
    created: number,
    messageId: string,
    settledText?: string[],
  ): Promise<void> {
    if (state.publishedTurn && !isNewerTurn(state.publishedTurn, created, messageId)) return
    try {
      const content = settledText ?? await settledTurnText(binding.sessionId, messageId)
      const turn = extractTurnText([
        { content, stopReason: normalizeTurnStopReason(normalizeFinish(stopReasonOf(info))) },
      ])
      const bounded = boundFinalOutput(turn.text)
      const sequence = ++state.sequence
      await publishLastTurn(
        { brokerUrl: binding.brokerUrl, runId: binding.runId, token: binding.runToken },
        { sequence, final_output: bounded, stop_reason: turn.stopReason, truncated: turn.text.length > bounded.length },
      )
      // Claimed only once the broker accepted the turn, so a transient failure
      // leaves it unclaimed and the next event for it is retried.
      state.publishedTurn = { created, id: messageId }
    } catch (error) {
      console.error('[avenor] last turn publish failed:', error instanceof Error ? error.message : error)
    }
  }

  const onSessionIdle = async (sessionID: string): Promise<void> => {
    await restartConversationFeed(sessionID)

    for (const run of trackedRuns.values()) {
      if (run.orchestratorSessionId === sessionID && !run.monitoring) {
        monitorRun(run).catch(console.error)
      }
    }
  }

  const onMessageUpdated = async (info: Record<string, unknown>): Promise<void> => {
    await publishHostTurn(info)
  }

  const onPermissionAsk = async (permission: PermissionAsk): Promise<void> => {
    const sessionID = permission.sessionID ?? ''
    const runId = sessionIdToRunId.get(sessionID)
    if (!runId) return

    const run = trackedRuns.get(runId)
    if (!run) return

    const patternStr = permission.pattern
      ? [permission.pattern].flat().join(', ')
      : undefined

    const lines = [
      `Sub-agent "${run.label}" is requesting permission for \`${permission.type}\`.`,
      ...(patternStr ? [`Pattern: \`${patternStr}\``] : []),
      '',
      'To answer: call `avenor_answer_permission` with:',
      `  run_id: "${runId}"`,
      ...(permission.id ? [`  request_id: "${permission.id}"`] : []),
      '  option_id: "allow_once" | "allow_always" | "deny"',
    ]

    await primeTrackedRun(run, { captureLatestSeq: true })
    await stopMonitor(run.runId, 'permission-routed')
    await notifySession(run.orchestratorSessionId, lines.join('\n'))
  }

  const onChatMessage = (message: { parts?: Array<{ type?: string; text?: string }> } | undefined): void => {
    if (!message?.parts) return
    for (const part of message.parts) {
      if (part?.type === 'text' && typeof part?.text === 'string' && part.text.includes('<channel ')) {
        part.text = formatChannelBlocks(part.text)
      }
    }
  }

  const onTextComplete = (output: { text: string }): void => {
    if (typeof output.text !== 'string' || !output.text.includes('<channel ')) return
    try {
      output.text = formatChannelBlocks(output.text)
    } catch {
      // Leave text as-is if formatting fails
    }
  }

  // The harness owns the session's user-defined name, and the broker lets it
  // outrank any name the model set for itself. A native rename publishes the
  // human title as the bound conversation run's harness name; an unchanged
  // title is not republished and a failed publish is retried on the next
  // rename event.
  let titlePublishChain: Promise<void> = Promise.resolve()
  const publishedTitles = new Map<string, { name: string | undefined }>()
  const refusedTitles = new Map<string, { name: string | undefined }>()
  // The latest native title each session has carried, remembered before a
  // first bind so a conversation that names itself before any messaging call
  // still publishes that name when it binds.
  const sessionTitles = new Map<string, string>()
  // Bumped by every native rename event. A background title read that started
  // under an older version answers with a name the human has already replaced,
  // so its answer is dropped instead of published over the newer one.
  const titleVersions = new Map<string, number>()
  // The sessions whose default-label derivation was already attempted, marked
  // before the git read starts: a later appearance re-runs it never — not when
  // the derivation named the session, and not when it found nothing to name (a
  // directory with no usable name in it, for instance).
  const derivationAttempts = new Set<string>()
  // The chain the default-label work queues onto — the same shape the title
  // publishes use: one read-git → read-taken → derive → publish at a time, so
  // two conversations appearing together cannot read the same taken-set and
  // name themselves alike; each derivation runs after the previous
  // conversation's publish.
  let labelPublishChain: Promise<void> = Promise.resolve()

  function titleVersion(sessionId: string): number {
    return titleVersions.get(sessionId) ?? 0
  }

  function publishSessionTitle(sessionId: string, title: string | undefined): Promise<void> {
    if (typeof title === 'string') sessionTitles.set(sessionId, title)
    // A rename never binds a session: a conversation that has not opted into
    // messaging gains no identity here, and its title waits for a bind.
    if (!conversationBindings.has(sessionId)) return titlePublishChain
    const published = publishedTitles.get(sessionId)
    if (published && published.name === title) return titlePublishChain
    const refused = refusedTitles.get(sessionId)
    if (refused && refused.name === title) return titlePublishChain
    // Record the attempt before it lands so a second event for the same title
    // while the first is in flight does not publish twice.
    const attempt: { name: string | undefined } = { name: title }
    publishedTitles.set(sessionId, attempt)
    titlePublishChain = titlePublishChain.then(async () => {
      try {
        const binding = conversationBindings.get(sessionId)
        if (!binding) return
        await publishHarnessName(
          { brokerUrl: binding.brokerUrl, runId: binding.runId, token: binding.runToken },
          title,
        )
        refusedTitles.delete(sessionId)
      } catch (error) {
        if (isInvalidNameRefusal(error)) {
          // Retrying a name the broker refuses only costs a failed POST per
          // rename event; a different title is sent again.
          refusedTitles.set(sessionId, { name: title ?? undefined })
          console.error('[avenor] session_name_set refused:', error instanceof Error ? error.message : error)
          return
        }
        // A later publish may already hold a newer record; only forget this
        // attempt's own record so the newer title is not published twice.
        if (publishedTitles.get(sessionId) === attempt) publishedTitles.delete(sessionId)
        console.error('[avenor] session_name_set failed:', error instanceof Error ? error.message : error)
      }
    })
    return titlePublishChain
  }

  /** Read the session's current native title and publish it if it changed. */
  function publishCurrentTitle(sessionId: string): Promise<void> {
    const remembered = sessionTitles.get(sessionId)
    const readTitle = host.getSessionTitle
    if (!readTitle) {
      if (typeof remembered === 'string') return publishSessionTitle(sessionId, remembered)
      return Promise.resolve()
    }
    // The read is asynchronous while renames keep arriving, and a rename that
    // lands first is strictly newer than an answer this read already owed: the
    // version it started under is the gate that keeps the human's name.
    const version = titleVersion(sessionId)
    return readTitle(sessionId)
      .then(title => {
        if (titleVersion(sessionId) !== version) return
        const name = typeof title === 'string' ? title : remembered
        if (typeof name === 'string') return publishSessionTitle(sessionId, name)
      })
      .catch(error => {
        console.error('[avenor] session title read failed:', error instanceof Error ? error.message : error)
      })
  }

  const onSessionRenamed = async (sessionID: string, title: string | undefined): Promise<void> => {
    // Every rename event outranks any title read already in flight.
    titleVersions.set(sessionID, titleVersion(sessionID) + 1)
    await publishSessionTitle(sessionID, title)
  }


  /**
   * The labels the broker already holds, read through the conversation's own
   * bound identity — the same read a bound session's peers list serves, leaving
   * out the conversation's own run. A read that fails, or comes back marked
   * incomplete, is no information (null), and the derived label takes its
   * run-id tail rather than guess at labels the listing did not carry.
   */
  async function takenSessionLabels(sessionId: string, ownRunId: string): Promise<Set<string> | null> {
    const provider = conversationProviders.get(sessionId)
    if (!provider) return null
    try {
      return takenLabelsFromListing(await provider.peers(), ownRunId) ?? null
    } catch {
      return null
    }
  }

  /**
   * Bind a conversation the moment the host shows it: the session's created
   * event, or the session's first turn when the plugin loaded after the
   * session already existed. Binding gives the conversation its run, feed and
   * name before its first messaging call, and an already-bound session takes
   * no second identity. A conversation no human has named is named from the
   * directory the appearing event carries — "[<hostname>-]opencode-<repo>"
   * through the shared default-label rule, with a run-id tail where the name
   * alone is not unique: a native title — remembered from an earlier rename or
   * read at bind time — always outranks the derived label, and a later rename
   * outranks a published default. Safe to run without awaiting: the caller
   * keeps the event pump moving while the bind and the git reads behind the
   * label proceed here.
   */
  const onSessionAppeared = async (sessionID: string, directory?: string): Promise<void> => {
    try {
      await bindConversation(sessionID)
    } catch (error) {
      console.error(`[avenor] conversation bind failed for session ${sessionID}:`, error instanceof Error ? error.message : error)
      return
    }
    if (directory === undefined || typeof sessionTitles.get(sessionID) === 'string') return
    if (derivationAttempts.has(sessionID)) return
    derivationAttempts.add(sessionID)
    // The narrowing copy keeps dir a string inside the asynchronous naming
    // work below.
    const dir = directory
    // Queued behind any earlier conversation's naming work, so the peers read
    // sees what the previous publish landed.
    const naming = labelPublishChain.then(async () => {
      const binding = conversationBindings.get(sessionID)
      if (!binding) return
      // The bind's own title read resolves while git runs: the version captured
      // here gates the publish on a native name arriving during the derivation.
      const version = titleVersion(sessionID)
      const toplevel = await readGitToplevel(dir)
      if (titleVersion(sessionID) !== version || typeof sessionTitles.get(sessionID) === 'string') return
      // A binding dropped while git ran belongs to a run that is gone: a label
      // derived from it must not be published or remembered for the rebind.
      if (conversationBindings.get(sessionID) !== binding) return
      // The bound conversation's own credential reads the labels the broker
      // holds, so a second conversation in the same work takes the run-id tail
      // instead of a name a peer already answers to. The gate runs again after
      // the read: a native title landing during it must not be overwritten
      // either.
      const taken = await takenSessionLabels(sessionID, binding.runId)
      if (titleVersion(sessionID) !== version || typeof sessionTitles.get(sessionID) === 'string') return
      if (conversationBindings.get(sessionID) !== binding) return
      // The machine's name matters only when the broker is on another machine.
      const hostname = brokerIsLocal(binding.brokerUrl, '') ? '' : (options.hostname ?? osHostname)()
      const label = defaultHostLabel({
        host: 'opencode',
        hostname,
        brokerUrl: binding.brokerUrl,
        dir,
        toplevel,
        runId: binding.runId,
        taken,
      })
      if (label === '') return
      await publishSessionTitle(sessionID, label)
    })
    // A failed naming step must not hold later conversations' labels: the
    // chain keeps a resolved tail while the caller still sees the rejection.
    labelPublishChain = naming.catch(() => {})
    await naming
  }

  /**
   * The V2 step event carries only the session and message IDs, so the settled
   * assistant message is read here and projected onto the same info shape the
   * V1 message.updated event carried.
   */
  const onStepEnded = async (sessionID: string, messageId: string): Promise<void> => {
    const message = await host.getMessage(sessionID, messageId).catch(() => undefined)
    const settled = stepEndedInfo(message, sessionID, messageId)
    if (!settled) return
    await publishHostTurn(settled.info, settled.settledText)
  }

  const hooks: Hooks = {
    event: async ({ event: busEvent }) => {
      // The bus event's type union predates session.renamed on some host
      // builds, so the event is read through the shape every generation shares.
      const event = busEvent as unknown as { type: string; properties?: unknown; data?: unknown }
      // A native rename reaches this hook as session.updated with the session
      // record's current title on the installed V1 SDK; some host generations
      // emit the narrower session.renamed instead, and both carry the same
      // fact, so both are handled and the dedup makes double delivery harmless.
      if (event.type === 'session.updated') {
        const info = (event.properties as { info?: { id?: unknown; title?: unknown } } | undefined)?.info
        if (info && typeof info.id === 'string' && typeof info.title === 'string') {
          await onSessionRenamed(info.id, info.title)
        }
        return
      }
      if (event.type === 'session.renamed') {
        const payload = ((event.properties ?? event.data) ?? {}) as { sessionID?: unknown; title?: unknown }
        const sessionID = typeof payload.sessionID === 'string' ? payload.sessionID : ''
        // A rename event always carries the title; a payload without one is
        // incomplete and never treated as a clear.
        if (sessionID && typeof payload.title === 'string') {
          await onSessionRenamed(sessionID, payload.title)
        }
        return
      }
      // OpenCode's per-message completion event is this surface's turn_end: it
      // carries the assistant message that settled, so the turn is published
      // from the event instead of re-derived from the session.
      if (event.type === 'message.updated') {
        const info = (event.properties as { info?: unknown } | undefined)?.info
        if (info !== null && typeof info === 'object') {
          await onMessageUpdated(info as Record<string, unknown>)
        }
        return
      }
      if (event.type !== 'session.idle') return
      const { sessionID } = (
        event as { type: 'session.idle'; properties: { sessionID: string } }
      ).properties

      // The V1 generation has no created event, so the turn's end is where a
      // session first appears: it binds here through the same handler. The
      // event carries no directory, so the derived default label is simply not
      // published on this generation — a native title still publishes at bind.
      await onSessionAppeared(sessionID)
      await onSessionIdle(sessionID)
    },

    'permission.ask': async (permission, _output) => {
      await onPermissionAsk(permission)
    },

    'chat.message': async (message: any, _output: any) => {
      onChatMessage(message)
    },

    'experimental.text.complete': async (
      _input: { sessionID: string; messageID: string; partID: string },
      output: { text: string },
    ) => {
      onTextComplete(output)
    },

    tool: {
      avenor_spawn: tool({
        description:
          `Dispatch an agent run via avenor. Identity comes from one of two modes. In direct mode pass any of agent, model, and backend, and omitted values use runtime defaults. In roster mode pass roster_file with roster_entry, which names all three and rejects them as arguments. Optional thinking accepts ${[...THINKING_LEVELS].slice(0, -1).join(', ')}, or ${THINKING_LEVELS[THINKING_LEVELS.length - 1]}; unsupported backends reject explicit values. Blocks by default, showing live progress as an updating tool call. Set wait=false for fire-and-forget — you will be re-prompted automatically on completion.`,
        args: {
          agent: tool.schema.string().optional().describe('Optional agent persona; the backend resolves this name to a prompt, tool set, and permission policy. Independent of model and backend: supply any of the three, or none. Rejected when roster_entry is set, because the entry names all three'),
          prompt: tool.schema.string().optional().describe('Prompt text'),
          prompt_file: tool.schema.string().optional().describe('Path to prompt file'),
          dir: tool.schema.string().optional().describe('Working directory for the run (defaults to the session project directory)'),
          label: tool.schema.string().optional().describe('Human-readable label for the run'),
          timeout: tool.schema.string().optional().describe('Timeout duration (e.g. 3600s)'),
          model: tool.schema.string().optional().describe('Optional model override; independent of agent and backend, so a model can be named alone. Combined with agent, it runs that persona on this model. Rejected when roster_entry is set; to run a roster persona on a different model, send the identity directly'),
          thinking: tool.schema.enum([...THINKING_LEVELS]).optional().describe('Thinking level; omission uses the backend default'),
          backend: tool.schema.string().optional().describe('Optional runtime backend, such as agy, pi, opencode-acp, codex-app-server, claude, or poo. Omitting it in direct mode uses opencode-acp. Independent of agent and model. Rejected when roster_entry is set'),
          roster_file: tool.schema.string().optional().describe('Optional path to a roster map; pair it with roster_entry to select a whole loadout by key. Setting roster_entry turns on roster mode, which rejects agent, model, and backend'),
          roster_entry: tool.schema.string().optional().describe('Optional roster entry key; the selected entry supplies backend, agent, and model as one identity, and its optional thinking is only a default. Requires roster_file. Passing agent, model, or backend alongside it is an error; to change one part of a roster persona, send the identity directly instead'),
          server_url: tool.schema.string().optional().describe('Backend server URL'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
          wait: tool.schema.boolean().default(true).describe(
            'Block until complete with live status updates. False = fire-and-forget.',
          ),
        },
        async execute(args, context: ToolContext) {
          validateSpawnSelection({
            agent: args.agent,
            model: args.model,
            backend: args.backend,
            roster_file: args.roster_file,
            roster_entry: args.roster_entry,
          })

          // The conversation's own bound run is the spawn parent, so a child's
          // messages ride the same inbox the session's feed consumes. A bind
          // that cannot land leaves no parent route at all: execution
          // continues, and nothing is advertised that no consumer serves. The
          // messaging switch itself stays owned by the spawn option on the
          // backend side; this surface only names the parent.
          let ownerRunId: string | undefined
          try {
            const binding = await bindConversation(context.sessionID)
            // A parent route only resolves on the broker that holds the parent
            // run. A spawn pointed at another supervisor runs there without
            // one, rather than handing that broker a run id it would mint an
            // unserved parent record for.
            if (spawnSharesConversationSupervisor(binding, args.supervisor_id)) {
              ownerRunId = binding.runId
            } else {
              console.error(`[avenor] spawn targets supervisor ${args.supervisor_id}; the conversation's parent run lives on ${binding.supervisorId}, so the child starts without a parent route`)
            }
          } catch (error) {
            console.error('[avenor] conversation bind failed; messaging unavailable for this session:', error instanceof Error ? error.message : error)
          }
          const result = await spawnTool({
            ...(args.agent !== undefined ? { agent: args.agent } : {}),
            ...(args.prompt !== undefined ? { prompt: args.prompt } : {}),
            ...(args.prompt_file !== undefined ? { promptFile: args.prompt_file } : {}),
            ...(args.label !== undefined ? { label: args.label } : {}),
            dir: args.dir ?? context.directory,
            ...(args.timeout !== undefined ? { timeout: args.timeout } : {}),
            ...(args.model !== undefined ? { model: args.model } : {}),
            ...(args.thinking !== undefined ? { thinking: args.thinking } : {}),
            ...(args.backend !== undefined ? { backend: args.backend } : {}),
            ...(args.roster_file !== undefined ? { rosterFile: args.roster_file } : {}),
            ...(args.roster_entry !== undefined ? { rosterEntry: args.roster_entry } : {}),
            ...(args.server_url !== undefined ? { serverUrl: args.server_url } : {}),
            ...(args.supervisor_id !== undefined ? { supervisorId: args.supervisor_id } : {}),
            ...(ownerRunId !== undefined ? { parent_run_id: ownerRunId } : {}),
          })

          const run: TrackedRun = {
            runId: result.run_id,
            runtimeId: result.runtime_id,
            agent: args.agent,
            model: args.model,
            backend: args.backend,
            rosterFile: args.roster_file,
            rosterEntry: args.roster_entry,
            orchestratorSessionId: context.sessionID,
            label: result.label,
            supervisorId: result.supervisor_id || args.supervisor_id,
            monitoring: false,
          }
          trackedRuns.set(result.run_id, run)
          registerSessionId(undefined, result.runtime_id, result.run_id)
          await primeTrackedRun(run)

          if (!args.wait) {
            void monitorRun(run).catch(console.error)
            return {
              title: `${result.label} — dispatched`,
              output: `Dispatched "${result.label}" (run_id: ${result.run_id}). Completion will be delivered automatically; use avenor_status with view "lifecycle" for progress or avenor_result to wait for the final result.`,
              metadata: hostIdentityMetadata(run),
            }
          }

          context.metadata({ title: `Starting ${result.label}…` })

          const outcome = await observeTrackedRun(run, {
            abort: context.abort,
            onSnapshot: (snapshot) => {
              context.metadata({
                title: snapshot.pending_permission ? `[blocked] ${result.label}` : result.label,
                metadata: buildSnapshotMetadata(snapshot, run),
              })
            },
          })

          if (outcome.kind === 'terminal') {
            const inspection = await loadInspection(run)
            const snapshot = inspection?.snapshot ?? outcome.snapshot
            unregisterRun(result.run_id)
            return {
              title: `${result.label} — ${terminalStatus(snapshot, inspection?.status)}`,
              output: buildCompletionText(run, snapshot, inspection?.status, inspection?.final_output),
              metadata: {
                run_id: result.run_id,
                session_id: inspection?.status.session_id ?? snapshot.identity.session_id,
                final_output: clipTail(inspection?.final_output ?? snapshot.final_output, FINAL_OUTPUT_CHARS),
                ...buildSnapshotMetadata(snapshot, run, inspection?.status),
              },
            }
          }

          if (outcome.kind === 'waiting') {
            const inspection = await loadInspection(run)
            const snapshot = inspection?.snapshot ?? outcome.snapshot
            return {
              title: `${result.label} — waiting`,
              output: buildWaitingText(run, snapshot),
              metadata: {
                run_id: result.run_id,
                session_id: inspection?.status.session_id ?? snapshot.identity.session_id,
                ...buildSnapshotMetadata(snapshot, run, inspection?.status),
              },
            }
          }

          if (outcome.kind === 'error') {
            return {
              title: `${result.label} — monitoring error`,
              output: `Monitoring failed: ${outcome.error ?? 'unknown error'}. The run may still be active — use avenor_status to check.`,
              metadata: { run_id: result.run_id, ...hostIdentityMetadata(run) },
            }
          }

          unregisterRun(result.run_id)
          return {
            title: `${result.label} — aborted`,
            output: `Monitoring of "${result.label}" (run_id: ${result.run_id}) was interrupted. The run may still be active — use avenor_status to check.`,
            metadata: { run_id: result.run_id, ...hostIdentityMetadata(run) },
          }
        },
      }),

      avenor_status: tool({
        description: 'Get lifecycle status of a run or all runs. Use view="lifecycle" for compact polling; full is the compatibility default. A broker peer with no runtime answers with its identity and broker status only. A label shared by several peers answers with the matching run ids.',
        args: {
          run_id: tool.schema.string().optional().describe('Specific run ID to query'),
          view: tool.schema.enum(['lifecycle', 'full']).optional().describe('Response detail (default full for compatibility)'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, _context) {
          const result = await statusTool({
            runId: args.run_id,
            supervisorId: args.supervisor_id,
            view: args.view,
          })
          return formatStatusOutput(args, result)
        },
      }),

      avenor_result: tool({
        description: 'Wait for a run to finish and return its complete final output without transcript or event details.',
        args: {
          run_id: tool.schema.string().describe('Run ID to await'),
          wait: tool.schema.boolean().optional().describe('Wait for a terminal result (default true)'),
          timeout: tool.schema.string().optional().describe('Maximum time to wait (e.g. 30s, 5m)'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, context) {
          const run = trackedRuns.get(args.run_id)
          if (run) await stopMonitor(run.runId, 'result')

          try {
            const result = await resultTool({
              runId: args.run_id,
              supervisorId: args.supervisor_id,
              wait: args.wait,
              timeout: args.timeout,
              signal: context.abort,
            })
            if (result.ready) {
              unregisterRun(args.run_id)
            } else if (run) {
              void monitorRun(run).catch(console.error)
            }
            return formatResultOutput(args, result)
          } catch (error) {
            if (run) void monitorRun(run).catch(console.error)
            throw error
          }
        },
      }),

      avenor_answer_permission: tool({
        description:
          'Answer a pending permission request for a sub-agent. Pass option_id from avenor_status pending_permission.options, or use "allow_once", "allow_always", "deny".',
        args: {
          run_id: tool.schema.string().describe('Run ID with the pending permission'),
          option_id: tool.schema.string().describe('allow_once | allow_always | deny, or option_id from pending_permission.options'),
          request_id: tool.schema.string().optional().describe('Permission request ID (auto-discovered if omitted)'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
          message: tool.schema.string().optional().describe('Optional write-in response text. Required only when the selected option declares requiresMessage: true.'),
        },
        async execute(args, _context) {
          const result = await answerPermissionTool({
            runId: args.run_id,
            optionId: args.option_id,
            requestId: args.request_id,
            supervisorId: args.supervisor_id,
            message: args.message,
          })
          return formatAnswerPermissionOutput(args, result)
        },
      }),

      avenor_message: tool({
        description:
          'Send a message to another agent run, withdraw a pending ask, resume a completed run, or deliver a control reply. Every action is explicit and the result states whether a failed ask can still be withdrawn. `reply` answers an inbound message: an ask this surface received is answered through its claim, and a tell is routed back to its sender by the broker. A message id with no route reports edge-absent.',
        args: {
          action: tool.schema.enum(['tell', 'ask', 'reply', 'resume', 'withdraw', 'control_reply']).describe('tell | ask | reply | resume | withdraw | control_reply'),
          target: tool.schema.string().optional().describe('Recipient for tell/ask/resume (a run id, a peer label, or "self"), or the opaque control message id for control_reply'),
          message: tool.schema.string().optional().describe('Message text for tell/ask/reply/resume'),
          reply_to: tool.schema.string().optional().describe('Inbound ask id being answered (reply)'),
          message_id: tool.schema.string().optional().describe('Ask id being withdrawn (withdraw)'),
          timeout_ms: tool.schema.number().int().optional().describe('Bound on the ask wait in milliseconds'),
          label: tool.schema.string().optional().describe('Override label for the resumed run (resume)'),
          idempotency_key: tool.schema.string().optional().describe('Forwarded verbatim to the resume authority (resume)'),
          payload: tool.schema.unknown().optional().describe('Control reply body (control_reply)'),
        },
        async execute(args, context) {
          const action = (args as { action?: unknown }).action
          if (action === 'resume') {
            // Resume is owner-only: a broker-authenticated conversation may
            // continue only a run it spawned, which the owner authority owns.
            const provider = await resumeProvider(context.sessionID)
            const result = await messageTool(args, provider, { signal: context.abort })
            return formatMessageOutput(args, result)
          }
          if (action === 'reply') {
            // A reply is answered through the feed's Go claim store, so it
            // runs the `avenor message` CLI as this conversation rather than
            // the shared adapter the other actions use.
            await conversationToolProvider(context.sessionID)
            const result = await replyViaCli(context.sessionID, args, context.abort)
            return formatMessageOutput(args, result)
          }
          const provider = await conversationToolProvider(context.sessionID)
          const result = await messageTool(args, provider, { signal: context.abort })
          return formatMessageOutput(args, result)
        },
      }),

      avenor_peers: tool({
        description:
          "List the visible agent sessions, rename a run, or read a run's last agent message. `last` returns the target's final text with its stop reason, or an empty result when the target has no completed turn yet.",
        args: {
          action: tool.schema.enum(['list', 'rename', 'last']).optional().describe('list (the default) | rename | last'),
          name: tool.schema.string().optional().describe('New display name (rename)'),
          session_id: tool.schema.string().optional().describe('Run to rename; omitted renames the caller (rename)'),
          target: tool.schema.string().optional().describe('Peer to read: a run id, a label, or "self" (last)'),
        },
        async execute(args, context) {
          // Discovery rides the conversation's own identity, never the shared
          // supervisor: list, rename and last all act as the bound run.
          const provider = await conversationToolProvider(context.sessionID)
          return formatPeersOutput(args, await peersTool(args, provider))
        },
      }),

      avenor_events: tool({
        description: 'Read events from a run. Filter by type. Returns last N events. A broker peer has no event log; peers action last reads its last agent message. A label shared by several peers answers with the matching run ids.',
        args: {
          run_id: tool.schema.string().describe('Run ID to read events from'),
          types: tool.schema.array(tool.schema.string()).optional().describe('Filter by event types'),
          limit: tool.schema.number().optional().describe('Max events to return (default 50)'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, _context) {
          const result = await eventsTool({
            runId: args.run_id,
            types: args.types,
            limit: args.limit,
            supervisorId: args.supervisor_id,
          })
          return formatEventsOutput(args, result)
        },
      }),

      avenor_inspect: tool({
        description: 'Inspect a run with reduced snapshot, transcript, tools, permissions, and final output.',
        args: {
          run_id: tool.schema.string().describe('Run ID to inspect'),
          limit: tool.schema.number().optional().describe('Max events to reduce into the snapshot (default 50)'),
          after_seq: tool.schema.number().optional().describe('Only include events after this sequence number'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, _context) {
          const result = await inspectTool({
            runId: args.run_id,
            limit: args.limit,
            after_seq: args.after_seq,
            supervisorId: args.supervisor_id,
          })
          return formatInspectOutput(args, result)
        },
      }),

      avenor_kill: tool({
        description: 'Terminate a run without shutting down the supervisor. Other runs and the supervisor itself keep running.',
        args: {
          run_id: tool.schema.string().min(1).describe('Run ID or label of the run to terminate'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const result = await killTool({ runId: args.run_id, supervisorId: args.supervisor_id })
          return formatKillOutput(args, result)
        },
      }),

      avenor_shutdown: tool({
        description: 'Shut down the avenor supervisor and clean up temp files.',
        args: {
          supervisor_id: tool.schema.string().optional().describe('Supervisor to shut down (defaults to singleton)'),
          force: tool.schema.boolean().optional().describe('Force shutdown instead of graceful'),
        },
        async execute(args, _context) {
          await stopTrackedRuns((run) => !args.supervisor_id || run.supervisorId === args.supervisor_id)
          // A supervisor going away means every conversation binding names a
          // dead run — but only the supervisor these bindings were minted on
          // can revoke them. A shutdown aimed at another supervisor leaves this
          // one's conversations, feeds and credentials untouched.
          if (shutdownDropsConversations(args.supervisor_id)) resetConversationBindings()
          const result = await shutdownTool({
            supervisorId: args.supervisor_id,
            force: args.force,
          })
          return formatShutdownOutput(args, result)
        },
      }),

      // The merged workflow tool: one registration whose `action` selects the
      // verb, routed through the shared core router.
      avenor_workflow: tool({
        description: "Drive a durable workflow instance: read its status, detail, events or wait for it; report controller readiness or status; or record a gate decision, complete a handoff activation, or cancel the instance. One `action` selects the verb. Read-only actions need no permission; gate, complete and cancel keep their authority checks.",
        args: {
          action: tool.schema.enum(['status', 'inspect', 'events', 'wait', 'ready', 'controller_status', 'gate', 'complete', 'cancel']).describe('Action to perform'),
          workflow_id: tool.schema.string().optional().describe('Workflow ID'),
          controller_id: tool.schema.string().optional().describe('Controller ID; omit for controller_status to list all controllers'),
          after_seq: tool.schema.number().int().optional().describe('Only include events with sequence greater than this value (events)'),
          limit: tool.schema.number().int().optional().describe('Max events or candidates to return (0 for the server default)'),
          timeout: tool.schema.string().optional().describe('Maximum wait time, such as 30s, 5m, or 1h (default 30s; wait)'),
          node_id: tool.schema.string().optional().describe('Node ID (gate, complete)'),
          gate_id: tool.schema.string().optional().describe('Gate ID (gate)'),
          activation_id: tool.schema.string().optional().describe('Activation ID (gate, complete)'),
          attempt_id: tool.schema.string().optional().describe('Attempt ID (complete)'),
          lease_id: tool.schema.string().optional().describe('Lease ID (complete)'),
          owner_token: tool.schema.string().optional().describe('Lease owner token (complete)'),
          operation: tool.schema.enum(['satisfy', 'reject', 'waive', 'external_result']).optional().describe('Gate operation (gate)'),
          actor: tool.schema.string().optional().describe('Actor id (gate, cancel)'),
          reason: tool.schema.string().min(1).optional().describe('Reason (gate, cancel)'),
          outcome: tool.schema.string().optional().describe('Outcome (gate, complete)'),
          subject: tool.schema.unknown().optional().describe('Bound exact subject as JSON (gate)'),
          poll_id: tool.schema.string().optional().describe('External poll id (gate external_result)'),
          source: tool.schema.string().optional().describe('External result source (gate external_result)'),
          result: tool.schema.string().optional().describe('External result status (gate external_result)'),
          response_hash: tool.schema.string().optional().describe('Response hash (gate external_result)'),
          observed_at: tool.schema.string().optional().describe('Observed-at RFC3339 timestamp (gate external_result)'),
          evidence_ids: tool.schema.array(tool.schema.string()).optional().describe('Evidence IDs (gate)'),
          outputs: tool.schema.unknown().optional().describe('Declared outputs as JSON (complete)'),
          artifacts: tool.schema.unknown().optional().describe('Staged evidence artifacts as JSON (complete)'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const result = await runWorkflowTool(args)
          if (args.action === 'controller_status') {
            return formatWorkflowControllerStatusOutput(args, result.value)
          }
          return { title: 'workflow', output: JSON.stringify(result.value, null, 2) }
        },
      }),
    },
  }

  return {
    ...hooks,
    // Handlers the V2 adapter registers through its own domains. The hooks
    // above carry the same behavior for the V1 generation, except where the
    // generations differ: V1 has no created event, so its hook binds on the
    // idle that ends a turn, and with no directory available the derived
    // default label is never published there.
    onSessionIdle,
    onSessionAppeared,
    onSessionRenamed,
    onMessageUpdated,
    onPermissionAsk,
    onStepEnded,
    onChatMessage,
    onTextComplete,
    formatChannelBlocks,
  }
}
