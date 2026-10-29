import type { Hooks, ToolContext } from '@opencode-ai/plugin'
import { tool } from '@opencode-ai/plugin'

import * as crypto from 'node:crypto'
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
  boundFinalOutput,
  createHostMessageProvider,
  createRunSnapshot,
  dial,
  eventsTool,
  extractTurnText,
  getSupervisorClient,
  inspectTool,
  isClosedSocketError,
  messageTool,
  normalizeTurnStopReason,
  observeRun,
  peersTool,
  resultTool,
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
  type StatusResult,
  THINKING_LEVELS,
  validateSpawnSelection,
  workflowCancelTool,
  workflowStatusTool,
  workflowWaitTool,
  workflowInspectTool,
  workflowEventsTool,
  workflowCompleteTool,
  workflowGateTool,
  workflowControllerStatusTool,
  workflowReadyTool,
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

type HostProviderBinding = {
  provider: MessageProvider
  supervisorId: string
  generation: string
}

/** The one OpenCode session this plugin publishes turns for. */
type HostBinding = HostProviderBinding & { sessionId: string }

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

const TERMINAL_STATUSES = new Set(['done', 'failed', 'timeout', 'killed'])
const POLL_INTERVAL_MS = 3_000
const FINAL_OUTPUT_CHARS = 1_500
const METADATA_TEXT_CHARS = 400
const TRANSCRIPT_PREVIEW_ENTRIES = 3

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function clipTail(text: string | undefined, limit: number): string | undefined {
  if (!text) return undefined
  if (text.length <= limit) return text
  return text.slice(text.length - limit)
}

export function terminalStatus(snapshot: RunSnapshotView, fallback?: StatusResult): string {
  if (fallback?.status) return fallback.status
  if (snapshot.phase && TERMINAL_STATUSES.has(snapshot.phase)) return snapshot.phase
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

function formatAgentMessage(source: string, fromRunId: string, fromRole: string, content: string): string {
  const label = source.replace(/^agent-/, '') || source
  const shortId = fromRunId ? fromRunId.slice(0, 8) : ''
  const lines = [
    `📨 ${label} (${shortId})`,
    '',
    ...content.trim().split('\n').map(l => `  ${l}`),
  ]
  return lines.join('\n') + '\n'
}

function renderChannelMessage(match: string, source: string, fromRunId: string, fromRole: string, content: string): string {
  return formatAgentMessage(source, fromRunId, fromRole, content)
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
  /** Deliver a user prompt into a session without waiting on the reply. */
  promptAsync(sessionId: string, text: string): Promise<void>
  /** Read one settled message; resolve undefined when it cannot be read. */
  getMessage(
    sessionId: string,
    messageId: string,
  ): Promise<{ parts?: unknown[]; content?: unknown[] } | undefined>
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
  onMessageUpdated(info: Record<string, unknown>): Promise<void>
  onPermissionAsk(permission: PermissionAsk): Promise<void>
  onStepEnded(sessionID: string, messageId: string): Promise<void>
  onChatMessage(message: { parts?: Array<{ type?: string; text?: string }> } | undefined): void
  onTextComplete(output: { text: string }): void
  formatChannelBlocks(text: string): string
}

export function createAvenorCore(host: PluginHost): AvenorCore {
  const trackedRuns = new Map<string, TrackedRun>()
  const activeMonitors = new Map<string, ActiveMonitor>()
  const sessionIdToRunId = new Map<string, string>()
  const runtimeIdToRunId = new Map<string, string>()
  let parentRunId = ''
  let parentToken = ''
  let brokerUrl = ''
  let channelPolling = false

  function ensureParentRunId(): string {
    if (!parentRunId) {
      parentRunId = crypto.randomUUID()
    }
    return parentRunId
  }

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

  async function promptSession(sessionId: string, text: string): Promise<void> {
    await host.promptAsync(sessionId, text)
  }

  async function monitorRun(run: TrackedRun): Promise<void> {
    const outcome = await observeTrackedRun(run)

    if (outcome.kind === 'error') {
      await promptSession(
        run.orchestratorSessionId,
        `Monitoring of "${run.label}" (run_id: ${run.runId}) failed: ${outcome.error ?? 'unknown error'}. The run may still be active — use avenor_status to check.`,
      )
      return
    }

    if (outcome.kind === 'waiting') {
      if (!outcome.notified) {
        await promptSession(run.orchestratorSessionId, buildWaitingText(run, outcome.snapshot))
      }
      return
    }

    if (outcome.kind === 'terminal') {
      const inspection = await loadInspection(run)
      const snapshot = inspection?.snapshot ?? outcome.snapshot
      await promptSession(
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

  async function pollChannelMessages(sessionId: string): Promise<void> {
    if (channelPolling || !brokerUrl || !parentRunId || !parentToken) return

    channelPolling = true
    let consecutiveErrors = 0
    const MAX_CONSECUTIVE_ERRORS = 10

    try {
      while (consecutiveErrors < MAX_CONSECUTIVE_ERRORS) {
        await sleep(POLL_INTERVAL_MS)
        let msgs: any[]
        try {
          const resp = await fetch(`${brokerUrl}/poll-control`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ run_id: parentRunId, token: parentToken }),
          })
          if (!resp.ok) {
            consecutiveErrors++
            continue
          }
          msgs = await resp.json()
          consecutiveErrors = 0
        } catch {
          consecutiveErrors++
          continue
        }
        for (const msg of (msgs ?? [])) {
          if (msg.type !== 'agent_message') continue
          let payload: any
          try { payload = typeof msg.payload === 'string' ? JSON.parse(msg.payload) : msg.payload } catch { continue }
          const text = typeof payload?.message === 'string' ? payload.message : ''
          if (!text) continue
          // The broker stamps the sender on the queued row. The payload's copy is
          // the sender's own claim and its role has no stamp anywhere, so neither
          // is rendered: a peer must not be able to name itself in the header the
          // model reads. Only a run this host tracks supplies a name it knows.
          const from = msg.from_run_id ?? payload?.from_run_id ?? 'unknown'
          const resolvedRunId = trackedRuns.has(from) ? from
            : sessionIdToRunId.get(from)
            ?? runtimeIdToRunId.get(from)
          const runInfo = resolvedRunId ? trackedRuns.get(resolvedRunId) : undefined
          const channelText = formatAgentMessage(runInfo?.agent ?? 'agent', from, '', text)
          try {
            await host.promptAsync(sessionId, channelText)
          } catch {
            return
          }
        }
      }
    } finally {
      channelPolling = false
    }
  }

  // One host provider for this plugin instance: bound to one supervisor and one
  // generation, created on the first host messaging tool call. It is rebuilt
  // when the supervisor's client changes (a shutdown followed by a fresh
  // singleton, or a closed socket, whose drop keeps the binding's session), so
  // a revoked generation is never reused and no call is retargeted to another
  // session. A provider call is also how the plugin learns which OpenCode
  // session owns the broker's supervisor turn.
  let hostProvider: { client: unknown; binding: HostProviderBinding } | undefined
  let hostBinding: HostBinding | undefined
  // Bumped by every bind that has to preflight. A bind whose sequence is stale
  // by the time its preflight resolves raced a later one, and its client may
  // name a generation the supervisor has already revoked.
  let bindSeq = 0

  async function boundHostProvider(): Promise<HostProviderBinding> {
    const { client, supervisorId } = await getSupervisorClient(undefined, { owner: true })
    if (hostProvider && hostProvider.client === client) return hostProvider.binding
    const seq = ++bindSeq
    const preflight = await client.messagePreflight()
    // A later bind started while this one preflighted, so this client may name
    // a generation the supervisor already revoked. The winner's binding is what
    // every later call must use; with no winner cached yet, the binding below
    // serves only this call and is never cached, so the client cannot revive.
    const stale = seq !== bindSeq
    if (stale && hostProvider) return hostProvider.binding
    const binding: HostProviderBinding = {
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
    hostProvider = { client, binding }
    if (hostBinding) hostBinding = { ...binding, sessionId: hostBinding.sessionId }
    return binding
  }

  async function hostMessageProvider(sessionId: string): Promise<MessageProvider> {
    const bound = await boundHostProvider()
    if (!hostBinding) {
      hostBinding = { ...bound, sessionId }
    } else if (hostBinding.sessionId !== sessionId) {
      console.error(`opencode: host session already bound to ${hostBinding.sessionId}`)
    }
    // The host binding stays with the session that first ran a host tool, but a
    // resumed run belongs to whichever session resumed it, so its monitor
    // prompts the caller rather than the host's bound session.
    return { ...bound.provider, onResumed: (result: ProviderResult) => trackResumed(sessionId, result) }
  }

  async function trackResumed(sessionId: string, result: ProviderResult): Promise<void> {
    const run: TrackedRun = {
      runId: result.runId ?? '',
      orchestratorSessionId: sessionId,
      label: result.label ?? result.runId ?? '',
      supervisorId: hostBinding?.supervisorId,
      monitoring: false,
    }
    trackedRuns.set(run.runId, run)
    await primeTrackedRun(run)
    void monitorRun(run).catch(console.error)
  }

  // The newest turn this plugin published, ordered by the message's own
  // creation time. A repeated message.updated and a late update for an older
  // message both publish nothing, so the recorded turn stays the one that came
  // last in the session rather than the one whose event arrived last.
  let publishedTurn: { created: number; id: string } | undefined

  // Publishes run one at a time in the order their events arrived. Without
  // that, two different messages can pass the newest-turn gate together and
  // settle in either order, so the broker's last-turn record and publishedTurn
  // can both end up on the older turn.
  let publishChain: Promise<void> = Promise.resolve()

  function isNewerTurn(created: number, id: string): boolean {
    if (!publishedTurn) return true
    if (created !== publishedTurn.created) return created > publishedTurn.created
    return id > publishedTurn.id
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

  // The host's own last-turn producer. It publishes the turn the event carries
  // only for the session that first ran a host messaging tool and only once the
  // assistant message has completed, so a turn from another session neither
  // publishes nor retargets the binding, and an idle that produced no message
  // publishes nothing. A failure is logged, never rethrown.
  async function publishHostTurn(info: Record<string, unknown>, settledText?: string[]): Promise<void> {
    const binding = hostBinding
    // A closed socket drops the client cache, not the binding: until the next
    // host tool rebinds, the binding's provider is stale, so skip quietly.
    if (!binding || !hostProvider) return
    if (info.role !== 'assistant') return
    if (info.sessionID !== binding.sessionId) return
    // A compaction summary is an assistant message on the same session that
    // also completes; it is the context OpenCode folded away, not an answer.
    if (info.summary === true) return
    const time = info.time as Record<string, unknown> | undefined
    if (typeof time?.completed !== 'number') return
    const created = typeof time.created === 'number' ? time.created : 0
    const messageId = typeof info.id === 'string' ? info.id : ''
    if (messageId === '') return
    if (!binding.provider.recordLastTurn) return
    const recordLastTurn = binding.provider.recordLastTurn
    const publish = publishChain.then(() =>
      publishTurn(binding, recordLastTurn, info, created, messageId, settledText))
    // A failed publish must not stop every later one, so the chain keeps a
    // resolved tail while the caller still sees the rejection.
    publishChain = publish.catch(() => {})
    await publish
  }

  /**
   * One queued publish. The newest-turn gate is read here rather than before
   * queueing, so it sees the turn the publish ahead of it claimed.
   */
  async function publishTurn(
    binding: HostBinding,
    recordLastTurn: NonNullable<MessageProvider['recordLastTurn']>,
    info: Record<string, unknown>,
    created: number,
    messageId: string,
    settledText?: string[],
  ): Promise<void> {
    if (!isNewerTurn(created, messageId)) return
    try {
      const content = settledText ?? await settledTurnText(binding.sessionId, messageId)
      const turn = extractTurnText([
        { content, stopReason: normalizeTurnStopReason(normalizeFinish(stopReasonOf(info))) },
      ])
      const bounded = boundFinalOutput(turn.text)
      await recordLastTurn(bounded, turn.stopReason, turn.text.length > bounded.length)
      // Claimed only once the broker accepted the turn, so a transient failure
      // leaves it unclaimed and the next update for it is retried.
      publishedTurn = { created, id: messageId }
    } catch (error) {
      if (isClosedSocketError(error)) {
        // Drop only the cached client whose publish failed: a concurrent bind
        // may have just cached a fresh one, mirroring the pi-side guard. The
        // binding stays, so publishes skip quietly until a rebind; the log
        // line below is the one observable record.
        if (!hostProvider || hostProvider.binding.provider === binding.provider) hostProvider = undefined
      }
      console.error('[avenor] session_last_set failed:', error instanceof Error ? error.message : error)
    }
  }

  const onSessionIdle = async (sessionID: string): Promise<void> => {
    pollChannelMessages(sessionID).catch(console.error)

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
    await promptSession(run.orchestratorSessionId, lines.join('\n'))
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

  /**
   * The V2 step event carries only the session and message IDs, so the settled
   * assistant message is read here and projected onto the same info shape the
   * V1 message.updated event carried. The read only happens for the bound host
   * session, mirroring the gate inside publishHostTurn, so another session's
   * steps never pay for a fetch.
   */
  const onStepEnded = async (sessionID: string, messageId: string): Promise<void> => {
    if (!hostBinding || hostBinding.sessionId !== sessionID) return
    const message = await host.getMessage(sessionID, messageId).catch(() => undefined)
    const settled = stepEndedInfo(message, sessionID, messageId)
    if (!settled) return
    await publishHostTurn(settled.info, settled.settledText)
  }

  const hooks: Hooks = {
    event: async ({ event }) => {
      // OpenCode's per-message completion event is this surface's turn_end: it
      // carries the assistant message that settled, so the turn is published
      // from the event instead of re-derived from the session.
      if (event.type === 'message.updated') {
        await onMessageUpdated(event.properties.info as unknown as Record<string, unknown>)
        return
      }
      if (event.type !== 'session.idle') return
      const { sessionID } = (
        event as { type: 'session.idle'; properties: { sessionID: string } }
      ).properties

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
          `Dispatch an agent run via avenor. Optional thinking accepts ${[...THINKING_LEVELS].slice(0, -1).join(', ')}, or ${THINKING_LEVELS[THINKING_LEVELS.length - 1]}; unsupported backends reject explicit values. Blocks by default, showing live progress as an updating tool call. Set wait=false for fire-and-forget — you will be re-prompted automatically on completion.`,
        args: {
          agent: tool.schema.string().optional().describe('Optional agent name; omission uses the supplied model or runtime defaults'),
          prompt: tool.schema.string().optional().describe('Prompt text'),
          prompt_file: tool.schema.string().optional().describe('Path to prompt file'),
          dir: tool.schema.string().optional().describe('Working directory for the run (defaults to the session project directory)'),
          label: tool.schema.string().optional().describe('Human-readable label for the run'),
          timeout: tool.schema.string().optional().describe('Timeout duration (e.g. 3600s)'),
          model: tool.schema.string().optional().describe('Model override'),
          thinking: tool.schema.enum([...THINKING_LEVELS]).optional().describe('Thinking level; omission uses the backend default'),
          backend: tool.schema.string().optional().describe('Backend override'),
          roster_file: tool.schema.string().optional().describe('Path to the roster map'),
          roster_entry: tool.schema.string().optional().describe('Roster entry to select'),
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

          const ownerRunId = ensureParentRunId()
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
            parent_run_id: ownerRunId,
          })

          if (result.broker_url) {
            brokerUrl = result.broker_url
          }
          if (result.parent_token) {
            parentToken = result.parent_token
          }

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
        description: 'Get lifecycle status of a run or all runs. Use view="lifecycle" for compact polling; full is the compatibility default.',
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
          'Send a message to another agent run, withdraw a pending ask, resume a completed run, or deliver a control reply. Every action is explicit and the result states whether a failed ask can still be withdrawn. `reply` always reports edge-absent here: this session receives no inbound asks, so nothing is sent.',
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
          const provider = await hostMessageProvider(context.sessionID)
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
          const provider = await hostMessageProvider(context.sessionID)
          return formatPeersOutput(args, await peersTool(args, provider))
        },
      }),

      avenor_workflow_cancel: tool({
        description: 'Cancel a workflow instance. The actor and reason are required.',
        args: {
          workflow_id: tool.schema.string().describe('Workflow ID'),
          actor: tool.schema.string().describe('Who is cancelling'),
          reason: tool.schema.string().describe('Why the workflow is being cancelled'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const { supervisor_id, ...rest } = args
          const result = await workflowCancelTool({ ...rest, supervisorId: supervisor_id })
          return { title: 'workflow', output: JSON.stringify(result, null, 2) }
        },
      }),

      avenor_events: tool({
        description: 'Read events from a run. Filter by type. Returns last N events.',
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
          const result = await shutdownTool({
            supervisorId: args.supervisor_id,
            force: args.force,
          })
          return formatShutdownOutput(args, result)
        },
      }),

      avenor_workflow_status: tool({
        description: 'Get lightweight status for a workflow instance',
        args: {
          workflow_id: tool.schema.string().describe('Workflow ID'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const result = await workflowStatusTool({
            workflowId: args.workflow_id,
            supervisorId: args.supervisor_id,
          })
          return { title: 'workflow', output: JSON.stringify(result, null, 2) }
        },
      }),

      avenor_workflow_wait: tool({
        description: 'Wait for a workflow to reach a terminal state or until timeout',
        args: {
          workflow_id: tool.schema.string().describe('Workflow ID'),
          timeout: tool.schema.string().optional().describe('Timeout (e.g. 30s, 5m)'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const result = await workflowWaitTool({
            workflowId: args.workflow_id,
            timeout: args.timeout,
            supervisorId: args.supervisor_id,
          })
          return { title: 'workflow', output: JSON.stringify(result, null, 2) }
        },
      }),

      avenor_workflow_inspect: tool({
        description: 'Return the full instance detail for a workflow',
        args: {
          workflow_id: tool.schema.string().describe('Workflow ID'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const result = await workflowInspectTool({
            workflowId: args.workflow_id,
            supervisorId: args.supervisor_id,
          })
          return { title: 'workflow', output: JSON.stringify(result, null, 2) }
        },
      }),

      avenor_workflow_events: tool({
        description: 'Read log events from a workflow instance\'s event log',
        args: {
          workflow_id: tool.schema.string().describe('Workflow ID'),
          after_seq: tool.schema.number().optional().describe('Only include events after this sequence number'),
          limit: tool.schema.number().optional().describe('Max events to return (default 50)'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const result = await workflowEventsTool({
            workflowId: args.workflow_id,
            afterSeq: args.after_seq,
            limit: args.limit,
            supervisorId: args.supervisor_id,
          })
          return { title: 'workflow', output: JSON.stringify(result, null, 2) }
        },
      }),

      avenor_workflow_complete: tool({
        description: 'Atomically complete a machine/external handoff activation',
        args: {
          workflow_id: tool.schema.string().describe('Workflow ID'),
          node_id: tool.schema.string().describe('Node ID'),
          activation_id: tool.schema.string().describe('Activation ID'),
          attempt_id: tool.schema.string().describe('Attempt ID'),
          lease_id: tool.schema.string().describe('Lease ID'),
          owner_token: tool.schema.string().describe('Owner token'),
          outcome: tool.schema.string().describe('Outcome'),
          outputs: tool.schema.unknown().optional().describe('Outputs'),
          artifacts: tool.schema.unknown().optional().describe('Artifacts'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const { supervisor_id, ...rest } = args
          const result = await workflowCompleteTool({
            ...rest,
            supervisorId: supervisor_id,
          })
          return { title: 'workflow', output: JSON.stringify(result, null, 2) }
        },
      }),

      avenor_workflow_gate: tool({
        description: 'Record a gate decision on a parked awaiting_gate activation',
        args: {
          workflow_id: tool.schema.string().describe('Workflow ID'),
          node_id: tool.schema.string().describe('Node ID'),
          gate_id: tool.schema.string().describe('Gate ID'),
          activation_id: tool.schema.string().describe('Activation ID'),
          operation: tool.schema.enum(['satisfy','reject','waive','external_result']).describe('Gate operation'),
          actor: tool.schema.string().optional().describe('Actor'),
          reason: tool.schema.string().optional().describe('Reason'),
          outcome: tool.schema.string().optional().describe('Outcome'),
          subject: tool.schema.unknown().optional().describe('Subject'),
          poll_id: tool.schema.string().optional().describe('Poll ID'),
          source: tool.schema.string().optional().describe('Source'),
          result: tool.schema.string().optional().describe('Result'),
          response_hash: tool.schema.string().optional().describe('Response hash'),
          observed_at: tool.schema.string().optional().describe('Observed at'),
          evidence_ids: tool.schema.array(tool.schema.string()).optional().describe('Evidence IDs'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const { supervisor_id, ...rest } = args
          const result = await workflowGateTool({
            ...rest,
            supervisorId: supervisor_id,
          })
          return { title: 'workflow', output: JSON.stringify(result, null, 2) }
        },
      }),

      avenor_workflow_controller_status: tool({
        description: 'Show workflow controller status. With controller_id, returns that controller\'s full status; without it, lists all controllers with summary state. Create, enable, and disable are CLI-only (avenor workflow controller ...).',
        args: {
          controller_id: tool.schema.string().optional().describe('Controller ID; omit to list all controllers'),
          supervisor_id: tool.schema.string().optional().describe('Supervisor ID for multi-supervisor mode'),
        },
        async execute(args, _context) {
          const result = await workflowControllerStatusTool({
            controllerId: args.controller_id,
            supervisorId: args.supervisor_id,
          })
          const details = { ...result }
          if (args.controller_id) {
            try {
              // Advisory candidate count; absent when the supervisor has no
              // workflow.ready support or the query fails.
              const ready = await workflowReadyTool({
                controllerId: args.controller_id,
                supervisorId: args.supervisor_id,
              })
              if (Array.isArray(ready.candidates)) details.candidate_count = ready.candidates.length
            } catch {
              // Advisory only — the status view renders without it.
            }
          }
          return formatWorkflowControllerStatusOutput(args, details)
        },
      }),
    },
  }

  return {
    ...hooks,
    // Handlers the V2 adapter registers through its own domains; the hooks
    // above are the V1 spellings of the same behavior.
    onSessionIdle,
    onMessageUpdated,
    onPermissionAsk,
    onStepEnded,
    onChatMessage,
    onTextComplete,
    formatChannelBlocks,
  }
}
