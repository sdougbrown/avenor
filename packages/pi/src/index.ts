import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent'
import type { Theme } from '@earendil-works/pi-tui'
import { Text } from '@earendil-works/pi-tui'
import { Type } from 'typebox'
import {
  dial,
  eventsTool,
  followUpTool,
  inspectTool,
  observeRun,
  resultTool,
  answerPermissionTool,
  shutdownTool,
  spawnTool,
  statusTool,
  Supervisor,
  type Client,
  type InspectResult,
  type RunObserver,
  type RunSnapshot,
  type StatusResult,
} from '@dougbots/avenor-core'
import {
  clipText,
  finalOutputPreview,
  openRunInspector,
  renderTranscriptLines,
  sanitizeText,
  type WatchDeps,
  type WatchRunRef,
} from './watch.js'
import type { TrackedRun, RunStatusEntry } from './types.js'
import { TERMINAL_STATUSES, findLiveStatusForTrackedRun, formatRunLine, statusEmoji } from './types.js'

const POLL_INTERVAL_MS = 3_000
const INSPECT_LIMIT = 128
const COMPLETION_OUTPUT_CHARS = 600
const INSPECT_JSON_STRING_CHARS = 600
const INSPECT_JSON_ARRAY_ITEMS = 24
const INSPECT_JSON_OBJECT_KEYS = 24
const INSPECT_JSON_DEPTH = 8

export interface ExtensionDeps {
  spawnTool: typeof spawnTool
  statusTool: typeof statusTool
  eventsTool: typeof eventsTool
  answerPermissionTool: typeof answerPermissionTool
  followUpTool: typeof followUpTool
  inspectTool: typeof inspectTool
  resultTool: typeof resultTool
  shutdownTool: typeof shutdownTool
  observeRun: typeof observeRun
  dial: typeof dial
  Supervisor: typeof Supervisor
}

const defaultDeps: ExtensionDeps = {
  spawnTool,
  statusTool,
  eventsTool,
  answerPermissionTool,
  followUpTool,
  inspectTool,
  resultTool,
  shutdownTool,
  observeRun,
  dial,
  Supervisor,
}

export function statusSupervisorId(
  requestedSupervisorId: string | undefined,
  spawnedSupervisorId: string,
): string | undefined {
  return requestedSupervisorId ? spawnedSupervisorId : undefined
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function normalizeMultilinePreview(text: string | undefined, limit = COMPLETION_OUTPUT_CHARS): string | undefined {
  if (!text) return undefined
  const clean = sanitizeText(text).trim()
  if (!clean) return undefined
  const clipped = clean.length <= limit ? clean : `${clean.slice(0, Math.max(0, limit - 1))}…`
  return clipped
}

function quotedLines(text: string | undefined): string[] {
  const preview = normalizeMultilinePreview(text)
  if (!preview) return []
  return preview.split('\n').slice(0, 8).map(line => `> ${line}`)
}

export function buildCompletionText(
  run: { runId: string; label: string },
  result: StatusResult,
  finalOutput?: string,
): string {
  const lines = [
    `Sub-agent "${run.label}" finished with status **${result.status}**.`,
  ]
  const output = normalizeMultilinePreview(finalOutput ?? result.final_output)
  if (output) {
    lines.push('', 'Final output:', ...quotedLines(output))
  }
  if (result.stop_reason && result.stop_reason.toUpperCase() !== result.status.toUpperCase()) {
    lines.push(`Stop reason: ${result.stop_reason}`)
  }
  if (result.session_id) {
    lines.push(`Session: \`${result.session_id}\``)
  }
  lines.push(
    '',
    `Call \`avenor_inspect\` with \`run_id: "${run.runId}"\` for the bounded transcript/snapshot, or \`avenor_follow_up\` to iterate.`,
  )
  return lines.join('\n')
}

function buildWaitingText(run: { runId: string; label: string; agent: string }, status: StatusResult): string {
  const perm = status.pending_permission
  const lines = [
    `Sub-agent "${run.label}" (${run.agent}) is waiting for input.`,
  ]
  if (perm) {
    lines.push(
      `Permission request: ${perm.description}`,
      `To answer: call \`avenor_answer_permission\` with run_id "${run.runId}", option_id "allow_once" | "allow_always" | "deny"`,
    )
  } else if (status.phase_label) {
    lines.push(`Question: ${status.phase_label}`)
  }
  if (!perm) {
    lines.push(
      `Use \`avenor_follow_up\` with run_id "${run.runId}" to respond, or \`avenor_inspect\` to review context first.`,
    )
  }
  return lines.join('\n')
}

export function isTerminalStatus(status: string | undefined): boolean {
  return !!status && TERMINAL_STATUSES.has(status as any)
}

export function renderStatusLines(entries: RunStatusEntry[]): string[] {
  const active = entries.filter(e => !isTerminalStatus(e.status))
  if (active.length === 0) return []

  return active.map(e => {
    const emoji = statusEmoji(e.status)
    const perm = e.pendingPermission ? ' 🔒' : ''
    const phase = e.phaseLabel ? ` [${e.phaseLabel}]` : ''
    return `${emoji} ${e.label} — ${e.status}${phase}${perm}`
  })
}

export function compactWhitespace(text: string | undefined, limit = 100): string | undefined {
  const clipped = clipText(text, limit)
  return clipped ? clipped.replace(/\s+/g, ' ').trim() : undefined
}

function snapshotProgressText(label: string, snapshot: RunSnapshot): string {
  if (snapshot.pending_permission) {
    return `[blocked] ${label} — permission: ${snapshot.pending_permission.description}`
  }

  const phase = snapshot.phase_label ? ` (${snapshot.phase_label})` : snapshot.phase ? ` (${snapshot.phase})` : ''
  const liveTool = snapshot.live_tools[snapshot.live_tools.length - 1]
  if (liveTool) {
    const toolName = liveTool.title ?? liveTool.kind ?? 'tool'
    const preview = compactWhitespace(liveTool.preview, 80)
    return `${label}${phase} — ${toolName}${preview ? `: ${preview}` : ''}`
  }

  const last = snapshot.transcript[snapshot.transcript.length - 1]
  const transcriptWidth = 160
  const rendered = last
    ? compactWhitespace(
        renderTranscriptLines(
          { ...snapshot, transcript: [last], live_tools: [], pending_permission: undefined },
          transcriptWidth,
          {
            fg: (_color: string, text: string) => text,
            bg: (_color: string, text: string) => text,
            bold: (text: string) => text,
            italic: (text: string) => text,
          } as Theme,
        )[0],
        80,
      )
    : undefined

  return `${label}${phase}${rendered ? ` — ${rendered}` : ''}`
}

function boundJsonValue(value: unknown, depth = 0): unknown {
  if (depth >= INSPECT_JSON_DEPTH) return '[truncated]'
  if (typeof value === 'string') {
    const clean = sanitizeText(value)
    return clean.length <= INSPECT_JSON_STRING_CHARS
      ? clean
      : `${clean.slice(0, INSPECT_JSON_STRING_CHARS - 1)}…`
  }
  if (Array.isArray(value)) {
    return value
      .slice(Math.max(0, value.length - INSPECT_JSON_ARRAY_ITEMS))
      .map(item => boundJsonValue(item, depth + 1))
  }
  if (!value || typeof value !== 'object') return value
  const result: Record<string, unknown> = {}
  for (const [key, item] of Object.entries(value).slice(0, INSPECT_JSON_OBJECT_KEYS)) {
    result[key] = boundJsonValue(item, depth + 1)
  }
  return result
}

export function buildInspectPayload(result: InspectResult): Record<string, unknown> {
  const finalOutput = finalOutputPreview(result.status, result.snapshot)
  return boundJsonValue({
    run_id: result.run_id,
    label: result.label,
    status: result.status,
    snapshot: result.snapshot,
    transcript: result.transcript,
    tools: result.tools,
    live_tools: result.live_tools,
    permissions: result.permissions,
    pending_permission: result.pending_permission,
    final_output: finalOutput,
  }) as Record<string, unknown>
}

function toWatchRunRef(run: TrackedRun): WatchRunRef {
  return {
    runId: run.runId,
    label: run.label,
    agent: run.agent,
    runtimeId: run.runtimeId,
    supervisorId: run.supervisorId,
  }
}

export function createExtension(deps: ExtensionDeps = defaultDeps) {
  return async function (pi: ExtensionAPI) {
    const trackedRuns = new Map<string, TrackedRun>()
    let pollingActive = false
    let pollingGeneration = 0
    let pollInFlight: Promise<RunStatusEntry[]> | null = null
    let sessionCtx: ExtensionContext | null = null

    function upsertTrackedRun(run: WatchRunRef, status?: StatusResult, blocking = false): void {
      const current = trackedRuns.get(run.runId)
      trackedRuns.set(run.runId, {
        runId: run.runId,
        agent: run.agent ?? status?.agent ?? current?.agent ?? 'unknown',
        label: run.label ?? status?.label ?? current?.label ?? run.runId,
        supervisorId: run.supervisorId ?? current?.supervisorId,
        runtimeId: run.runtimeId ?? status?.runtime_id ?? current?.runtimeId,
        startTime: current?.startTime ?? Date.now(),
        lastStatus: status ?? current?.lastStatus,
        blocking: current?.blocking ?? blocking,
        permissionNotified: current?.permissionNotified,
      })
    }

    async function resolveFinalOutput(run: Pick<TrackedRun, 'runId' | 'supervisorId'>, status: StatusResult): Promise<string | undefined> {
      const preview = finalOutputPreview(status)
      if (preview) return preview
      try {
        const inspected = await deps.inspectTool({
          runId: run.runId,
          supervisorId: run.supervisorId,
          limit: INSPECT_LIMIT,
        })
        return finalOutputPreview(inspected.status, inspected.snapshot)
      } catch {
        return undefined
      }
    }

    async function pollRuns(): Promise<RunStatusEntry[]> {
      const liveMap = new Map<string, StatusResult>()
      try {
        const allLive = await deps.statusTool({})
        const liveList = Array.isArray(allLive) ? allLive : [allLive]
        for (const result of liveList) {
          liveMap.set(result.run_id, result)
        }
      } catch (err) {
        console.error('avenor pollRuns: failed to list singleton runs', err)
      }

      const entries: RunStatusEntry[] = []
      for (const run of trackedRuns.values()) {
        const live = findLiveStatusForTrackedRun(run, liveMap.values())
        if (live) {
          liveMap.delete(live.run_id)
          run.lastStatus = live
          entries.push({
            runId: run.runId,
            label: run.label,
            status: live.status,
            phase: live.phase,
            phaseLabel: live.phase_label,
            agent: run.agent,
            pendingPermission: !!live.pending_permission,
            permissionDescription: live.pending_permission?.description,
          })
          continue
        }
        try {
          const raw = await deps.statusTool({
            runId: run.runId,
            supervisorId: run.supervisorId,
          })
          const result = Array.isArray(raw) ? raw[0] : raw
          if (!result) throw new Error('status response was empty')
          run.lastStatus = result
          entries.push({
            runId: run.runId,
            label: run.label,
            status: result.status,
            phase: result.phase,
            phaseLabel: result.phase_label,
            agent: run.agent,
            pendingPermission: !!result.pending_permission,
            permissionDescription: result.pending_permission?.description,
          })
        } catch (err) {
          console.error(`avenor pollRuns: statusTool failed for ${run.runId}`, err)
          const previous = run.lastStatus
          entries.push({
            runId: run.runId,
            label: run.label,
            status: previous?.status ?? 'unavailable',
            phase: previous?.phase,
            phaseLabel: previous?.phase_label,
            agent: run.agent,
            pendingPermission: !!previous?.pending_permission,
            permissionDescription: previous?.pending_permission?.description,
          })
        }
      }

      for (const live of liveMap.values()) {
        entries.push({
          runId: live.run_id,
          label: live.label,
          status: live.status,
          phase: live.phase,
          phaseLabel: live.phase_label,
          agent: live.agent ?? 'unknown',
          pendingPermission: !!live.pending_permission,
          permissionDescription: live.pending_permission?.description,
        })
      }

      return entries
    }

    function updateWidget(entries: RunStatusEntry[]): void {
      if (!sessionCtx) return

      const lines = renderStatusLines(entries)
      if (lines.length > 0) {
        sessionCtx.ui.setWidget('avenor-status', lines)
        sessionCtx.ui.setStatus('avenor', lines.map(line => line.slice(0, 60)).join('  '))
      } else {
        sessionCtx.ui.setWidget('avenor-status', undefined)
        sessionCtx.ui.setStatus('avenor', '')
      }
    }

    function startPolling(): void {
      if (pollingActive) return
      pollingActive = true
      const generation = ++pollingGeneration

      const tick = async () => {
        if (!pollingActive || generation !== pollingGeneration) return

        const prevStatuses = new Map<string, string | undefined>()
        for (const [id, run] of trackedRuns) {
          prevStatuses.set(id, run.lastStatus?.status)
        }

        const currentPoll = pollRuns()
        pollInFlight = currentPoll
        const entries = await currentPoll.finally(() => {
          if (pollInFlight === currentPoll) pollInFlight = null
        })
        if (!pollingActive || generation !== pollingGeneration) return

        updateWidget(entries)

        for (const entry of entries) {
          const run = trackedRuns.get(entry.runId)
          if (!run) continue

          if (isTerminalStatus(entry.status)) {
            const prevStatus = prevStatuses.get(entry.runId)
            if (!prevStatus || !isTerminalStatus(prevStatus)) {
              sessionCtx?.ui.notify(
                `Sub-agent "${entry.label}" finished: ${entry.status}`,
                entry.status === 'done' ? 'info' : 'warning',
              )
              if (!run.blocking && run.lastStatus) {
                const finalOutput = await resolveFinalOutput(run, run.lastStatus)
                try {
                  pi.sendUserMessage(
                    buildCompletionText({ runId: run.runId, label: run.label }, run.lastStatus, finalOutput),
                    { deliverAs: 'followUp' },
                  )
                } catch (err) {
                  console.error('avenor tick: sendUserMessage failed', err)
                }
              }
            }
            trackedRuns.delete(entry.runId)
          } else if (
            entry.pendingPermission &&
            !run.blocking &&
            !run.permissionNotified
          ) {
            run.permissionNotified = true
            if (run.lastStatus) {
              try {
                pi.sendUserMessage(
                  buildWaitingText({ runId: run.runId, label: run.label, agent: run.agent }, run.lastStatus),
                  { deliverAs: 'followUp' },
                )
              } catch (err) {
                console.error('avenor tick: sendUserMessage for permission failed', err)
              }
            }
          } else if (!entry.pendingPermission && run.permissionNotified) {
            run.permissionNotified = false
          }
        }

        const hasActive = entries.some(entry => !isTerminalStatus(entry.status))
        if (hasActive && pollingActive && generation === pollingGeneration) {
          setTimeout(() => {
            void tick().catch(err => {
              pollingActive = false
              console.error('avenor polling loop failed', err)
            })
          }, POLL_INTERVAL_MS)
        } else {
          pollingActive = false
          sessionCtx?.ui.setStatus('avenor', '')
          sessionCtx?.ui.setWidget('avenor-status', undefined)
        }
      }

      void tick().catch(err => {
        pollingActive = false
        console.error('avenor polling loop failed', err)
      })
    }

    async function stopPolling(): Promise<void> {
      pollingActive = false
      pollingGeneration++
      await pollInFlight?.catch(() => {})
      sessionCtx?.ui.setStatus('avenor', '')
      sessionCtx?.ui.setWidget('avenor-status', undefined)
    }

    function createWatchDeps(): WatchDeps {
      async function resolveSupervisorSocket(run: WatchRunRef): Promise<string> {
        if (run.supervisorId) return run.supervisorId
        const sup = await deps.Supervisor.get()
        return sup.supervisorId
      }

      async function withClient<T>(run: WatchRunRef, work: (client: Client) => Promise<T>): Promise<T> {
        const socket = await resolveSupervisorSocket(run)
        const client = await deps.dial(socket)
        try {
          return await work(client)
        } finally {
          client.close()
        }
      }

      return {
        inspectRun(run) {
          return deps.inspectTool({
            runId: run.runId,
            supervisorId: run.supervisorId,
            limit: INSPECT_LIMIT,
          })
        },
        async observeRun(run, afterSeq, initialSnapshot) {
          const runtimeId = run.runtimeId
          if (!runtimeId) return null
          const socket = await resolveSupervisorSocket(run)
          const client = await deps.dial(socket)
          return deps.observeRun(client, {
            runtime_id: runtimeId,
            replay: true,
            after_seq: afterSeq,
            initial_snapshot: initialSnapshot,
            close_client: true,
          })
        },
        cancelRun(run, runtimeId) {
          return withClient(run, async client => {
            await client.cancel(runtimeId)
          })
        },
        promptRun(run, runtimeId, text) {
          return withClient(run, async client => {
            await client.prompt(runtimeId, text)
          })
        },
        followUpRun(run, text) {
          return deps.followUpTool({
            runId: run.runId,
            message: text,
            supervisorId: run.supervisorId,
          })
        },
      }
    }

    async function followUpTrackedRun(run: WatchRunRef, text: string): Promise<{ run_id: string; label: string }> {
      const result = await deps.followUpTool({
        runId: run.runId,
        message: text,
        supervisorId: run.supervisorId,
      })
      try {
        const rawStatus = await deps.statusTool({ runId: result.run_id, supervisorId: run.supervisorId })
        const status = Array.isArray(rawStatus) ? rawStatus[0] : rawStatus
        upsertTrackedRun({
          runId: result.run_id,
          label: result.label,
          agent: status?.agent ?? run.agent,
          runtimeId: status?.runtime_id,
          supervisorId: run.supervisorId,
        }, status, false)
        startPolling()
      } catch {
        upsertTrackedRun({
          runId: result.run_id,
          label: result.label,
          agent: run.agent,
          supervisorId: run.supervisorId,
        }, undefined, false)
        startPolling()
      }
      return result
    }

    async function waitForRunWithObserver(
      run: WatchRunRef,
      signal: AbortSignal | undefined,
      onUpdate: ((update: { content: Array<{ type: 'text'; text: string }>; details?: Record<string, unknown> }) => void) | undefined,
    ): Promise<{ status?: StatusResult; aborted: boolean }> {
      let observer: RunObserver | null = null
      let unsubscribe: (() => void) | undefined
      let lastUpdate = ''
      let abortListener: (() => void) | undefined

      const emit = (snapshot: RunSnapshot) => {
        const text = snapshotProgressText(run.label ?? run.runId, snapshot)
        if (text === lastUpdate) return
        lastUpdate = text
        onUpdate?.({
          content: [{ type: 'text', text }],
          details: {
            status: snapshot.pending_permission ? 'waiting' : snapshot.ended ? 'done' : 'running',
            run_id: run.runId,
          },
        })
      }

      const waitPromise = new Promise<'settled'>((resolve) => {
        const complete = () => resolve('settled')
        void (async () => {
          try {
            observer = await createWatchDeps().observeRun(run)
            if (!observer) {
              complete()
              return
            }
            const maybeComplete = (snapshot: RunSnapshot) => {
              emit(snapshot)
              if (snapshot.pending_permission || snapshot.ended) {
                complete()
              }
            }
            unsubscribe = observer.subscribe((snapshot) => {
              maybeComplete(snapshot)
            })
            const current = observer.snapshot()
            if (current.last_event || current.transcript.length > 0 || current.pending_permission || current.ended) {
              maybeComplete(current)
            }
          } catch {
            complete()
          }
        })()
      })

      const abortPromise = signal
        ? new Promise<'aborted'>((resolve) => {
            if (signal.aborted) {
              resolve('aborted')
              return
            }
            const onAbort = () => resolve('aborted')
            abortListener = onAbort
            signal.addEventListener('abort', onAbort, { once: true })
          })
        : undefined

      const outcome = abortPromise ? await Promise.race([waitPromise, abortPromise]) : await waitPromise

      if (signal && abortListener) {
        signal.removeEventListener('abort', abortListener)
      }
      unsubscribe?.()
      await observer?.close().catch(() => {})

      if (outcome === 'aborted') {
        return { aborted: true }
      }

      try {
        const raw = await deps.statusTool({ runId: run.runId, supervisorId: run.supervisorId })
        const status = Array.isArray(raw) ? raw[0] : raw
        return { status, aborted: false }
      } catch {
        return { aborted: false }
      }
    }

    async function waitForRunByPolling(
      run: WatchRunRef,
      signal: AbortSignal | undefined,
      onUpdate: ((update: { content: Array<{ type: 'text'; text: string }>; details?: Record<string, unknown> }) => void) | undefined,
    ): Promise<{ status?: StatusResult; aborted: boolean }> {
      let firstPoll = true
      while (!signal?.aborted) {
        if (!firstPoll) await sleep(POLL_INTERVAL_MS)
        firstPoll = false

        try {
          const raw = await deps.statusTool({
            runId: run.runId,
            supervisorId: run.supervisorId,
          })
          const status = Array.isArray(raw) ? raw[0] : raw
          if (!status) continue

          const text = status.pending_permission
            ? `[blocked] ${run.label ?? run.runId} — permission: ${status.pending_permission.description}`
            : `${run.label ?? run.runId}${status.phase_label ? ` (${status.phase_label})` : ''}`
          onUpdate?.({
            content: [{ type: 'text', text }],
            details: { status: status.status, run_id: run.runId },
          })

          if (status.status === 'waiting' || isTerminalStatus(status.status)) {
            return { status, aborted: false }
          }
        } catch (err) {
          console.error('avenor spawn poll: statusTool failed', err)
        }
      }

      return { aborted: true }
    }

    async function waitForRun(
      run: WatchRunRef,
      signal: AbortSignal | undefined,
      onUpdate: ((update: { content: Array<{ type: 'text'; text: string }>; details?: Record<string, unknown> }) => void) | undefined,
    ): Promise<{ status?: StatusResult; aborted: boolean }> {
      const observed = await waitForRunWithObserver(run, signal, onUpdate)
      if (observed.status || observed.aborted) return observed
      return waitForRunByPolling(run, signal, onUpdate)
    }

    pi.on('session_start', async (_event, ctx) => {
      sessionCtx = ctx
    })

    pi.on('session_shutdown', async () => {
      await stopPolling()
      sessionCtx = null
    })

    pi.on('before_agent_start', async () => {
      if (trackedRuns.size === 0) return

      const entries = await pollRuns()
      const active = entries.filter(entry => !isTerminalStatus(entry.status))
      if (active.length === 0) return

      const lines = active.map(entry => {
        const perm = entry.pendingPermission ? ` [blocked: ${entry.permissionDescription ?? 'permission'}]` : ''
        const phase = entry.phaseLabel ? ` (${entry.phaseLabel})` : ''
        return `  • ${entry.label} (${entry.agent}): ${entry.status}${phase}${perm}`
      })

      return {
        message: {
          customType: 'avenor-active-runs',
          content: `Active sub-agents:\n${lines.join('\n')}`,
          display: true,
        },
      }
    })

    pi.registerMessageRenderer('avenor-active-runs', (message, _options, theme: Theme) => {
      const lines = (message.content as string).split('\n')
      let text = ''
      for (let i = 0; i < lines.length; i++) {
        text += i === 0
          ? theme.fg('accent', theme.bold(lines[i] ?? ''))
          : theme.fg('muted', lines[i] ?? '')
        if (i < lines.length - 1) text += '\n'
      }
      return new Text(text, 0, 0)
    })

    pi.registerTool({
      name: 'avenor_spawn',
      label: 'Avenor Spawn',
      description:
        'Dispatch an agent run via avenor. Blocks by default, showing live progress. Set wait=false for fire-and-forget.',
      promptSnippet: 'Dispatch sub-agent runs via avenor (spawn, status, result, inspect, events, follow-up, shutdown)',
      promptGuidelines: [
        'Use avenor_spawn to delegate well-defined tasks to sub-agents.',
        'Use avenor_status with view "lifecycle" only to check progress or pending permissions.',
        'Use avenor_result to wait for and retrieve a sub-agent final result.',
        'Use avenor_inspect to review a bounded transcript and tool activity from a run.',
        'Use avenor_events only when raw event payloads are specifically needed.',
        'Use avenor_follow_up to iterate on a completed run.',
        'Use avenor_shutdown when done delegating work.',
      ],
      parameters: Type.Object({
        agent: Type.String({ description: 'Agent name (required, no default)' }),
        prompt: Type.Optional(Type.String({ description: 'Prompt text' })),
        prompt_file: Type.Optional(Type.String({ description: 'Path to prompt file' })),
        dir: Type.Optional(Type.String({ description: 'Working directory (defaults to session directory)' })),
        label: Type.Optional(Type.String({ description: 'Human-readable label for the run' })),
        timeout: Type.Optional(Type.String({ description: 'Timeout duration (e.g. 3600s)' })),
        model: Type.Optional(Type.String({ description: 'Model override' })),
        backend: Type.Optional(Type.String({ description: 'Backend override' })),
        server_url: Type.Optional(Type.String({ description: 'Backend server URL' })),
        supervisor_id: Type.Optional(Type.String({ description: 'Reuse an existing supervisor by socket path' })),
        wait: Type.Optional(Type.Boolean({ description: 'Block until complete (default true)' })),
      }),
      async execute(_toolCallId, params, signal, onUpdate, ctx) {
        if (signal?.aborted) {
          return { content: [{ type: 'text', text: 'Cancelled' }] }
        }

        const wait = params.wait ?? true
        const dir = params.dir ?? ctx.cwd
        const label = params.label ?? `${params.agent}-${Date.now()}`

        const result = await deps.spawnTool({
          agent: params.agent,
          prompt: params.prompt,
          promptFile: params.prompt_file,
          label,
          dir,
          timeout: params.timeout,
          model: params.model,
          backend: params.backend,
          serverUrl: params.server_url,
          supervisorId: params.supervisor_id,
        })

        const supervisorId = statusSupervisorId(params.supervisor_id, result.supervisor_id)
        const runRef: WatchRunRef = {
          runId: result.run_id,
          agent: params.agent,
          label,
          supervisorId,
          runtimeId: result.runtime_id,
        }
        upsertTrackedRun(runRef, undefined, wait)
        startPolling()

        if (!wait) {
          return {
            content: [{
              type: 'text',
              text: `Dispatched "${label}" (run_id: ${result.run_id}). Completion will be delivered automatically; use avenor_status with view "lifecycle" for progress or avenor_result to wait for the final result.`,
            }],
            details: { run_id: result.run_id, label },
          }
        }

        onUpdate?.({
          content: [{ type: 'text', text: `Starting ${label}…` }],
          details: { status: 'running', run_id: result.run_id },
        })

        const waitResult = await waitForRun(runRef, signal, onUpdate)
        if (waitResult.aborted) {
          trackedRuns.delete(result.run_id)
          return {
            content: [{ type: 'text', text: `Monitoring of "${label}" (run_id: ${result.run_id}) was interrupted. Use avenor_status or avenor_inspect to check.` }],
            details: { run_id: result.run_id },
          }
        }

        const status = waitResult.status
        if (!status) {
          return {
            content: [{ type: 'text', text: `Sub-agent "${label}" is still running. Use avenor_status or avenor_inspect to check progress.` }],
            details: { status: 'running', run_id: result.run_id },
          }
        }

        upsertTrackedRun(runRef, status, wait)

        if (status.status === 'waiting') {
          const tracked = trackedRuns.get(result.run_id)
          if (tracked) tracked.permissionNotified = true
          return {
            content: [{ type: 'text', text: buildWaitingText({ runId: result.run_id, label, agent: params.agent }, status) }],
            details: {
              status: 'waiting',
              run_id: result.run_id,
              session_id: status.session_id,
              ...(status.pending_permission && { pending_permission: status.pending_permission }),
            },
          }
        }

        if (isTerminalStatus(status.status)) {
          const finalOutput = await resolveFinalOutput({ runId: result.run_id, supervisorId }, status)
          trackedRuns.delete(result.run_id)
          return {
            content: [{ type: 'text', text: buildCompletionText({ runId: result.run_id, label }, status, finalOutput) }],
            details: {
              status: status.status,
              run_id: result.run_id,
              session_id: status.session_id,
              final_output: finalOutput,
            },
          }
        }

        return {
          content: [{ type: 'text', text: `${label} is ${status.status}. Use avenor_status or avenor_inspect to check progress.` }],
          details: { status: status.status, run_id: result.run_id },
        }
      },
      renderCall(args, theme) {
        const agent = theme.fg('toolTitle', theme.bold('avenor_spawn '))
        const agentName = theme.fg('accent', args.agent)
        const label = args.label ? theme.fg('dim', ` "${args.label}"`) : ''
        const wait = args.wait === false ? theme.fg('warning', ' [async]') : ''
        return new Text(agent + agentName + label + wait, 0, 0)
      },
      renderResult(result, { isPartial }, theme) {
        if (isPartial) {
          const status = result.details?.status ?? 'running'
          const emoji = statusEmoji(String(status))
          return new Text(theme.fg('accent', `${emoji} ${result.content?.[0]?.text ?? 'Running…'}`), 0, 0)
        }

        const status = String(result.details?.status ?? '')
        if (status && isTerminalStatus(status)) {
          const emoji = statusEmoji(status)
          const header = theme.fg(status === 'done' ? 'success' : 'error', `${emoji} ${status}`)
          const body = (result.content?.[0]?.text ?? '').split('\n').map(line => theme.fg('muted', `  ${line}`)).join('\n')
          return new Text(`${header}\n${body}`, 0, 0)
        }

        const text = (result.content?.[0]?.text ?? '').split('\n').map(line => theme.fg('muted', line)).join('\n')
        return new Text(text, 0, 0)
      },
    })

    pi.registerTool({
      name: 'avenor_status',
      label: 'Avenor Status',
      description: 'Get lifecycle status of a run or all runs. Use view="lifecycle" for compact polling; full is the compatibility default.',
      parameters: Type.Object({
        run_id: Type.Optional(Type.String({ description: 'Specific run ID to query' })),
        view: Type.Optional(Type.Unsafe<'lifecycle' | 'full'>({
          type: 'string',
          enum: ['lifecycle', 'full'],
          description: 'Response detail (default full for compatibility)',
        })),
        supervisor_id: Type.Optional(Type.String({ description: 'Reuse an existing supervisor by socket path' })),
      }),
      async execute(_toolCallId, params) {
        const result = await deps.statusTool({
          runId: params.run_id,
          supervisorId: params.supervisor_id,
          view: params.view,
        })
        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
          details: result,
        }
      },
    })

    pi.registerTool({
      name: 'avenor_result',
      label: 'Avenor Result',
      description: 'Wait for a run to finish and return its bounded final output without transcript or event details.',
      parameters: Type.Object({
        run_id: Type.String({ description: 'Run ID to await' }),
        wait: Type.Optional(Type.Boolean({ description: 'Wait for a terminal result (default true)' })),
        timeout: Type.Optional(Type.String({ description: 'Maximum time to wait (e.g. 30s, 5m)' })),
        supervisor_id: Type.Optional(Type.String({ description: 'Reuse an existing supervisor by socket path' })),
      }),
      async execute(_toolCallId, params, signal) {
        const tracked = trackedRuns.get(params.run_id)
        const previousBlocking = tracked?.blocking
        if (tracked) tracked.blocking = true
        let completed = false

        try {
          const result = await deps.resultTool({
            runId: params.run_id,
            supervisorId: params.supervisor_id,
            wait: params.wait,
            timeout: params.timeout,
            signal,
          })
          completed = result.ready
          if (completed) {
            trackedRuns.delete(params.run_id)
          } else if (tracked && result.status === 'waiting') {
            tracked.permissionNotified = true
          }
          return {
            content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            details: result,
          }
        } finally {
          if (!completed && tracked && trackedRuns.get(params.run_id) === tracked) {
            tracked.blocking = previousBlocking ?? false
          }
        }
      },
    })

    pi.registerTool({
      name: 'avenor_inspect',
      label: 'Avenor Inspect',
      description: 'Inspect a run with bounded snapshot, transcript, tool, permission, and final output details.',
      parameters: Type.Object({
        run_id: Type.String({ description: 'Run ID to inspect' }),
        supervisor_id: Type.Optional(Type.String({ description: 'Reuse an existing supervisor by socket path' })),
        limit: Type.Optional(Type.Number({ description: 'Replay event limit (default 128)' })),
        after_seq: Type.Optional(Type.Number({ description: 'Only include events after this sequence number' })),
      }),
      async execute(_toolCallId, params) {
        const result = await deps.inspectTool({
          runId: params.run_id,
          supervisorId: params.supervisor_id,
          limit: params.limit,
          after_seq: params.after_seq,
        })
        const payload = buildInspectPayload(result)
        return {
          content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }],
          details: payload,
        }
      },
    })

    pi.registerTool({
      name: 'avenor_answer_permission',
      label: 'Avenor Answer Permission',
      description:
        'Answer a pending permission request for a sub-agent. Pass option_id from avenor_status pending_permission.options.',
      parameters: Type.Object({
        run_id: Type.String({ description: 'Run ID with the pending permission' }),
        option_id: Type.String({ description: 'allow_once | allow_always | deny' }),
        request_id: Type.Optional(Type.String({ description: 'Permission request ID (auto-discovered if omitted)' })),
        supervisor_id: Type.Optional(Type.String({ description: 'Reuse an existing supervisor by socket path' })),
      }),
      async execute(_toolCallId, params) {
        const result = await deps.answerPermissionTool({
          runId: params.run_id,
          optionId: params.option_id,
          requestId: params.request_id,
          supervisorId: params.supervisor_id,
        })
        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
          details: result,
        }
      },
    })

    pi.registerTool({
      name: 'avenor_follow_up',
      label: 'Avenor Follow Up',
      description: 'Resume a completed run with a follow-up message.',
      parameters: Type.Object({
        run_id: Type.String({ description: 'Completed run ID to resume' }),
        message: Type.String({ description: 'Follow-up message text' }),
        label: Type.Optional(Type.String({ description: 'Override label (defaults to <original>-followup)' })),
        supervisor_id: Type.Optional(Type.String({ description: 'Reuse an existing supervisor by socket path' })),
      }),
      async execute(_toolCallId, params) {
        const result = await deps.followUpTool({
          runId: params.run_id,
          message: params.message,
          label: params.label,
          supervisorId: params.supervisor_id,
        })
        try {
          const rawStatus = await deps.statusTool({ runId: result.run_id, supervisorId: params.supervisor_id })
          const status = Array.isArray(rawStatus) ? rawStatus[0] : rawStatus
          upsertTrackedRun({
            runId: result.run_id,
            label: result.label,
            agent: status?.agent,
            runtimeId: status?.runtime_id,
            supervisorId: params.supervisor_id,
          }, status, false)
        } catch {
          upsertTrackedRun({
            runId: result.run_id,
            label: result.label,
            supervisorId: params.supervisor_id,
          }, undefined, false)
        }
        startPolling()
        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
          details: result,
        }
      },
    })

    pi.registerTool({
      name: 'avenor_events',
      label: 'Avenor Events',
      description: 'Read events from a run. Filter by type. Returns last N events.',
      parameters: Type.Object({
        run_id: Type.String({ description: 'Run ID to read events from' }),
        types: Type.Optional(Type.Array(Type.String(), { description: 'Filter by event types' })),
        limit: Type.Optional(Type.Number({ description: 'Max events to return (default 50)' })),
        supervisor_id: Type.Optional(Type.String({ description: 'Reuse an existing supervisor by socket path' })),
      }),
      async execute(_toolCallId, params) {
        const result = await deps.eventsTool({
          runId: params.run_id,
          types: params.types,
          limit: params.limit,
          supervisorId: params.supervisor_id,
        })
        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
          details: result,
        }
      },
    })

    pi.registerTool({
      name: 'avenor_shutdown',
      label: 'Avenor Shutdown',
      description: 'Shut down the avenor supervisor and clean up temp files.',
      parameters: Type.Object({
        supervisor_id: Type.Optional(Type.String({ description: 'Supervisor to shut down (defaults to singleton)' })),
        force: Type.Optional(Type.Boolean({ description: 'Force shutdown instead of graceful' })),
      }),
      async execute(_toolCallId, params) {
        await stopPolling()
        trackedRuns.clear()
        const result = await deps.shutdownTool({
          supervisorId: params.supervisor_id,
          force: params.force,
        })
        return {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
          details: result,
        }
      },
    })

    pi.registerCommand('avenor-status', {
      description: 'Show status of all avenor runs',
      handler: async (_args, ctx) => {
        const entries = await pollRuns()
        if (entries.length === 0) {
          ctx.ui.notify('No active avenor runs', 'info')
          return
        }
        ctx.ui.setWidget('avenor-status-detail', entries.map(entry => formatRunLine(entry)))
      },
    })

    pi.registerCommand('avenor-watch', {
      description: 'Open a live run inspector by run ID, label, or agent name',
      getArgumentCompletions: (prefix: string) => {
        const items = Array.from(trackedRuns.values())
          .filter(run => run.label.startsWith(prefix) || run.agent.startsWith(prefix) || run.runId.startsWith(prefix))
          .map(run => ({
            value: run.runId,
            label: `${sanitizeText(run.label)} (${sanitizeText(run.agent)}, ${sanitizeText(run.runId).slice(0, 8)})`,
          }))
        return items.length > 0 ? items : null
      },
      handler: async (args, ctx) => {
        await openRunInspector(ctx, {
          trackedRuns: Array.from(trackedRuns.values()).map(toWatchRunRef),
          initialRunId: args?.trim() || undefined,
          deps: {
            ...createWatchDeps(),
            followUpRun: (run, text) => followUpTrackedRun(run, text),
          },
          onTrackRun: (run, status) => {
            upsertTrackedRun(run, status, false)
            if (status && !isTerminalStatus(status.status)) {
              startPolling()
            }
          },
        })
      },
    })

    pi.registerCommand('avenor-cancel', {
      description: 'Cancel a running avenor sub-agent',
      getArgumentCompletions: (prefix: string) => {
        const items = Array.from(trackedRuns.values())
          .filter(run => run.label.startsWith(prefix) || run.agent.startsWith(prefix) || run.runId.startsWith(prefix))
          .map(run => ({
            value: run.runId,
            label: `${sanitizeText(run.label)} (${sanitizeText(run.agent)}, ${sanitizeText(run.runId).slice(0, 8)})`,
          }))
        return items.length > 0 ? items : null
      },
      handler: async (args, ctx) => {
        if (!args?.trim()) {
          ctx.ui.notify('Usage: /avenor-cancel <run_id>', 'warning')
          return
        }
        const runId = args.trim()
        const run = trackedRuns.get(runId)
        const label = run?.label ?? runId
        const runtimeId = run?.runtimeId ?? run?.lastStatus?.runtime_id
        if (!runtimeId) {
          ctx.ui.notify(`No runtime id available for "${label}"`, 'warning')
          return
        }

        const ok = await ctx.ui.confirm('Cancel run', `Cancel "${label}" (${runId.slice(0, 8)})?`)
        if (!ok) return

        try {
          const watchDeps = createWatchDeps()
          await watchDeps.cancelRun(toWatchRunRef(run ?? { runId, agent: 'unknown', label, startTime: Date.now() }), runtimeId)
          ctx.ui.notify(`Cancelled "${label}"`, 'info')
        } catch (err) {
          ctx.ui.notify(`Failed to cancel: ${err instanceof Error ? err.message : String(err)}`, 'error')
        }
      },
    })
  }
}

export default createExtension()
