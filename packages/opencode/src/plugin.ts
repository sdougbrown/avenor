import type { Plugin, ToolContext } from '@opencode-ai/plugin'
import { tool } from '@opencode-ai/plugin'

import * as crypto from 'node:crypto'
import {
  answerPermissionTool,
  createRunSnapshot,
  dial,
  eventsTool,
  followUpTool,
  inspectTool,
  observeRun,
  resultTool,
  type RunObserver,
  type RunSnapshot,
  shutdownTool,
  spawnTool,
  statusTool,
  Supervisor,
  type StatusResult,
} from '@dougbots/avenor-core'

type TrackedRun = {
  runId: string
  runtimeId?: string
  sessionId?: string
  agent: string
  orchestratorSessionId: string
  label: string
  supervisorId?: string
  monitoring: boolean
  latestSeq?: number
  lastSnapshot?: RunSnapshot
}

type MonitorStopReason = 'permission-routed' | 'shutdown' | 'abort' | 'result'

type MonitorOutcome = {
  kind: 'terminal' | 'waiting' | 'aborted' | 'shutdown' | 'stopped' | 'error'
  snapshot: RunSnapshot
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

export function terminalStatus(snapshot: RunSnapshot, fallback?: StatusResult): string {
  if (fallback?.status) return fallback.status
  if (snapshot.phase && TERMINAL_STATUSES.has(snapshot.phase)) return snapshot.phase
  return snapshot.stop_reason ?? 'done'
}

function formatLiveTool(snapshot: RunSnapshot): string | undefined {
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

function formatTranscriptPreview(snapshot: RunSnapshot): string | undefined {
  const lines = snapshot.transcript
    .slice(-TRANSCRIPT_PREVIEW_ENTRIES)
    .map(formatTranscriptEntry)
    .filter((line): line is string => Boolean(line))
  if (lines.length === 0) return undefined
  return clipTail(lines.join('\n'), METADATA_TEXT_CHARS)
}

function buildSnapshotMetadata(snapshot: RunSnapshot): Record<string, unknown> {
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
  return metadata
}

export function buildWaitingText(run: TrackedRun, snapshot: RunSnapshot): string {
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
      `Use \`avenor_follow_up\` with run_id "${run.runId}" to respond, or \`avenor_status\` to re-check.`,
    )
  }

  return lines.join('\n')
}

function buildCompletionText(
  run: TrackedRun,
  snapshot: RunSnapshot,
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
    `Call \`avenor_events\` with \`run_id: "${run.runId}"\` for raw events, or \`avenor_follow_up\` to iterate.`,
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

function formatChannelBlocks(text: string): string {
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

export const AvenorPlugin: Plugin = async (ctx) => {
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

  function syncRunSnapshot(run: TrackedRun, snapshot: RunSnapshot): void {
    run.lastSnapshot = snapshot
    if (snapshot.latest_seq !== undefined) run.latestSeq = snapshot.latest_seq
    registerSessionId(snapshot.identity.session_id, snapshot.identity.runtime_id, run.runId)
  }

  async function primeTrackedRun(run: TrackedRun, options: { captureLatestSeq?: boolean } = {}): Promise<StatusResult | undefined> {
    try {
      const raw = await statusTool({ runId: run.runId, supervisorId: run.supervisorId })
      const status = Array.isArray(raw) ? raw[0] : raw
      if (!status) return undefined
      registerSessionId(status.session_id, status.runtime_id, run.runId)
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
      return await inspectTool({
        runId: run.runId,
        supervisorId: run.supervisorId,
      })
    } catch {
      return undefined
    }
  }

  async function observeTrackedRun(
    run: TrackedRun,
    options: {
      abort?: AbortSignal
      onSnapshot?: (snapshot: RunSnapshot) => void
    } = {},
  ): Promise<MonitorOutcome> {
    const existing = activeMonitors.get(run.runId)
    if (existing) return existing.promise

    run.monitoring = true
    const handle: ActiveMonitor = { promise: Promise.resolve({ kind: 'stopped', snapshot: run.lastSnapshot as RunSnapshot }) }

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
    await ctx.client.session.promptAsync({
      path: { id: sessionId },
      body: { parts: [{ type: 'text', text }] },
    }).catch(() => {})
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
          const from = payload.from_run_id ?? msg.from_run_id ?? 'unknown'
          const role = payload.role ?? ''
          const resolvedRunId = trackedRuns.has(from) ? from
            : sessionIdToRunId.get(from)
            ?? runtimeIdToRunId.get(from)
          const runInfo = resolvedRunId ? trackedRuns.get(resolvedRunId) : undefined
          const source = runInfo?.agent ?? (role ? `agent-${role}` : 'agent')
          const channelText = formatAgentMessage(source, from, role, text)
          try {
            await ctx.client.session.promptAsync({
              path: { id: sessionId },
              body: { parts: [{ type: 'text', text: channelText }] },
            })
          } catch {
            return
          }
        }
      }
    } finally {
      channelPolling = false
    }
  }

  return {
    event: async ({ event }) => {
      if (event.type !== 'session.idle') return
      const { sessionID } = (
        event as { type: 'session.idle'; properties: { sessionID: string } }
      ).properties

      pollChannelMessages(sessionID).catch(console.error)

      for (const run of trackedRuns.values()) {
        if (run.orchestratorSessionId === sessionID && !run.monitoring) {
          monitorRun(run).catch(console.error)
        }
      }
    },

    'permission.ask': async (permission, _output) => {
      const runId = sessionIdToRunId.get(permission.sessionID)
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
        `  request_id: "${permission.id}"`,
        '  option_id: "allow_once" | "allow_always" | "deny"',
      ]

      await primeTrackedRun(run, { captureLatestSeq: true })
      await stopMonitor(run.runId, 'permission-routed')
      await promptSession(run.orchestratorSessionId, lines.join('\n'))
    },

    'chat.message': async (message: any, _output: any) => {
      if (!message?.parts) return
      for (const part of message.parts) {
        if (part?.type === 'text' && typeof part?.text === 'string' && part.text.includes('<channel ')) {
          part.text = formatChannelBlocks(part.text)
        }
      }
    },

    'experimental.text.complete': async (
      _input: { sessionID: string; messageID: string; partID: string },
      output: { text: string },
    ) => {
      if (typeof output.text !== 'string' || !output.text.includes('<channel ')) return
      try {
        output.text = formatChannelBlocks(output.text)
      } catch {
        // Leave text as-is if formatting fails
      }
    },

    tool: {
      avenor_spawn: tool({
        description:
          'Dispatch an agent run via avenor. Blocks by default, showing live progress as an updating tool call. Set wait=false for fire-and-forget — you will be re-prompted automatically on completion.',
        args: {
          agent: tool.schema.string().describe('Agent name (required, no default)'),
          prompt: tool.schema.string().optional().describe('Prompt text'),
          prompt_file: tool.schema.string().optional().describe('Path to prompt file'),
          dir: tool.schema.string().optional().describe('Working directory for the run (defaults to the session project directory)'),
          label: tool.schema.string().optional().describe('Human-readable label for the run'),
          timeout: tool.schema.string().optional().describe('Timeout duration (e.g. 3600s)'),
          model: tool.schema.string().optional().describe('Model override'),
          backend: tool.schema.string().optional().describe('Backend override'),
          server_url: tool.schema.string().optional().describe('Backend server URL'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
          wait: tool.schema.boolean().default(true).describe(
            'Block until complete with live status updates. False = fire-and-forget.',
          ),
        },
        async execute(args, context: ToolContext) {
          const ownerRunId = ensureParentRunId()
          const result = await spawnTool({
            agent: args.agent,
            prompt: args.prompt,
            promptFile: args.prompt_file,
            label: args.label,
            dir: args.dir ?? context.directory,
            timeout: args.timeout,
            model: args.model,
            backend: args.backend,
            serverUrl: args.server_url,
            supervisorId: args.supervisor_id,
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
            }
          }

          context.metadata({ title: `Starting ${result.label}…` })

          const outcome = await observeTrackedRun(run, {
            abort: context.abort,
            onSnapshot: (snapshot) => {
              context.metadata({
                title: snapshot.pending_permission ? `[blocked] ${result.label}` : result.label,
                metadata: buildSnapshotMetadata(snapshot),
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
                ...buildSnapshotMetadata(snapshot),
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
                ...buildSnapshotMetadata(snapshot),
              },
            }
          }

          if (outcome.kind === 'error') {
            return {
              title: `${result.label} — monitoring error`,
              output: `Monitoring failed: ${outcome.error ?? 'unknown error'}. The run may still be active — use avenor_status to check.`,
              metadata: { run_id: result.run_id },
            }
          }

          unregisterRun(result.run_id)
          return {
            title: `${result.label} — aborted`,
            output: `Monitoring of "${result.label}" (run_id: ${result.run_id}) was interrupted. The run may still be active — use avenor_status to check.`,
            metadata: { run_id: result.run_id },
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
          return JSON.stringify(result, null, 2)
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
            return JSON.stringify(result, null, 2)
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
        },
        async execute(args, _context) {
          const result = await answerPermissionTool({
            runId: args.run_id,
            optionId: args.option_id,
            requestId: args.request_id,
            supervisorId: args.supervisor_id,
          })
          return JSON.stringify(result, null, 2)
        },
      }),

      avenor_follow_up: tool({
        description: 'Resume a completed run with a follow-up message.',
        args: {
          run_id: tool.schema.string().describe('Completed run ID to resume'),
          message: tool.schema.string().describe('Follow-up message text'),
          label: tool.schema.string().optional().describe('Override label (defaults to <original>-followup)'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, context) {
          const result = await followUpTool({
            runId: args.run_id,
            message: args.message,
            label: args.label,
            supervisorId: args.supervisor_id,
          })

          const priorRun = trackedRuns.get(args.run_id)
          const run: TrackedRun = {
            runId: result.run_id,
            agent: priorRun?.agent ?? 'agent',
            orchestratorSessionId: context.sessionID,
            label: result.label,
            supervisorId: args.supervisor_id ?? Supervisor.currentInstance()?.supervisorId,
            monitoring: false,
          }
          trackedRuns.set(result.run_id, run)
          await primeTrackedRun(run)
          void monitorRun(run).catch(console.error)

          return JSON.stringify(result, null, 2)
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
          return JSON.stringify(result, null, 2)
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
          return JSON.stringify(result, null, 2)
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
          return JSON.stringify(result, null, 2)
        },
      }),
    },
  }
}
