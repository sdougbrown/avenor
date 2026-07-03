import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent'
import type { Theme } from '@earendil-works/pi-tui'
import { Text } from '@earendil-works/pi-tui'
import { Type } from 'typebox'
import {
  spawnTool,
  statusTool,
  eventsTool,
  answerPermissionTool,
  followUpTool,
  shutdownTool,
  type StatusResult,
} from '@dougbots/avenor-core'
import { EventFeedOverlay } from './watch.js'
import type { TrackedRun, RunStatusEntry } from './types.js'
import { TERMINAL_STATUSES, formatRunLine, statusEmoji } from './types.js'

const POLL_INTERVAL_MS = 3_000

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function buildCompletionText(run: { runId: string; label: string }, result: StatusResult): string {
  const lines = [
    `Sub-agent "${run.label}" finished with status **${result.status}**.`,
  ]
  if (result.stop_reason && result.stop_reason.toUpperCase() !== result.status.toUpperCase()) {
    lines.push(`Stop reason: ${result.stop_reason}`)
  }
  if (result.session_id) {
    lines.push(`Session: \`${result.session_id}\``)
  }
  lines.push(
    `\nCall \`avenor_events\` with \`run_id: "${run.runId}"\` to review the output, or \`avenor_follow_up\` to iterate.`,
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
      `Use \`avenor_follow_up\` with run_id "${run.runId}" to respond, or \`avenor_status\` to re-check.`,
    )
  }
  return lines.join('\n')
}

function summarizeEvent(evt: { type?: string; event?: string; tool?: string; name?: string; [key: string]: unknown }): string {
  const type = String(evt.type ?? evt.event ?? '')
  if (type === 'tool.call' || type === 'tool.use') {
    return `called ${String(evt.tool ?? evt.name ?? type)}`
  }
  if (type === 'agent.message_chunk') return 'thinking…'
  if (type.startsWith('permission.')) return `permission requested`
  return type
}

export default async function (pi: ExtensionAPI) {
  // ── State ────────────────────────────────────────────────────────────────

  const trackedRuns = new Map<string, TrackedRun>()
  let pollingActive = false
  // Captured context for polling callbacks (valid during session lifecycle)
  let sessionCtx: ExtensionContext | null = null

  // ── Helpers ──────────────────────────────────────────────────────────────

  async function pollRuns(): Promise<RunStatusEntry[]> {
    let raw: StatusResult | StatusResult[]
    try {
      raw = await statusTool({})
    } catch (err) {
      console.error('avenor pollRuns: statusTool failed', err)
      return []
    }

    const results = Array.isArray(raw) ? raw : [raw]
    const entries: RunStatusEntry[] = []

    for (const r of results) {
      const run = trackedRuns.get(r.run_id)
      if (run) {
        run.lastStatus = r
      }

      entries.push({
        runId: r.run_id,
        label: r.label,
        status: r.status,
        phase: r.phase,
        phaseLabel: r.phase_label,
        agent: trackedRuns.get(r.run_id)?.agent ?? 'unknown',
        pendingPermission: !!r.pending_permission,
        permissionDescription: r.pending_permission?.description,
      })
    }

    return entries
  }

  function renderStatusLines(entries: RunStatusEntry[]): string[] {
    const active = entries.filter(e => !TERMINAL_STATUSES.has(e.status))
    if (active.length === 0) return []

    return active.map(e => {
      const emoji = statusEmoji(e.status)
      const perm = e.pendingPermission ? ` 🔒` : ''
      const phase = e.phaseLabel ? ` [${e.phaseLabel}]` : ''
      return `${emoji} ${e.label} — ${e.status}${phase}${perm}`
    })
  }

  function updateWidget(entries: RunStatusEntry[]): void {
    if (!sessionCtx) return

    const lines = renderStatusLines(entries)
    if (lines.length > 0) {
      sessionCtx.ui.setWidget('avenor-status', lines)
      const parts = lines.map(l => l.slice(0, 60))
      sessionCtx.ui.setStatus('avenor', parts.join('  '))
    } else {
      sessionCtx.ui.setWidget('avenor-status', undefined)
      sessionCtx.ui.setStatus('avenor', '')
    }
  }

  function startPolling(): void {
    if (pollingActive) return
    pollingActive = true

    const tick = async () => {
      // Snapshot previous statuses before pollRuns mutates run.lastStatus
      const prevStatuses = new Map<string, string | undefined>()
      for (const [id, run] of trackedRuns) {
        prevStatuses.set(id, run.lastStatus?.status)
      }

      const entries = await pollRuns()
      updateWidget(entries)

      // Check for completed runs and notify
      for (const entry of entries) {
        const run = trackedRuns.get(entry.runId)
        if (!run) continue

        if (TERMINAL_STATUSES.has(entry.status)) {
          // Status just transitioned to terminal
          const prevStatus = prevStatuses.get(entry.runId)
          if (prevStatus && !TERMINAL_STATUSES.has(prevStatus)) {
            sessionCtx?.ui.notify(
              `Sub-agent "${entry.label}" finished: ${entry.status}`,
              entry.status === 'done' ? 'info' : 'warning',
            )
            // Inject completion notification for fire-and-forget runs so the
            // model can decide to follow up.
            if (!run.blocking) {
              try {
                pi.sendUserMessage(
                  buildCompletionText({ runId: run.runId, label: run.label }, run.lastStatus!),
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
          // Inject permission notification for fire-and-forget runs. The
          // control-plane claim may time out before the user notices the 🔒.
          run.permissionNotified = true
          try {
            pi.sendUserMessage(
              buildWaitingText({ runId: run.runId, label: run.label, agent: run.agent }, run.lastStatus!),
              { deliverAs: 'followUp' },
            )
          } catch (err) {
            console.error('avenor tick: sendUserMessage for permission failed', err)
          }
        } else if (!entry.pendingPermission && run.permissionNotified) {
          // Permission was resolved — allow a new notification if another
          // permission request arrives.
          run.permissionNotified = false
        }
      }

      // Stop polling if no active runs
      const hasActive = entries.some(e => !TERMINAL_STATUSES.has(e.status))
      if (hasActive) {
        setTimeout(tick, POLL_INTERVAL_MS)
      } else {
        pollingActive = false
        sessionCtx?.ui.setStatus('avenor', '')
        sessionCtx?.ui.setWidget('avenor-status', undefined)
      }
    }

    tick()
  }

  // ── Session lifecycle ────────────────────────────────────────────────────

  pi.on('session_start', async (_event, ctx) => {
    sessionCtx = ctx
  })

  pi.on('session_shutdown', async (_event, _ctx) => {
    pollingActive = false
    sessionCtx = null
  })

  // ── Context enrichment ───────────────────────────────────────────────────

  pi.on('before_agent_start', async (event, _ctx) => {
    if (trackedRuns.size === 0) return

    const entries = await pollRuns()
    const active = entries.filter(e => !TERMINAL_STATUSES.has(e.status))
    if (active.length === 0) return

    const lines = active.map(e => {
      const perm = e.pendingPermission ? ` [blocked: ${e.permissionDescription ?? 'permission'}]` : ''
      const phase = e.phaseLabel ? ` (${e.phaseLabel})` : ''
      return `  • ${e.label} (${e.agent}): ${e.status}${phase}${perm}`
    })

    return {
      message: {
        customType: 'avenor-active-runs',
        content: `Active sub-agents:\n${lines.join('\n')}`,
        display: true,
      },
    }
  })

  // ── Message renderer ─────────────────────────────────────────────────────

  pi.registerMessageRenderer('avenor-active-runs', (message, options, theme: Theme) => {
    const lines = (message.content as string).split('\n')
    let text = ''
    for (let i = 0; i < lines.length; i++) {
      if (i === 0) {
        text += theme.fg('info', theme.bold(lines[i]))
      } else {
        text += theme.fg('muted', lines[i])
      }
      if (i < lines.length - 1) text += '\n'
    }
    return new Text(text, 0, 0)
  })

  // ── Tools ────────────────────────────────────────────────────────────────

  pi.registerTool({
    name: 'avenor_spawn',
    label: 'Avenor Spawn',
    description:
      'Dispatch an agent run via avenor. Blocks by default, showing live progress. Set wait=false for fire-and-forget.',
    promptSnippet: 'Dispatch sub-agent runs via avenor (spawn, status, events, follow-up, shutdown)',
    promptGuidelines: [
      'Use avenor_spawn to delegate well-defined tasks to sub-agents.',
      'Use avenor_status to check progress of fire-and-forget runs.',
      'Use avenor_events to inspect detailed output from a run.',
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

      const result = await spawnTool({
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

      trackedRuns.set(result.run_id, {
        runId: result.run_id,
        agent: params.agent,
        label,
        supervisorId: result.supervisor_id,
        startTime: Date.now(),
        blocking: wait,
      })

      // Start polling for widget updates
      startPolling()

      if (!wait) {
        return {
          content: [{
            type: 'text',
            text: `Dispatched "${label}" (run_id: ${result.run_id}). Call avenor_status to check progress.`,
          }],
          details: { run_id: result.run_id, label },
        }
      }

      // Blocking mode: live progress updates
      onUpdate?.({
        content: [{ type: 'text', text: `Starting ${label}…` }],
        details: { status: 'running', run_id: result.run_id },
      })

      let firstPoll = true
      while (!signal?.aborted) {
        if (!firstPoll) await sleep(POLL_INTERVAL_MS)
        firstPoll = false

        let raw: StatusResult | StatusResult[]
        try {
          raw = await statusTool({
            runId: result.run_id,
            supervisorId: result.supervisor_id,
          })
        } catch (err) {
          console.error('avenor spawn poll: statusTool failed', err)
          continue
        }

        const status = Array.isArray(raw) ? raw[0] : raw
        if (!status) continue

        if (status.pending_permission) {
          onUpdate?.({
            content: [{ type: 'text', text: `[blocked] ${label} — permission: ${status.pending_permission.description}` }],
            details: { status: 'permission', run_id: result.run_id },
          })
        } else {
          let lastAction = ''
          if (!result.supervisor_id) {
            try {
              const eventsResult = await eventsTool({ runId: result.run_id, limit: 3 })
              const last = eventsResult.events[eventsResult.events.length - 1]
              if (last) lastAction = summarizeEvent(last)
            } catch (err) {
              console.error('avenor spawn poll: eventsTool failed', err)
            }
          }

          onUpdate?.({
            content: [{ type: 'text', text: `${label}${status.phase_label ? ` (${status.phase_label})` : ''}${lastAction ? ` — ${lastAction}` : ''}` }],
            details: { status: status.status, run_id: result.run_id, lastAction },
          })
        }

        // If the sub-agent is waiting on a question or permission, break out
        // of the blocking loop so the orchestrator gets its turn back.
        if (status.status === 'waiting') {
          const tracked = trackedRuns.get(result.run_id)
          if (tracked) {
            tracked.permissionNotified = true
          }
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

        if (TERMINAL_STATUSES.has(status.status)) {
          trackedRuns.delete(result.run_id)
          return {
            content: [{ type: 'text', text: buildCompletionText({ runId: result.run_id, label }, status) }],
            details: {
              status: status.status,
              run_id: result.run_id,
              session_id: status.session_id,
            },
          }
        }
      }

      trackedRuns.delete(result.run_id)
      return {
        content: [{ type: 'text', text: `Monitoring of "${label}" (run_id: ${result.run_id}) was interrupted. Use avenor_status to check.` }],
        details: { run_id: result.run_id },
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
        const emoji = statusEmoji(status)
        return new Text(theme.fg('info', `${emoji} ${result.content?.[0]?.text ?? 'Running…'}`), 0, 0)
      }

      const status = result.details?.status
      if (status && TERMINAL_STATUSES.has(status)) {
        const emoji = statusEmoji(status)
        const header = theme.fg(status === 'done' ? 'success' : 'error', `${emoji} ${status}`)
        const lines = (result.content?.[0]?.text ?? '').split('\n')
        const text = header + '\n' + lines.map(l => theme.fg('muted', `  ${l}`)).join('\n')
        return new Text(text, 0, 0)
      }

      const text = (result.content?.[0]?.text ?? '').split('\n').map(l => theme.fg('muted', l)).join('\n')
      return new Text(text, 0, 0)
    },
  })

  pi.registerTool({
    name: 'avenor_status',
    label: 'Avenor Status',
    description: 'Get status of a run or all runs. Surfaces pending permission requests.',
    parameters: Type.Object({
      run_id: Type.Optional(Type.String({ description: 'Specific run ID to query' })),
      supervisor_id: Type.Optional(Type.String({ description: 'Reuse an existing supervisor by socket path' })),
    }),
    async execute(_toolCallId, params) {
      const result = await statusTool({
        runId: params.run_id,
        supervisorId: params.supervisor_id,
      })
      return {
        content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        details: result,
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
      const result = await answerPermissionTool({
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
      const result = await followUpTool({
        runId: params.run_id,
        message: params.message,
        label: params.label,
        supervisorId: params.supervisor_id,
      })
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
      const result = await eventsTool({
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
      const result = await shutdownTool({
        supervisorId: params.supervisor_id,
        force: params.force,
      })
      return {
        content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        details: result,
      }
    },
  })

  // ── Commands ─────────────────────────────────────────────────────────────

  pi.registerCommand('avenor-status', {
    description: 'Show status of all avenor runs',
    handler: async (_args, ctx) => {
      const entries = await pollRuns()
      if (entries.length === 0) {
        ctx.ui.notify('No active avenor runs', 'info')
        return
      }
      const lines = entries.map(e => formatRunLine(e))
      ctx.ui.setWidget('avenor-status-detail', lines)
    },
  })

  pi.registerCommand('avenor-watch', {
    description: 'Open a live event feed for a run',
    getArgumentCompletions: (prefix: string) => {
      const items = Array.from(trackedRuns.values())
        .filter(r => r.label.startsWith(prefix) || r.runId.startsWith(prefix))
        .map(r => ({ value: r.runId, label: `${r.label} (${r.runId.slice(0, 8)})` }))
      return items.length > 0 ? items : null
    },
    handler: async (args, ctx) => {
      if (!ctx.hasUI) {
        ctx.ui.notify('avenor-watch requires TUI mode', 'warning')
        return
      }

      let runId = args?.trim()
      if (!runId) {
        const runList = Array.from(trackedRuns.values())
        if (runList.length === 0) {
          ctx.ui.notify('No tracked runs', 'warning')
          return
        }
        const choices = runList.map(r => r.runId)
        const labels = runList.map(r => `${r.label} (${r.runId.slice(0, 8)})`)
        const idx = await ctx.ui.select('Watch run:', labels)
        if (idx === null) return
        runId = choices[idx]
      }

      let watchActive = true
      const overlay = new EventFeedOverlay(runId)

      // Load initial events
      try {
        const { events } = await eventsTool({ runId, limit: 50 })
        overlay.setEvents(events)
      } catch (err) {
        console.error('avenor watch: failed to load initial events', err)
        overlay.setEvents([])
      }

      const handle = ctx.ui.custom(
        (_tui, _theme, _kb, done) => {
          overlay.setOnClose(() => {
            watchActive = false
            done()
          })
          return overlay
        },
        {
          overlay: true,
          overlayOptions: {
            anchor: 'right-center',
            width: '45%',
            minWidth: 60,
            maxHeight: '70%',
          },
          onHandle: (h) => {
            // Store for polling updates
            ;(handle as any)._inner = h
          },
        },
      )

      // Poll for new events
      const watchTick = async () => {
        while (watchActive) {
          await sleep(POLL_INTERVAL_MS)
          try {
            const { events } = await eventsTool({ runId, limit: 50 })
            overlay.setEvents(events)
            handle.requestRender?.()
          } catch (err) {
            console.error('avenor watch poll: eventsTool failed', err)
            watchActive = false
          }
        }
      }
      watchTick()
    },
  })

  pi.registerCommand('avenor-cancel', {
    description: 'Cancel a running avenor sub-agent',
    getArgumentCompletions: (prefix: string) => {
      const items = Array.from(trackedRuns.values())
        .filter(r => r.label.startsWith(prefix) || r.runId.startsWith(prefix))
        .map(r => ({ value: r.runId, label: `${r.label} (${r.runId.slice(0, 8)})` }))
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

      const ok = await ctx.ui.confirm('Cancel run', `Cancel "${label}" (${runId.slice(0, 8)})?`)
      if (!ok) return

      try {
        const { Supervisor } = await import('@dougbots/avenor-core')
        const sup = await Supervisor.get()
        const client = sup.getClient()
        await client.cancel(run?.lastStatus?.runtime_id ?? runId)
        ctx.ui.notify(`Cancelled "${label}"`, 'info')
      } catch (err) {
        ctx.ui.notify(`Failed to cancel: ${err instanceof Error ? err.message : String(err)}`, 'error')
      }
    },
  })
}
