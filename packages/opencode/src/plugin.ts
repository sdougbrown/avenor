import type { Plugin, ToolContext } from '@opencode-ai/plugin'
import { tool } from '@opencode-ai/plugin'
import { z } from 'zod'
import {
  spawnTool, statusTool, eventsTool, answerPermissionTool,
  followUpTool, shutdownTool,
  type StatusResult,
} from '@dougbots/avenor-core'

type TrackedRun = {
  runId: string
  orchestratorSessionId: string
  label: string
  supervisorId?: string
  // true = a monitor is already running (either the blocking loop or monitorRun)
  monitoring: boolean
}

const TERMINAL_STATUSES = new Set(['done', 'failed', 'timeout', 'killed'])
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

function summarizeEvent(evt: { type?: unknown; event?: unknown; tool?: unknown; name?: unknown }): string {
  const type = String(evt.type ?? evt.event ?? '')
  if (type === 'tool.call' || type === 'tool.use') {
    return `called ${String(evt.tool ?? evt.name ?? type)}`
  }
  if (type === 'agent.message_chunk') return 'thinking…'
  if (type.startsWith('permission.')) return `permission requested`
  return type
}

export const AvenorPlugin: Plugin = async (ctx) => {
  // Primary state: runId → run info
  const trackedRuns = new Map<string, TrackedRun>()
  // Reverse index: opencode sessionId → avenor runId (for permission routing)
  const sessionIdToRunId = new Map<string, string>()

  function registerSessionId(sessionId: string | undefined, runId: string): void {
    if (sessionId && !sessionIdToRunId.has(sessionId)) {
      sessionIdToRunId.set(sessionId, runId)
    }
  }

  // Used by both fire-and-forget (session.idle trigger) and permision routing
  // to re-prompt the orchestrator when a run completes.
  async function monitorRun(run: TrackedRun): Promise<void> {
    while (true) {
      await sleep(POLL_INTERVAL_MS)

      let raw: StatusResult | StatusResult[]
      try {
        raw = await statusTool({ runId: run.runId, supervisorId: run.supervisorId })
      } catch {
        continue
      }

      const result = Array.isArray(raw) ? raw[0] : raw
      if (!result) continue

      registerSessionId(result.session_id, run.runId)

      if (TERMINAL_STATUSES.has(result.status)) {
        trackedRuns.delete(run.runId)
        await ctx.client.session.promptAsync({
          path: { id: run.orchestratorSessionId },
          body: { parts: [{ type: 'text', text: buildCompletionText(run, result) }] },
        }).catch(() => {
          // Session may have been deleted; nothing to do.
        })
        return
      }
    }
  }

  return {
    // ── Lifecycle ────────────────────────────────────────────────────────────

    event: async ({ event }) => {
      if (event.type !== 'session.idle') return
      const { sessionID } = (
        event as { type: 'session.idle'; properties: { sessionID: string } }
      ).properties

      for (const run of trackedRuns.values()) {
        if (run.orchestratorSessionId === sessionID && !run.monitoring) {
          run.monitoring = true
          monitorRun(run).catch(console.error)
        }
      }
    },

    // ── Permission routing ────────────────────────────────────────────────────
    // When a tracked sub-agent needs a permission, inject a re-prompt into the
    // orchestrating session so the LLM can answer via avenor_answer_permission.
    // output.status is left as "ask" so the normal dialog shows as a fallback.

    "permission.ask": async (permission, _output) => {
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
        ``,
        `To answer: call \`avenor_answer_permission\` with:`,
        `  run_id: "${runId}"`,
        `  request_id: "${permission.id}"`,
        `  option_id: "allow_once" | "allow_always" | "deny"`,
      ]

      await ctx.client.session.promptAsync({
        path: { id: run.orchestratorSessionId },
        body: { parts: [{ type: 'text', text: lines.join('\n') }] },
      }).catch(() => {})
    },

    // ── Tools ─────────────────────────────────────────────────────────────────

    tool: {
      avenor_spawn: tool({
        description:
          'Dispatch an agent run via avenor. Blocks by default, showing live progress as an updating tool call. Set wait=false for fire-and-forget — you will be re-prompted automatically on completion.',
        args: {
          agent: z.string().describe('Agent name (required, no default)'),
          prompt: z.string().optional().describe('Prompt text'),
          prompt_file: z.string().optional().describe('Path to prompt file'),
          label: z.string().optional().describe('Human-readable label for the run'),
          timeout: z.string().optional().describe('Timeout duration (e.g. 3600s)'),
          model: z.string().optional().describe('Model override'),
          backend: z.string().optional().describe('Backend override'),
          server_url: z.string().optional().describe('Backend server URL'),
          supervisor_id: z.string().optional().describe('Reuse an existing supervisor by socket path'),
          wait: z.boolean().default(true).describe(
            'Block until complete with live status updates. False = fire-and-forget.',
          ),
        },
        async execute(args, context: ToolContext) {
          const result = await spawnTool({
            agent: args.agent,
            prompt: args.prompt,
            promptFile: args.prompt_file,
            label: args.label,
            dir: context.directory,
            timeout: args.timeout,
            model: args.model,
            backend: args.backend,
            serverUrl: args.server_url,
            supervisorId: args.supervisor_id,
          })

          const supervisorId = result.supervisor_id || args.supervisor_id

          trackedRuns.set(result.run_id, {
            runId: result.run_id,
            orchestratorSessionId: context.sessionID,
            label: result.label,
            supervisorId,
            // Blocking mode is the monitor — prevent session.idle from starting a duplicate.
            monitoring: args.wait,
          })

          if (!args.wait) {
            return result as any
          }

          // ── Blocking mode: live updates via context.metadata ──────────────

          context.metadata({ title: `Starting ${result.label}…` })

          let firstPoll = true
          while (!context.abort.aborted) {
            // Poll immediately on first iteration so we register the session_id
            // before the first permission request can arrive.
            if (!firstPoll) await sleep(POLL_INTERVAL_MS)
            firstPoll = false

            if (context.abort.aborted) break

            let raw: StatusResult | StatusResult[]
            try {
              raw = await statusTool({ runId: result.run_id, supervisorId })
            } catch {
              continue
            }

            const status = Array.isArray(raw) ? raw[0] : raw
            if (!status) continue

            registerSessionId(status.session_id, result.run_id)

            if (status.pending_permission) {
              context.metadata({
                title: `[blocked] ${result.label}`,
                metadata: {
                  status: 'permission',
                  permission: status.pending_permission.description,
                },
              })
            } else {
              let lastAction = ''
              // eventsTool requires the singleton supervisor — skip when using an explicit one.
              if (!supervisorId) {
                try {
                  const { events } = await eventsTool({ runId: result.run_id, limit: 3 })
                  const last = events[events.length - 1]
                  if (last) lastAction = summarizeEvent(last)
                } catch {}
              }
              context.metadata({
                title: result.label,
                metadata: {
                  status: status.status,
                  ...(status.phase_label && { phase: status.phase_label }),
                  ...(lastAction && { last: lastAction }),
                },
              })
            }

            if (TERMINAL_STATUSES.has(status.status)) {
              trackedRuns.delete(result.run_id)
              return {
                title: `${result.label} — ${status.status}`,
                output: buildCompletionText({ runId: result.run_id, label: result.label }, status),
                metadata: {
                  status: status.status,
                  run_id: result.run_id,
                  session_id: status.session_id,
                },
              }
            }
          }

          // Aborted — clean up but don't cancel the underlying run.
          trackedRuns.delete(result.run_id)
          return {
            title: `${result.label} — aborted`,
            output: `Monitoring of "${result.label}" (run_id: ${result.run_id}) was interrupted. The run may still be active — use avenor_status to check.`,
            metadata: { run_id: result.run_id },
          }
        },
      }),

      avenor_status: tool({
        description: 'Get status of a run or all runs. Surfaces pending permission requests.',
        args: {
          run_id: z.string().optional().describe('Specific run ID to query'),
          supervisor_id: z.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, _context) {
          return statusTool({
            runId: args.run_id,
            supervisorId: args.supervisor_id,
          }) as any
        },
      }),

      avenor_answer_permission: tool({
        description:
          'Answer a pending permission request for a sub-agent. Pass option_id from avenor_status pending_permission.options, or use "allow_once", "allow_always", "deny".',
        args: {
          run_id: z.string().describe('Run ID with the pending permission'),
          option_id: z.string().describe('allow_once | allow_always | deny, or option_id from pending_permission.options'),
          request_id: z.string().optional().describe('Permission request ID (auto-discovered if omitted)'),
          supervisor_id: z.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, _context) {
          return answerPermissionTool({
            runId: args.run_id,
            optionId: args.option_id,
            requestId: args.request_id,
            supervisorId: args.supervisor_id,
          }) as any
        },
      }),

      avenor_follow_up: tool({
        description: 'Resume a completed run with a follow-up message.',
        args: {
          run_id: z.string().describe('Completed run ID to resume'),
          message: z.string().describe('Follow-up message text'),
          label: z.string().optional().describe('Override label (defaults to <original>-followup)'),
          supervisor_id: z.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, _context) {
          return followUpTool({
            runId: args.run_id,
            message: args.message,
            label: args.label,
            supervisorId: args.supervisor_id,
          }) as any
        },
      }),

      avenor_events: tool({
        description: 'Read events from a run. Filter by type. Returns last N events.',
        args: {
          run_id: z.string().describe('Run ID to read events from'),
          types: z.array(z.string()).optional().describe('Filter by event types'),
          limit: z.number().optional().describe('Max events to return (default 50)'),
          supervisor_id: z.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, _context) {
          return eventsTool({
            runId: args.run_id,
            types: args.types,
            limit: args.limit,
            supervisorId: args.supervisor_id,
          }) as any
        },
      }),

      avenor_shutdown: tool({
        description: 'Shut down the avenor supervisor and clean up temp files.',
        args: {
          supervisor_id: z.string().optional().describe('Supervisor to shut down (defaults to singleton)'),
          force: z.boolean().optional().describe('Force shutdown instead of graceful'),
        },
        async execute(args, _context) {
          return shutdownTool({
            supervisorId: args.supervisor_id,
            force: args.force,
          }) as any
        },
      }),
    },
  }
}
