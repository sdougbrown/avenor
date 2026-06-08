import type { Plugin, ToolContext } from '@opencode-ai/plugin'
import { tool } from '@opencode-ai/plugin'

import * as crypto from 'node:crypto'
import {
  spawnTool, statusTool, eventsTool, answerPermissionTool,
  followUpTool, shutdownTool,
  type StatusResult,
} from '@dougbots/avenor-core'

type TrackedRun = {
  runId: string
  agent: string
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

// Channel block pattern: <channel source="agent-reviewer" from_run_id="abc123" from_role="reviewer">content</channel>
const CHANNEL_RE = /<channel\s+source="([^"]*)"(?:\s+from_run_id="([^"]*)")?(?:\s+from_role="([^"]*)")?[^>]*>([\s\S]*?)<\/channel>/g

function formatAgentMessage(source: string, fromRunId: string, fromRole: string, content: string): string {
  // Strip redundant "agent-" prefix that both the Go sidecar (ChannelWrap) and
  // the plugin's pollChannelMessages may prepend, so "agent-agent" becomes "agent".
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
  // Primary state: runId → run info
  const trackedRuns = new Map<string, TrackedRun>()
  // Reverse index: opencode sessionId → avenor runId (for permission routing)
  const sessionIdToRunId = new Map<string, string>()
  // Reverse index: avenor runtimeId → avenor runId (for channel message attribution)
  const runtimeIdToRunId = new Map<string, string>()
  // Channel-messaging state for receiving messages from spawned children.
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

  function registerSessionId(sessionId: string | undefined, runtimeId: string | undefined, runId: string): void {
    if (sessionId && !sessionIdToRunId.has(sessionId)) {
      sessionIdToRunId.set(sessionId, runId)
    }
    if (runtimeId && !runtimeIdToRunId.has(runtimeId)) {
      runtimeIdToRunId.set(runtimeId, runId)
    }
  }

  // Used by both fire-and-forget (session.idle trigger) and permision routing
  // to re-prompt the orchestrator when a run completes.
  async function monitorRun(run: TrackedRun): Promise<void> {
    let firstPoll = true
    while (true) {
      if (!firstPoll) await sleep(POLL_INTERVAL_MS)
      firstPoll = false

      let raw: StatusResult | StatusResult[]
      try {
        raw = await statusTool({ runId: run.runId, supervisorId: run.supervisorId })
      } catch {
        continue
      }

      const result = Array.isArray(raw) ? raw[0] : raw
      if (!result) continue

      registerSessionId(result.session_id, result.runtime_id, run.runId)

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

  // ── Channel message polling ─────────────────────────────────────────────

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
            // Resolve the agent name from any known ID: run_id, session_id, or runtime_id.
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
             // Session likely gone — stop polling.
             return
           }
         }
       }
     } finally {
       channelPolling = false
     }
   }
  return {
    // ── Lifecycle ────────────────────────────────────────────────────────────

    event: async ({ event }) => {
      if (event.type !== 'session.idle') return
      const { sessionID } = (
        event as { type: 'session.idle'; properties: { sessionID: string } }
      ).properties

      pollChannelMessages(sessionID).catch(console.error)

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

    // ── Channel rendering ────────────────────────────────────────────────────
    // When a channel message arrives at the top-level orchestrator (injected as
    // a raw <channel> XML prompt), format it for human readability. The model
    // still receives the full XML with the untrusted-content instruction; this
    // only prettifies the display.

    "chat.message": async (message: any, _output: any) => {
      if (!message?.parts) return
      for (const part of message.parts) {
        if (part?.type === 'text' && typeof part?.text === 'string' && part.text.includes('<channel ')) {
          part.text = formatChannelBlocks(part.text)
        }
      }
    },

    "experimental.text.complete": async (
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

    // ── Tools ─────────────────────────────────────────────────────────────────

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
          const parentRunId = ensureParentRunId()
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
            parent_run_id: parentRunId,
          })

           // Capture broker info for channel messaging. Update on every spawn
           // so a different supervisor can take over if needed.
           if (result.broker_url) {
             brokerUrl = result.broker_url
           }
           if (result.parent_token) {
             parentToken = result.parent_token
           }

          const supervisorId = result.supervisor_id || args.supervisor_id

          trackedRuns.set(result.run_id, {
            runId: result.run_id,
            agent: args.agent,
            orchestratorSessionId: context.sessionID,
            label: result.label,
            supervisorId,
            // Blocking mode is the monitor — prevent session.idle from starting a duplicate.
            monitoring: args.wait,
          })

          // Register runtime_id immediately so channel messages arriving
          // before the first status poll can still resolve the agent name.
          if (result.runtime_id) {
            runtimeIdToRunId.set(result.runtime_id, result.run_id)
          }

          if (!args.wait) {
            return {
              title: `${result.label} — dispatched`,
              output: `Dispatched "${result.label}" (run_id: ${result.run_id}). Call avenor_status with run_id to check progress, or wait for the completion notification.`,
            }
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

            registerSessionId(status.session_id, status.runtime_id, result.run_id)

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
          run_id: tool.schema.string().optional().describe('Specific run ID to query'),
          supervisor_id: tool.schema.string().optional().describe('Reuse an existing supervisor by socket path'),
        },
        async execute(args, _context) {
          const result = await statusTool({
            runId: args.run_id,
            supervisorId: args.supervisor_id,
          })
          return JSON.stringify(result, null, 2)
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
        async execute(args, _context) {
          const result = await followUpTool({
            runId: args.run_id,
            message: args.message,
            label: args.label,
            supervisorId: args.supervisor_id,
          })
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

      avenor_shutdown: tool({
        description: 'Shut down the avenor supervisor and clean up temp files.',
        args: {
          supervisor_id: tool.schema.string().optional().describe('Supervisor to shut down (defaults to singleton)'),
          force: tool.schema.boolean().optional().describe('Force shutdown instead of graceful'),
        },
        async execute(args, _context) {
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
