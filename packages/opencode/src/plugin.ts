import type { Plugin, ToolContext } from '@opencode-ai/plugin'
import { tool } from '@opencode-ai/plugin'
import { z } from 'zod'
import {
  spawnTool, statusTool, answerPermissionTool,
  followUpTool, eventsTool, shutdownTool,
} from '@dougbots/avenor-core'

export const AvenorPlugin: Plugin = async (_ctx) => ({
  tool: {
    avenor_spawn: tool({
      description: 'Dispatch an agent run via avenor. Returns run_id for status polling.',
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
      },
      async execute(args, context: ToolContext) {
        return spawnTool({
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
        }) as any
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
      description: 'Answer a pending permission request. Pass option_id from avenor_status pending_permission.options.',
      args: {
        run_id: z.string().describe('Run ID with the pending permission'),
        option_id: z.string().describe('Option ID from pending_permission.options array'),
        request_id: z.string().optional().describe('Request ID (auto-discovered if omitted)'),
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
})
