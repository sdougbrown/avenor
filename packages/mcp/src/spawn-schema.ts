import { z } from 'zod'

import { THINKING_LEVELS } from '@dougbots/avenor-core'

export const spawnInputShape = {
  agent: z.string()
    .optional()
    .describe('Optional agent name; omission uses the supplied model or runtime defaults'),
  repo_dir: z.string().describe('Working directory for the agent'),
  prompt: z.string().optional().describe('Prompt text'),
  prompt_file: z.string().optional().describe('Path to a prompt file'),
  label: z.string().optional().describe('Human-readable label for this run'),
  timeout: z.string().optional().describe('Timeout duration (e.g. 5m, 1h)'),
  model: z.string().optional().describe('Model override'),
  thinking: z.enum(THINKING_LEVELS)
    .optional()
    .describe('Thinking level; omission uses the backend default and unsupported backends reject explicit values'),
  backend: z.string().optional().describe('Backend override'),
  roster_file: z.string().optional().describe('Path to the roster map'),
  roster_entry: z.string().optional().describe('Roster entry to select'),
  server_url: z.string().optional().describe('Backend server URL'),
  supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
}
