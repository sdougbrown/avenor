import { z } from 'zod'

import { THINKING_LEVELS, spawnFieldDescription, spawnFieldEnum } from '@dougbots/avenor-core'

// Field text comes from schemas/spawn.input.json through @dougbots/avenor-core.
// Only the shapes, and which fields this surface publishes, are written here.
export const spawnInputShape = {
  agent: z.string().optional().describe(spawnFieldDescription('agent')),
  dir: z.string().describe(spawnFieldDescription('dir')),
  prompt: z.string().optional().describe(spawnFieldDescription('prompt')),
  prompt_file: z.string().optional().describe(spawnFieldDescription('prompt_file')),
  label: z.string().optional().describe(spawnFieldDescription('label')),
  timeout: z.string().optional().describe(spawnFieldDescription('timeout')),
  model: z.string().optional().describe(spawnFieldDescription('model')),
  thinking: z.enum(THINKING_LEVELS).optional().describe(spawnFieldDescription('thinking')),
  backend: z.enum(spawnFieldEnum('backend') as [string, ...string[]]).optional().describe(spawnFieldDescription('backend')),
  roster_file: z.string().optional().describe(spawnFieldDescription('roster_file')),
  roster_entry: z.string().optional().describe(spawnFieldDescription('roster_entry')),
  server_url: z.string().optional().describe(spawnFieldDescription('server_url')),
  supervisor_id: z.string().optional().describe(spawnFieldDescription('supervisor_id')),
}
