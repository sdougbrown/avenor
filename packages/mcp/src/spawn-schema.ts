import { z } from 'zod'

import { THINKING_LEVELS } from '@dougbots/avenor-core'

export const spawnInputShape = {
  agent: z.string()
    .optional()
    .describe('Optional agent persona; the backend resolves this name to a prompt, tool set, and permission policy. Independent of model and backend: supply any of the three, or none. Rejected when roster_entry is set, because the entry names all three'),
  repo_dir: z.string().describe('Working directory for the agent'),
  prompt: z.string().optional().describe('Prompt text'),
  prompt_file: z.string().optional().describe('Path to a prompt file'),
  label: z.string().optional().describe('Human-readable label for this run'),
  timeout: z.string().optional().describe('Timeout duration (e.g. 5m, 1h)'),
  model: z.string().optional().describe('Optional model override; independent of agent and backend, so a model can be named alone. Combined with agent, it runs that persona on this model. Rejected when roster_entry is set; to run a roster persona on a different model, send the identity directly'),
  thinking: z.enum(THINKING_LEVELS)
    .optional()
    .describe('Thinking level; omission uses the backend default and unsupported backends reject explicit values'),
  backend: z.string().optional().describe('Optional runtime backend, such as agy, pi, opencode-acp, codex-app-server, claude, or poo. Independent of agent and model; naming one avoids relying on a runtime default. Rejected when roster_entry is set'),
  roster_file: z.string().optional().describe('Optional path to a roster map; pair it with roster_entry to select a whole loadout by key. Setting roster_entry turns on roster mode, which rejects agent, model, and backend'),
  roster_entry: z.string().optional().describe('Optional roster entry key; the selected entry supplies backend, agent, and model as one identity, and its optional thinking is only a default. Requires roster_file. Passing agent, model, or backend alongside it is an error; to change one part of a roster persona, send the identity directly instead'),
  server_url: z.string().optional().describe('Backend server URL'),
  supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
}
