import * as crypto from 'node:crypto'
import { Supervisor } from '../supervisor.js'
import { dial } from '../client.js'
import { ensureRunPaths } from '../paths.js'
import { validateSupervisorSocketPath, validateTimeout } from './validate.js'

export async function spawnTool(args: {
  agent: string
  prompt?: string
  promptFile?: string
  label?: string
  dir?: string
  timeout?: string
  model?: string
  sessionId?: string
  supervisorId?: string
}): Promise<{ run_id: string; label: string; supervisor_id: string }> {
  const runId = crypto.randomUUID()
  const label = args.label ?? runId

  if (args.supervisorId) {
    const client = await dial(validateSupervisorSocketPath(args.supervisorId))
    const { sentinelPath, eventLogPath } = ensureRunPaths(runId)

    try {
      const result = await client.spawn({
        agent: args.agent,
        prompt: args.prompt,
        prompt_file: args.promptFile,
        label,
        dir: args.dir,
        timeout:
          args.timeout !== undefined ? validateTimeout(args.timeout) : undefined,
        model: args.model,
        session_id: args.sessionId,
        sentinel_file: sentinelPath,
        on_event: eventLogPath,
      })

      return {
        run_id: (result.runtime_id as string | undefined) ?? runId,
        label,
        supervisor_id: args.supervisorId,
      }
    } finally {
      client.close()
    }
  }

  const sup = await Supervisor.get()
  await sup.spawn({
    agent: args.agent,
    prompt: args.prompt,
    prompt_file: args.promptFile,
    label,
    dir: args.dir,
    timeout: args.timeout !== undefined ? validateTimeout(args.timeout) : undefined,
    model: args.model,
    session_id: args.sessionId,
  })

  return { run_id: runId, label, supervisor_id: sup.supervisorId }
}
