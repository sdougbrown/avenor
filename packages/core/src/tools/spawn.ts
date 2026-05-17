import * as crypto from 'node:crypto'
import * as path from 'node:path'
import * as fs from 'node:fs'
import * as os from 'node:os'
import { Supervisor } from '../supervisor.js'
import { dial } from '../client.js'
import { validateSupervisorSocketPath, validateTimeout } from './validate.js'

function runsRoot(): string {
  return path.join(os.homedir(), '.avenor', 'runs')
}

function ensureRunDir(runId: string): { sentinelPath: string; eventLogPath: string } {
  const runDir = path.join(runsRoot(), runId)
  fs.mkdirSync(runDir, { recursive: true, mode: 0o700 })
  return {
    sentinelPath: path.join(runDir, 'sentinel.done'),
    eventLogPath: path.join(runDir, 'events.log'),
  }
}

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
    const { sentinelPath, eventLogPath } = ensureRunDir(runId)

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
