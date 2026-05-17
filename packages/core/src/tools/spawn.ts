import * as crypto from 'node:crypto'
import * as os from 'node:os'
import * as path from 'node:path'
import { Supervisor } from '../supervisor.js'
import { dial } from '../client.js'

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
    const client = await dial(args.supervisorId)
    const sentinelPath = path.join(os.tmpdir(), `avenor-run-${runId}.done`)
    const eventLogPath = path.join(os.tmpdir(), `avenor-run-${runId}.log`)

    await client.spawn({
      agent: args.agent,
      prompt: args.prompt,
      promptFile: args.promptFile,
      label,
      dir: args.dir,
      timeout: args.timeout,
      model: args.model,
      sessionId: args.sessionId,
      sentinel_file: sentinelPath,
      on_event: eventLogPath,
    })

    return { run_id: runId, label, supervisor_id: args.supervisorId }
  }

  const sup = await Supervisor.get()
  await sup.spawn({
    agent: args.agent,
    prompt: args.prompt,
    promptFile: args.promptFile,
    label,
    dir: args.dir,
    timeout: args.timeout,
    model: args.model,
    sessionId: args.sessionId,
  })

  return { run_id: runId, label, supervisor_id: sup.supervisorId }
}
