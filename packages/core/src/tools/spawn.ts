import * as crypto from 'node:crypto'
import { Supervisor } from '../supervisor.js'
import { ensureRunPaths } from '../paths.js'
import { validateTimeout } from './validate.js'
import { getSupervisorClient } from './get-supervisor-client.js'

export async function spawnTool(args: {
  agent: string
  prompt?: string
  promptFile?: string
  label?: string
  dir?: string
  timeout?: string
  model?: string
  backend?: string
  serverUrl?: string
  sessionId?: string
  supervisorId?: string
  parent_run_id?: string
}): Promise<{ run_id: string; label: string; supervisor_id: string; runtime_id?: string; broker_url?: string; parent_token?: string }> {
  const runId = crypto.randomUUID()
  const label = args.label ?? runId

  if (args.supervisorId) {
    const { client, isSingleton, supervisorId } = await getSupervisorClient(args.supervisorId)
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
        backend: args.backend,
        server_url: args.serverUrl,
        session_id: args.sessionId,
        sentinel_file: sentinelPath,
        on_event: eventLogPath,
        parent_run_id: args.parent_run_id,
      })

      return {
        run_id: (result.runtime_id as string | undefined) ?? runId,
        label,
        supervisor_id: supervisorId,
        runtime_id: result.runtime_id as string | undefined,
        broker_url: result.broker_url as string | undefined,
        parent_token: result.parent_token as string | undefined,
      }
    } finally {
      if (!isSingleton) {
        client.close()
      }
    }
  }

  const sup = await Supervisor.get()
  const runInfo = await sup.spawn({
    agent: args.agent,
    prompt: args.prompt,
    prompt_file: args.promptFile,
    label,
    dir: args.dir,
    timeout: args.timeout !== undefined ? validateTimeout(args.timeout) : undefined,
    model: args.model,
    backend: args.backend,
    server_url: args.serverUrl,
    session_id: args.sessionId,
    parent_run_id: args.parent_run_id,
  })

  return {
    run_id: runId,
    label,
    supervisor_id: sup.supervisorId,
    runtime_id: runInfo.runtimeId,
    broker_url: runInfo.brokerUrl,
    parent_token: runInfo.parentToken,
  }
}
