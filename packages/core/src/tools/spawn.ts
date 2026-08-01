import * as crypto from 'node:crypto'
import { Supervisor } from '../supervisor.js'
import type { SpawnParams, ThinkingLevel } from '../client.js'
import { ensureRunPaths } from '../paths.js'
import { validateTimeout } from './validate.js'
import { validateSpawnSelection } from '../spawn-selection.js'
import { getSupervisorClient } from './get-supervisor-client.js'

export interface SpawnToolArgs {
  agent?: string
  prompt?: string
  promptFile?: string
  label?: string
  dir?: string
  timeout?: string
  model?: string
  thinking?: ThinkingLevel
  agentProfile?: string
  backend?: string
  rosterFile?: string
  rosterEntry?: string
  serverUrl?: string
  sessionId?: string
  supervisorId?: string
  parent_run_id?: string
}

export interface SpawnToolResult {
  run_id: string
  label: string
  supervisor_id: string
  runtime_id?: string
  broker_url?: string
  parent_token?: string
}

export async function spawnTool(args: SpawnToolArgs): Promise<SpawnToolResult> {
  validateSpawnSelection({
    agent: args.agent,
    model: args.model,
    backend: args.backend,
    roster_file: args.rosterFile,
    roster_entry: args.rosterEntry,
  })

  const runId = crypto.randomUUID()
  const label = args.label ?? runId
  const baseParams: SpawnParams = {
    prompt: args.prompt,
    prompt_file: args.promptFile,
    label,
    dir: args.dir,
    timeout:
      args.timeout !== undefined ? validateTimeout(args.timeout) : undefined,
    model: args.model,
    agent_profile: args.agentProfile,
    backend: args.backend,
    server_url: args.serverUrl,
    session_id: args.sessionId,
    parent_run_id: args.parent_run_id,
  }
  if (args.rosterFile) {
    baseParams.roster_file = args.rosterFile
  }
  if (args.rosterEntry) {
    baseParams.roster_entry = args.rosterEntry
  }
  if (args.agent) {
    baseParams.agent = args.agent
  }
  if (args.thinking !== undefined) {
    baseParams.thinking = args.thinking
  }

  if (args.supervisorId) {
    const { client, isSingleton, supervisorId } = await getSupervisorClient(args.supervisorId)
    const { sentinelPath, eventLogPath } = ensureRunPaths(runId)

    try {
      const result = await client.spawn({
        ...baseParams,
        sentinel_file: sentinelPath,
        on_event: eventLogPath,
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
  const runInfo = await sup.spawn(baseParams, runId)

  return {
    run_id: runInfo.runId,
    label: runInfo.label,
    supervisor_id: sup.supervisorId,
    runtime_id: runInfo.runtimeId,
    broker_url: runInfo.brokerUrl,
    parent_token: runInfo.parentToken,
  }
}
