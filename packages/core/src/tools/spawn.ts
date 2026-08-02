import * as crypto from 'node:crypto'
import { Supervisor } from '../supervisor.js'
import type { SpawnParams, ThinkingLevel } from '../client.js'
import { ensureRunPaths } from '../paths.js'
import { validateTimeout } from './validate.js'
import { validateSpawnSelection } from '../spawn-selection.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { registerExternalRun } from './run-registry.js'

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

async function executeSpawnTool(
  args: SpawnToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<SpawnToolResult> {
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
    const { client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId)

    if (isSingleton && sup) {
      const runInfo = await sup.spawn(baseParams, runId)
      return {
        run_id: runInfo.runId,
        label: runInfo.label,
        supervisor_id: supervisorId,
        runtime_id: runInfo.runtimeId,
        broker_url: runInfo.brokerUrl,
        parent_token: runInfo.parentToken,
      }
    }

    const { sentinelPath, eventLogPath } = ensureRunPaths(runId)

    try {
      const result = await client.spawn({
        ...baseParams,
        sentinel_file: sentinelPath,
        on_event: eventLogPath,
      })
      const stringValue = (candidate: unknown): string | undefined =>
        typeof candidate === 'string' && candidate.length > 0 ? candidate : undefined
      const runtimeId = stringValue(result.runtime_id)

      registerExternalRun({
        runId,
        label,
        supervisorId,
        sentinelPath,
        eventLogPath,
        runtimeId,
        sessionId: stringValue(result.session_id),
        agent: stringValue(result.effective_agent) ?? stringValue(result.agent) ?? baseParams.agent,
        model: stringValue(result.effective_model) ?? stringValue(result.model) ?? baseParams.model,
        backend: stringValue(result.effective_backend) ?? stringValue(result.backend) ?? baseParams.backend,
        effectiveAgent: stringValue(result.effective_agent) ?? stringValue(result.agent) ?? baseParams.agent,
        effectiveModel: stringValue(result.effective_model) ?? stringValue(result.model) ?? baseParams.model,
        effectiveBackend: stringValue(result.effective_backend) ?? stringValue(result.backend) ?? baseParams.backend,
        agentProfile: stringValue(result.agent_profile) ?? baseParams.agent_profile,
        rosterFile: stringValue(result.roster_file) ?? baseParams.roster_file,
        rosterEntry: stringValue(result.roster_entry) ?? baseParams.roster_entry,
        thinking: baseParams.thinking,
        dir: baseParams.dir,
        brokerUrl: stringValue(result.broker_url),
        parentToken: stringValue(result.parent_token),
      })

      return {
        run_id: runId,
        label,
        supervisor_id: supervisorId,
        runtime_id: runtimeId,
        broker_url: stringValue(result.broker_url),
        parent_token: stringValue(result.parent_token),
      }
    } finally {
      if (!isSingleton) client.close()
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

export function createSpawnTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: SpawnToolArgs) => Promise<SpawnToolResult> {
  return args => executeSpawnTool(args, getSupervisorClient)
}

export const spawnTool = createSpawnTool(realGetSupervisorClient)
