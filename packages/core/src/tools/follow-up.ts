import * as fs from 'node:fs'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import { Supervisor, retainLiveIdentity, type RunInfo } from '../supervisor.js'
import type { SpawnParams, ThinkingLevel } from '../client.js'
import { ensureRunPaths, runsRoot } from '../paths.js'
import { validateRunId } from './validate.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { findExternalRun, registerExternalRun } from './run-registry.js'
import { findLocalRunByReference } from './run-resolution.js'

export interface FollowUpToolArgs {
  runId: string
  message: string
  label?: string
  supervisorId?: string
}

export interface FollowUpToolResult {
  run_id: string
  label: string
  runtime_id?: string
}

function resolveAutoApprove(
  liveStatus: Record<string, unknown> | null,
  runInfo?: RunInfo,
): boolean | undefined {
  const liveAutoApprove = liveStatus?.auto_approve
  if (typeof liveAutoApprove === 'boolean') return liveAutoApprove
  return typeof runInfo?.autoApprove === 'boolean' ? runInfo.autoApprove : undefined
}

function value(candidate: unknown): string | undefined {
  return typeof candidate === 'string' && candidate.length > 0 ? candidate : undefined
}

function resolvedIdentity(
  liveStatus: Record<string, unknown> | null,
  effectiveKey: string,
  directKey: string,
  fallbackEffective: string | undefined,
  fallbackDirect: string | undefined,
): string | undefined {
  // A live empty string is authoritative too: agent-only/model-only/defaulted
  // sessions must clear stale run-level fallback fields on follow-up.
  if (liveStatus && typeof liveStatus[effectiveKey] === 'string') {
    return value(liveStatus[effectiveKey])
  }
  if (liveStatus && typeof liveStatus[directKey] === 'string') {
    return value(liveStatus[directKey])
  }
  return fallbackEffective ?? fallbackDirect
}

async function parseSentinel(filePath: string): Promise<Record<string, string> | null> {
  try {
    const content = (await fs.promises.readFile(filePath, 'utf-8')).trim()
    if (!content) return null
    const lines = content.split('\n')
    const result: Record<string, string> = {}
    result._status = lines[0]
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim()
      if (!line) continue
      const eq = line.indexOf('=')
      if (eq > 0) {
        result[line.substring(0, eq)] = line.substring(eq + 1)
      }
    }
    return result
  } catch {
    return null
  }
}

function rejectSessionConflict(
  sentinel: Record<string, string> | null,
  liveStatus: Record<string, unknown> | null,
): void {
  const sentinelStopReason = value(sentinel?.STOP_REASON)
  const liveStopReason = value(liveStatus?.stop_reason)
  if (sentinelStopReason === 'session_id_conflict' || liveStopReason === 'session_id_conflict') {
    throw new Error('run is not resumable: session_id_conflict')
  }
}

async function executeFollowUpTool(
  args: FollowUpToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<FollowUpToolResult> {
  if (args.supervisorId) {
    const { client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId)
    try {
      validateRunId(args.runId)

      // Try to resolve the run via the local supervisor's run map first
      let runInfo: RunInfo | undefined
      if (isSingleton && sup) {
        runInfo = findLocalRunByReference(sup, args.runId)
      } else {
        runInfo = findExternalRun(supervisorId, args.runId)
      }
      const fallbackSessionId = runInfo?.sessionId

      let liveStatus: Record<string, unknown> | null = null
      try {
        liveStatus = await client.status(runInfo?.runtimeId ?? args.runId)
      } catch {
        // ignore
      }

      const sentinelPath =
        (liveStatus?.sentinel_file as string | undefined) ??
        runInfo?.sentinelPath ??
        path.join(runsRoot(), args.runId, 'sentinel.done')
      const sentinel = await parseSentinel(sentinelPath)
      rejectSessionConflict(sentinel, liveStatus)
      const sessionId =
        sentinel?.SESSION ??
        (liveStatus?.session_id as string | undefined) ??
        fallbackSessionId
      if (!sessionId) {
        throw new Error('run has no session to resume')
      }

      // Prefer the supervisor's resolved identity, then local metadata. Never
      // send the roster selector on a follow-up: the original resolved values
      // are immutable for this continuation.
      const agent = resolvedIdentity(
        liveStatus,
        'effective_agent',
        'agent',
        runInfo?.effectiveAgent,
        runInfo?.agent,
      )
      const model = resolvedIdentity(
        liveStatus,
        'effective_model',
        'model',
        runInfo?.effectiveModel,
        runInfo?.model,
      )
      const backend = resolvedIdentity(
        liveStatus,
        'effective_backend',
        'backend',
        runInfo?.effectiveBackend,
        runInfo?.backend,
      )
      if (runInfo && liveStatus) retainLiveIdentity(runInfo, liveStatus)
      const thinking = (liveStatus?.thinking as ThinkingLevel | undefined) ?? runInfo?.thinking
      const dir = (liveStatus?.dir as string | undefined) ?? runInfo?.dir
      const agentProfile =
        (liveStatus?.agent_profile as string | undefined) ?? runInfo?.agentProfile
      const autoApprove = resolveAutoApprove(liveStatus, runInfo)

      const followUpRunId = crypto.randomUUID()
      const followUpLabel = args.label ?? `${args.runId}-followup`
      const spawnParams: SpawnParams = {
        prompt: args.message,
        label: followUpLabel,
        session_id: sessionId,
      }
      if (agent) spawnParams.agent = agent
      if (backend) spawnParams.backend = backend
      if (model) spawnParams.model = model
      if (thinking) spawnParams.thinking = thinking
      if (dir) spawnParams.dir = dir
      if (agentProfile) spawnParams.agent_profile = agentProfile
      if (autoApprove === true) spawnParams.auto_approve = true

      if (isSingleton && sup) {
        const followUpRun = await sup.spawn(spawnParams, followUpRunId, {
          rosterFile: runInfo?.rosterFile,
          rosterEntry: runInfo?.rosterEntry,
          effectiveAgent: agent,
          effectiveModel: model,
          effectiveBackend: backend,
        })
        return {
          run_id: followUpRun.runId,
          label: followUpRun.label,
          runtime_id: followUpRun.runtimeId,
        }
      }

      const { sentinelPath: followUpSentinelPath, eventLogPath } =
        ensureRunPaths(followUpRunId)
      spawnParams.sentinel_file = followUpSentinelPath
      spawnParams.on_event = eventLogPath
      const result = await client.spawn(spawnParams)
      const runtimeId = value(result.runtime_id)
      registerExternalRun({
        runId: followUpRunId,
        label: followUpLabel,
        supervisorId,
        sentinelPath: followUpSentinelPath,
        eventLogPath,
        runtimeId,
        sessionId: value(result.session_id) ?? sessionId,
        agent,
        model,
        backend,
        effectiveAgent: agent,
        effectiveModel: model,
        effectiveBackend: backend,
        rosterFile: runInfo?.rosterFile,
        rosterEntry: runInfo?.rosterEntry,
        thinking,
        dir,
        agentProfile,
        autoApprove,
      })
      return {
        run_id: followUpRunId,
        label: followUpLabel,
        runtime_id: runtimeId,
      }
    } finally {
      if (!isSingleton) {
        client.close()
      }
    }
  }

  const sup = await Supervisor.get()
  const runInfo = findLocalRunByReference(sup, args.runId)

  if (!runInfo) {
    throw new Error(`run not found: ${args.runId}`)
  }

  const sentinel = await parseSentinel(runInfo.sentinelPath)
  rejectSessionConflict(sentinel, null)
  const sessionId = sentinel?.SESSION ?? runInfo.sessionId

  if (!sessionId) {
    throw new Error('run has no session to resume')
  }

  const client = sup.getClient()

  // The run map preserves explicitly supplied context. Live status supplies
  // the resolved values when the original spawn relied on supervisor defaults.
  let liveStatus: Record<string, unknown> | null = null
  if (runInfo.runtimeId) {
    try {
      liveStatus = await client.status(runInfo.runtimeId)
    } catch {
      // Stored metadata still permits a follow-up when status is unavailable.
    }
  }
  rejectSessionConflict(sentinel, liveStatus)
  const agent = resolvedIdentity(
    liveStatus,
    'effective_agent',
    'agent',
    runInfo.effectiveAgent,
    runInfo.agent,
  )
  const model = resolvedIdentity(
    liveStatus,
    'effective_model',
    'model',
    runInfo.effectiveModel,
    runInfo.model,
  )
  const backend = resolvedIdentity(
    liveStatus,
    'effective_backend',
    'backend',
    runInfo.effectiveBackend,
    runInfo.backend,
  )
  if (liveStatus) retainLiveIdentity(runInfo, liveStatus)
  const thinking = (liveStatus?.thinking as ThinkingLevel | undefined) ?? runInfo.thinking
  const dir = (liveStatus?.dir as string | undefined) ?? runInfo.dir
  const agentProfile =
    (liveStatus?.agent_profile as string | undefined) ?? runInfo.agentProfile
  const autoApprove = resolveAutoApprove(liveStatus, runInfo)

  const followUpLabel = args.label ?? `${runInfo.label}-followup`

  const followUpParams: SpawnParams = {
    ...(agent ? { agent } : {}),
    ...(backend ? { backend } : {}),
    ...(model ? { model } : {}),
    ...(thinking ? { thinking } : {}),
    ...(dir ? { dir } : {}),
    ...(agentProfile ? { agent_profile: agentProfile } : {}),
    prompt: args.message,
    session_id: sessionId,
    label: followUpLabel,
    ...(autoApprove === true ? { auto_approve: true } : {}),
  }
  const followUpRun = await sup.spawn(followUpParams, undefined, {
    rosterFile: runInfo.rosterFile,
    rosterEntry: runInfo.rosterEntry,
    effectiveAgent: agent,
    effectiveModel: model,
    effectiveBackend: backend,
  })

  return {
    run_id: followUpRun.runId,
    label: followUpRun.label,
    runtime_id: followUpRun.runtimeId,
  }
}

export function createFollowUpTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: FollowUpToolArgs) => Promise<FollowUpToolResult> {
  return args => executeFollowUpTool(args, getSupervisorClient)
}

export const followUpTool = createFollowUpTool(realGetSupervisorClient)
