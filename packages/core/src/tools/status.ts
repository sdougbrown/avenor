import * as fs from 'node:fs'
import * as path from 'node:path'
import { Supervisor, retainLiveIdentity, type RunInfo } from '../supervisor.js'
import { type Client } from '../client.js'
import { runsRoot } from '../paths.js'
import { asRecord, numberField, stringArrayField, stringField } from '../value-fields.js'
import {
  validateRunId,
} from './validate.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { findExternalRun, listExternalRuns } from './run-registry.js'
import { findLocalRunByReference, findLocalRunByRuntimeId } from './run-resolution.js'

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

async function sentinelExists(filePath: string): Promise<boolean> {
  try {
    await fs.promises.access(filePath, fs.constants.R_OK)
    return true
  } catch {
    return false
  }
}

function rawStatusPhase(source: Record<string, unknown> | null): string {
  return stringField(source ?? {}, 'phase') || stringField(source ?? {}, 'status') || 'running'
}

function sentinelStatusToResult(sentinelStatus: string): string {
  switch (sentinelStatus) {
    case 'DONE':
      return 'done'
    case 'FAILED':
      return 'failed'
    case 'TIMEOUT':
      return 'timeout'
    case 'KILLED':
      return 'killed'
    case 'BLOCKED':
      return 'failed'
    default:
      return 'running'
  }
}

export function translateStatus(
  rawPhase: string,
  sentinelData: Record<string, string> | null,
): string {
  const sentinelStatus = sentinelData?._status
  const phase = rawPhase.toLowerCase()

  if (sentinelStatus && (phase === 'running' || phase === 'idle')) {
    return sentinelStatusToResult(sentinelStatus)
  }

  switch (phase) {
    case 'idle':
      return sentinelStatus ? sentinelStatusToResult(sentinelStatus) : 'running'
    case 'running':
      return 'running'
    case 'waiting':
      return 'waiting'
    case 'ended':
      return sentinelStatus ? sentinelStatusToResult(sentinelStatus) : 'running'
    case 'done':
      return 'done'
    case 'failed':
      return 'failed'
    case 'timeout':
      return 'timeout'
    case 'killed':
      return 'killed'
    case 'blocked':
      return 'failed'
    default:
      return 'running'
  }
}

function normalizePermission(raw: Record<string, unknown> | null | undefined): StatusResult['pending_permission'] | undefined {
  if (!raw) return undefined
  const request_id = stringField(raw, 'request_id', 'requestID') ?? ''
  const description =
    stringField(raw, 'description', 'question', 'tool', 'title') ?? ''
  const optionsRaw = raw.options
  const options = Array.isArray(optionsRaw)
    ? optionsRaw
        .map((item) => {
          const record = asRecord(item)
          if (!record) return null
          return {
            option_id: stringField(record, 'option_id', 'optionId') ?? '',
            label: stringField(record, 'label', 'name') ?? '',
            kind: stringField(record, 'kind') ?? '',
            ...((record.requires_message === true || record.requiresMessage === true) && { requires_message: true }),
          }
        })
        .filter((item): item is NonNullable<typeof item> => item !== null)
    : []

  if (!request_id && !description && options.length === 0) {
    return undefined
  }

  return { request_id, description, options }
}

function extractPendingPermission(liveStatus: Record<string, unknown> | null): StatusResult['pending_permission'] | undefined {
  if (!liveStatus) return undefined

  const permission = normalizePermission(asRecord(liveStatus.permission))
  if (permission) return permission

  const pendingPermission = liveStatus.pending_permission
  if (typeof pendingPermission === 'boolean') {
    return undefined
  }

  return normalizePermission(asRecord(pendingPermission))
}

function buildBaseStatus(
  source: Record<string, unknown>,
  fallback: {
    run_id: string
    label: string
    runtime_id?: string
    session_id?: string
    stop_reason?: string
    prefer_fallback_run_id?: boolean
    identity?: {
      roster_file?: string
      roster_entry?: string
      agent?: string
      model?: string
      backend?: string
      agent_profile?: string
      effective_agent?: string
      effective_model?: string
      effective_backend?: string
    }
  },
  translatedStatus: string,
): StatusResult {
  const identity = fallback.identity
  const authoritativeValue = (
    effectiveKey: string,
    directKey: string,
    fallbackEffective?: string,
    fallbackDirect?: string,
  ): string | undefined => {
    if (typeof source[effectiveKey] === 'string') return stringField(source, effectiveKey)
    if (typeof source[directKey] === 'string') return stringField(source, directKey)
    return fallbackEffective ?? fallbackDirect
  }
  const effectiveAgent = authoritativeValue(
    'effective_agent', 'agent', identity?.effective_agent, identity?.agent,
  )
  const effectiveModel = authoritativeValue(
    'effective_model', 'model', identity?.effective_model, identity?.model,
  )
  const effectiveBackend = authoritativeValue(
    'effective_backend', 'backend', identity?.effective_backend, identity?.backend,
  )

  return {
    run_id: fallback.prefer_fallback_run_id
      ? fallback.run_id
      : stringField(source, 'run_id') ?? fallback.run_id,
    label: stringField(source, 'label') ?? fallback.label,
    status: translatedStatus,
    runtime_id: stringField(source, 'runtime_id', 'id') ?? fallback.runtime_id,
    phase: stringField(source, 'phase'),
    phase_label: stringField(source, 'phase_label'),
    pending_permission: extractPendingPermission(source),
    session_id: stringField(source, 'session_id') ?? fallback.session_id,
    stop_reason: stringField(source, 'stop_reason') ?? fallback.stop_reason,
    roster_file: typeof source.roster_file === 'string'
      ? stringField(source, 'roster_file')
      : identity?.roster_file,
    roster_entry: typeof source.roster_entry === 'string'
      ? stringField(source, 'roster_entry')
      : identity?.roster_entry,
    backend: effectiveBackend,
    agent: effectiveAgent,
    model: effectiveModel,
    agent_profile: typeof source.agent_profile === 'string'
      ? stringField(source, 'agent_profile')
      : identity?.agent_profile,
    effective_backend: effectiveBackend,
    effective_agent: effectiveAgent,
    effective_model: effectiveModel,
    dir: stringField(source, 'dir'),
    parent_id: stringField(source, 'parent_id', 'parentId'),
    children: stringArrayField(source, 'children'),
    pid: typeof source.pid === 'number' ? source.pid : undefined,
    event_path: stringField(source, 'event_path', 'on_event'),
    usage: asRecord(source.usage) ?? undefined,
    latest_seq: numberField(source, 'latest_seq'),
    final_output: stringField(source, 'final_output', 'finalOutput'),
    ...(source.final_output_truncated === true && { final_output_truncated: true }),
  }
}

export type StatusView = 'lifecycle' | 'full'

export interface StatusToolArgs {
  runId?: string
  supervisorId?: string
  view?: StatusView
}

export interface StatusResult {
  run_id: string
  label: string
  status: string
  runtime_id?: string
  phase?: string
  phase_label?: string
  pending_permission?: {
    request_id: string
    description: string
    options: Array<{ option_id: string; label: string; kind: string; requires_message?: boolean }>
  }
  session_id?: string
  stop_reason?: string
  roster_file?: string
  roster_entry?: string
  backend?: string
  agent?: string
  model?: string
  agent_profile?: string
  effective_backend?: string
  effective_agent?: string
  effective_model?: string
  dir?: string
  parent_id?: string
  children?: string[]
  pid?: number
  event_path?: string
  usage?: Record<string, unknown>
  latest_seq?: number
  final_output?: string
  final_output_truncated?: boolean
}

export function shapeStatusResult(result: StatusResult, view: StatusView = 'full'): StatusResult {
  if (view === 'full') return result
  if (view !== 'lifecycle') throw new Error(`invalid status view: ${String(view)}`)

  return {
    run_id: result.run_id,
    label: result.label,
    status: result.status,
    ...(result.runtime_id !== undefined && { runtime_id: result.runtime_id }),
    ...(result.phase !== undefined && { phase: result.phase }),
    ...(result.phase_label !== undefined && { phase_label: result.phase_label }),
    ...(result.pending_permission !== undefined && { pending_permission: result.pending_permission }),
    ...(result.latest_seq !== undefined && { latest_seq: result.latest_seq }),
  }
}

async function buildRunStatus(
  runInfo: RunInfo,
  liveStatus: Record<string, unknown> | null,
): Promise<StatusResult> {
  if (liveStatus) retainLiveIdentity(runInfo, liveStatus)
  let sentinel: Record<string, string> | null = null
  if (await sentinelExists(runInfo.sentinelPath)) {
    sentinel = await parseSentinel(runInfo.sentinelPath)
  }

  const rawPhase = rawStatusPhase(liveStatus)
  const translated = translateStatus(rawPhase, sentinel)
  return buildBaseStatus(
    liveStatus ?? {},
    {
      run_id: runInfo.runId,
      label: runInfo.label,
      prefer_fallback_run_id: true,
      runtime_id: runInfo.runtimeId,
      session_id:
        (sentinel?.SESSION as string) ??
        (liveStatus?.session_id as string | undefined) ??
        runInfo.sessionId,
      stop_reason: sentinel?.STOP_REASON,
      identity: {
        roster_file: runInfo.rosterFile,
        roster_entry: runInfo.rosterEntry,
        agent: runInfo.agent,
        model: runInfo.model,
        backend: runInfo.backend,
        agent_profile: runInfo.agentProfile,
        effective_agent: runInfo.effectiveAgent,
        effective_model: runInfo.effectiveModel,
        effective_backend: runInfo.effectiveBackend,
      },
    },
    translated,
  )
}

async function queryLiveStatus(
  client: Client,
  runtimeId: string | undefined,
): Promise<Record<string, unknown> | null> {
  if (!runtimeId) return null
  try {
    return await client.status(runtimeId)
  } catch {
    return null
  }
}

async function executeStatusTool(
  args: StatusToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<StatusResult | StatusResult[]> {
  if (args.supervisorId) {
    const { client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId)
    try {
      if (args.runId) {
        validateRunId(args.runId)

        const runInfo = isSingleton && sup
          ? findLocalRunByReference(sup, args.runId)
          : findExternalRun(supervisorId, args.runId)
        if (runInfo) {
          const liveStatus = await queryLiveStatus(client, runInfo.runtimeId)
          return shapeStatusResult(await buildRunStatus(runInfo, liveStatus), args.view)
        }

        let liveStatus = await queryLiveStatus(client, args.runId)
        if (!liveStatus) {
          try {
            const list = await client.list()
            const entry = list.find((item) => {
              const runtimeId = String(item.runtime_id ?? item.id ?? '')
              const label = String(item.label ?? '')
              const runId = String(item.run_id ?? '')
              return runtimeId === args.runId || label === args.runId || runId === args.runId
            })
            if (entry) {
              liveStatus = (await queryLiveStatus(client, String(entry.runtime_id ?? entry.id ?? ''))) ?? entry
            }
          } catch {
            // live list unavailable
          }
        }

        const sentinelPath =
          (liveStatus?.sentinel_file as string | undefined) ??
          path.join(runsRoot(), args.runId, 'sentinel.done')
        let sentinel: Record<string, string> | null = null
        if (await sentinelExists(sentinelPath)) {
          sentinel = await parseSentinel(sentinelPath)
        }

        return shapeStatusResult(buildBaseStatus(
          liveStatus ?? {},
          {
            run_id: args.runId,
            label: args.runId,
            prefer_fallback_run_id: true,
            runtime_id: stringField(liveStatus ?? {}, 'runtime_id', 'id'),
            session_id:
              (sentinel?.SESSION as string) ??
              (liveStatus?.session_id as string | undefined),
            stop_reason: sentinel?.STOP_REASON,
          },
          translateStatus(rawStatusPhase(liveStatus), sentinel),
        ), args.view)
      }

      const list = await client.list()
      if (!isSingleton) listExternalRuns(supervisorId)
      const results: StatusResult[] = []
      for (const entry of list) {
        const entryId = String(entry.runtime_id ?? entry.id ?? '')
        const runInfo = !isSingleton
          ? findExternalRun(supervisorId, entryId)
            ?? findExternalRun(supervisorId, String(entry.run_id ?? ''))
            ?? findExternalRun(supervisorId, String(entry.label ?? ''))
          : undefined
        if (runInfo) {
          results.push(shapeStatusResult(await buildRunStatus(runInfo, entry), args.view))
          continue
        }
        results.push(shapeStatusResult(buildBaseStatus(
          entry,
          {
            run_id: String(entry.run_id ?? entryId),
            label: String(entry.label ?? entryId),
            runtime_id: entryId,
            session_id: entry.session_id as string | undefined,
          },
          translateStatus(rawStatusPhase(entry), null),
        ), args.view))
      }
      return results
    } finally {
      if (!isSingleton) {
        client.close()
      }
    }
  }

  const sup = await Supervisor.get()
  const client = sup.getClient()

  if (args.runId) {
    const runInfo = findLocalRunByReference(sup, args.runId)

    if (runInfo) {
      const liveStatus = await queryLiveStatus(client, runInfo.runtimeId)
      return shapeStatusResult(await buildRunStatus(runInfo, liveStatus), args.view)
    }

    validateRunId(args.runId)
    const liveStatus = await queryLiveStatus(client, args.runId)
    const sentinelPath = path.join(runsRoot(), args.runId, 'sentinel.done')
    let sentinel: Record<string, string> | null = null
    if (await sentinelExists(sentinelPath)) {
      sentinel = await parseSentinel(sentinelPath)
    }
    return shapeStatusResult(buildBaseStatus(
      liveStatus ?? {},
      {
        run_id: args.runId,
        label: args.runId,
        runtime_id: stringField(liveStatus ?? {}, 'runtime_id', 'id'),
        session_id:
          (sentinel?.SESSION as string) ??
          (liveStatus?.session_id as string | undefined),
        stop_reason: sentinel?.STOP_REASON,
      },
      translateStatus(rawStatusPhase(liveStatus), sentinel),
    ), args.view)
  }

  const list = await client.list()
  const results: StatusResult[] = []

  for (const entry of list) {
    const entryId = String(entry.runtime_id ?? entry.id ?? '')
    const entryLabel = String(entry.label ?? entryId)
    const runInfo = findLocalRunByRuntimeId(sup, entryId)
      ?? findLocalRunByReference(sup, entryId)
      ?? findLocalRunByReference(sup, entryLabel)

    const liveStatus = runInfo
      ? await queryLiveStatus(client, runInfo.runtimeId)
      : null

    if (runInfo) {
      results.push(shapeStatusResult(await buildRunStatus(runInfo, liveStatus), args.view))
    } else {
      results.push(shapeStatusResult(buildBaseStatus(
        entry,
        {
          run_id: String(entry.run_id ?? entryId),
          label: entryLabel,
          runtime_id: entryId,
          session_id: entry.session_id as string | undefined,
        },
        translateStatus(rawStatusPhase(entry), null),
      ), args.view))
    }
  }

  return results
}

export function createStatusTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: StatusToolArgs) => Promise<StatusResult | StatusResult[]> {
  return args => executeStatusTool(args, getSupervisorClient)
}

export const statusTool = createStatusTool(realGetSupervisorClient)
