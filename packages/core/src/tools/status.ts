import * as fs from 'node:fs'
import * as path from 'node:path'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { type Client } from '../client.js'
import { runsRoot } from '../paths.js'
import { asRecord, numberField, stringArrayField, stringField } from '../value-fields.js'
import {
  validateRunId,
} from './validate.js'
import { getSupervisorClient } from './get-supervisor-client.js'

function findRunByLabel(sup: Supervisor, runId: string): RunInfo | undefined {
  const runs = (sup as any).runs as Map<string, RunInfo>
  const byKey = runs.get(runId)
  if (byKey) return byKey
  for (const info of runs.values()) {
    if (info.label === runId) return info
  }
  return undefined
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

async function sentinelExists(filePath: string): Promise<boolean> {
  try {
    await fs.promises.access(filePath, fs.constants.R_OK)
    return true
  } catch {
    return false
  }
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
            label: stringField(record, 'label') ?? '',
            kind: stringField(record, 'kind') ?? '',
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
  },
  translatedStatus: string,
): StatusResult {
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
    backend: stringField(source, 'backend'),
    agent: stringField(source, 'agent'),
    model: stringField(source, 'model'),
    dir: stringField(source, 'dir'),
    parent_id: stringField(source, 'parent_id', 'parentId'),
    children: stringArrayField(source, 'children'),
    event_path: stringField(source, 'event_path', 'on_event'),
    usage: asRecord(source.usage) ?? undefined,
    latest_seq: numberField(source, 'latest_seq'),
    final_output: stringField(source, 'final_output', 'finalOutput'),
  }
}

export type StatusView = 'lifecycle' | 'full'

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
    options: Array<{ option_id: string; label: string; kind: string }>
  }
  session_id?: string
  stop_reason?: string
  backend?: string
  agent?: string
  model?: string
  dir?: string
  parent_id?: string
  children?: string[]
  event_path?: string
  usage?: Record<string, unknown>
  latest_seq?: number
  final_output?: string
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
  let sentinel: Record<string, string> | null = null
  if (await sentinelExists(runInfo.sentinelPath)) {
    sentinel = await parseSentinel(runInfo.sentinelPath)
  }

  const rawPhase = (liveStatus?.phase ?? liveStatus?.status ?? 'running') as string
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
      stop_reason: sentinel?._status,
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

export async function statusTool(
  args: {
    runId?: string
    supervisorId?: string
    view?: StatusView
  },
): Promise<StatusResult | StatusResult[]> {
  if (args.supervisorId) {
    const { client, isSingleton, sup } = await getSupervisorClient(args.supervisorId)
    try {
      if (args.runId) {
        validateRunId(args.runId)

        const runInfo = isSingleton && sup
          ? findRunByLabel(sup, args.runId)
          : undefined
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
            runtime_id: stringField(liveStatus ?? {}, 'runtime_id', 'id'),
            session_id:
              (sentinel?.SESSION as string) ??
              (liveStatus?.session_id as string | undefined),
            stop_reason: sentinel?._status,
          },
          translateStatus((liveStatus?.phase ?? liveStatus?.status ?? 'running') as string, sentinel),
        ), args.view)
      }

      const list = await client.list()
      return list.map((entry: any) => shapeStatusResult(buildBaseStatus(
        entry,
        {
          run_id: String(entry.run_id ?? entry.runtime_id ?? entry.id ?? ''),
          label: String(entry.label ?? entry.runtime_id ?? entry.id ?? ''),
          runtime_id: String(entry.runtime_id ?? entry.id ?? ''),
          session_id: entry.session_id as string | undefined,
        },
        translateStatus((entry.phase ?? entry.status ?? 'running') as string, null),
      ), args.view))
    } finally {
      if (!isSingleton) {
        client.close()
      }
    }
  }

  const sup = await Supervisor.get()
  const client = sup.getClient()

  if (args.runId) {
    const runInfo = findRunByLabel(sup, args.runId)

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
        stop_reason: sentinel?._status,
      },
      translateStatus((liveStatus?.phase ?? liveStatus?.status ?? 'running') as string, sentinel),
    ), args.view)
  }

  const list = await client.list()
  const results: StatusResult[] = []

  for (const entry of list) {
    const entryId = String(entry.runtime_id ?? entry.id ?? '')
    const entryLabel = String(entry.label ?? entryId)
    const runInfo = findRunByLabel(sup, entryId) ?? findRunByLabel(sup, entryLabel)

    const liveStatus = runInfo
      ? await queryLiveStatus(client, runInfo.runtimeId)
      : null

    if (runInfo && liveStatus) {
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
        translateStatus((entry.phase ?? entry.status ?? 'running') as string, null),
      ), args.view))
    }
  }

  return results
}
