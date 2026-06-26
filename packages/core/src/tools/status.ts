import * as fs from 'node:fs'
import * as path from 'node:path'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { type Client } from '../client.js'
import { runsRoot } from '../paths.js'
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

  // A terminal sentinel takes precedence over a live "running" phase. In
  // stable mode the supervisor parks a runtime after a clean end_turn, leaving
  // status "running" while the sentinel already reads DONE. Without this
  // override the orchestrator's blocking loop would never observe completion.
  if (sentinelStatus && (rawPhase === 'running' || rawPhase === 'idle')) {
    return sentinelStatusToResult(sentinelStatus)
  }

  switch (rawPhase) {
    case 'idle':
      return sentinelStatus ? sentinelStatusToResult(sentinelStatus) : 'running'
    case 'running':
      return 'running'
    case 'waiting':
      return 'waiting'
    case 'ended':
      return sentinelStatus ? sentinelStatusToResult(sentinelStatus) : 'running'
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
}

async function buildRunStatus(
  client: Client,
  runInfo: RunInfo,
  liveStatus: Record<string, unknown> | null,
): Promise<StatusResult> {
  let sentinel: Record<string, string> | null = null
  if (await sentinelExists(runInfo.sentinelPath)) {
    sentinel = await parseSentinel(runInfo.sentinelPath)
  }

  const rawPhase = (liveStatus?.phase ?? liveStatus?.status ?? 'running') as string
  const translated = translateStatus(rawPhase, sentinel)

  const result: StatusResult = {
    run_id: runInfo.label,
    label: runInfo.label,
    status: translated,
    runtime_id: (liveStatus?.runtime_id as string) ?? runInfo.runtimeId,
    phase: liveStatus?.phase as string | undefined,
    phase_label: liveStatus?.phase_label as string | undefined,
    session_id:
      (sentinel?.SESSION as string) ??
      (liveStatus?.session_id as string) ??
      undefined,
    stop_reason: sentinel?._status ?? undefined,
  }

  if (liveStatus?.pending_permission) {
    const pp = liveStatus.pending_permission as any
    result.pending_permission = {
      request_id: pp.request_id ?? '',
      description: pp.description ?? '',
      options: Array.isArray(pp.options)
        ? pp.options.map((o: any) => ({
            option_id: o.option_id ?? '',
            label: o.label ?? '',
            kind: o.kind ?? '',
          }))
        : [],
    }
  }

  return result
}

export async function statusTool(
  args: {
    runId?: string
    supervisorId?: string
  },
): Promise<StatusResult | StatusResult[]> {
  if (args.supervisorId) {
    const { client, isSingleton } = await getSupervisorClient(args.supervisorId)
    try {
      if (args.runId) {
        validateRunId(args.runId)
        let liveStatus: Record<string, unknown> | null = null
        try {
          liveStatus = await client.status(args.runId)
        } catch {
          // live status unavailable
        }

        const sentinelPath =
          (liveStatus?.sentinel_file as string | undefined) ??
          path.join(runsRoot(), args.runId, 'sentinel.done')
        let sentinel: Record<string, string> | null = null
        if (await sentinelExists(sentinelPath)) {
          sentinel = await parseSentinel(sentinelPath)
        }

        return {
          run_id: args.runId,
          label: args.runId,
          status: translateStatus((liveStatus?.phase ?? liveStatus?.status ?? 'running') as string, sentinel),
          runtime_id: liveStatus?.runtime_id as string | undefined,
          phase: liveStatus?.phase as string | undefined,
          phase_label: liveStatus?.phase_label as string | undefined,
          session_id:
            (sentinel?.SESSION as string) ??
            (liveStatus?.session_id as string) ??
            undefined,
          stop_reason: sentinel?._status ?? undefined,
          pending_permission: liveStatus?.pending_permission
            ? {
                request_id:
                  (liveStatus.pending_permission as any).request_id ?? '',
                description:
                  (liveStatus.pending_permission as any).description ?? '',
                options: Array.isArray(
                  (liveStatus.pending_permission as any).options,
                )
                  ? (
                      liveStatus.pending_permission as any
                    ).options.map((o: any) => ({
                      option_id: o.option_id ?? '',
                      label: o.label ?? '',
                      kind: o.kind ?? '',
                    }))
                  : [],
              }
            : undefined,
        }
      }

      const list = await client.list()
      return list.map((entry: any) => ({
        run_id: (entry.runtime_id ?? entry.id ?? '') as string,
        label: (entry.label ?? '') as string,
        status: translateStatus((entry.phase ?? entry.status ?? 'running') as string, null),
        phase: entry.phase as string | undefined,
        session_id: entry.session_id as string | undefined,
      }))
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

    // Registry hit: use tracked run info
    if (runInfo) {
      let liveStatus: Record<string, unknown> | null = null
      if (runInfo.runtimeId) {
        try {
          liveStatus = await client.status(runInfo.runtimeId)
        } catch {
          // live status unavailable
        }
      }
      return buildRunStatus(client, runInfo, liveStatus)
    }

    // Registry miss: query stable runtime ID directly via the default client
    validateRunId(args.runId)
    let liveStatus: Record<string, unknown> | null = null
    try {
      liveStatus = await client.status(args.runId)
    } catch {
      // ignore
    }
    const sentinelPath = path.join(runsRoot(), args.runId, 'sentinel.done')
    let sentinel: Record<string, string> | null = null
    if (await sentinelExists(sentinelPath)) {
      sentinel = await parseSentinel(sentinelPath)
    }
    return {
      run_id: args.runId,
      label: args.runId,
      status: translateStatus((liveStatus?.phase ?? liveStatus?.status ?? 'running') as string, sentinel),
      phase: liveStatus?.phase as string | undefined,
      phase_label: liveStatus?.phase_label as string | undefined,
      session_id:
        (sentinel?.SESSION as string) ??
        (liveStatus?.session_id as string) ??
        undefined,
      stop_reason: sentinel?._status ?? undefined,
      pending_permission: liveStatus?.pending_permission
        ? {
            request_id: (liveStatus.pending_permission as any).request_id ?? '',
            description: (liveStatus.pending_permission as any).description ?? '',
            options: Array.isArray((liveStatus.pending_permission as any).options)
              ? (liveStatus.pending_permission as any).options.map((o: any) => ({
                  option_id: o.option_id ?? '',
                  label: o.label ?? '',
                  kind: o.kind ?? '',
                }))
              : [],
          }
        : undefined,
    }
  }

  const list = await client.list()
  const results: StatusResult[] = []

  for (const entry of list) {
    const entryId = (entry.runtime_id ?? entry.id ?? '') as string
    const entryLabel = (entry.label ?? entryId) as string
    const runInfo = findRunByLabel(sup, entryId) ?? findRunByLabel(sup, entryLabel)

    let liveStatus: Record<string, unknown> | null = null
    if (runInfo?.runtimeId) {
      try {
        liveStatus = await client.status(runInfo.runtimeId)
      } catch {
        // ignore
      }
    }

    if (runInfo && liveStatus) {
      results.push(await buildRunStatus(client, runInfo, liveStatus))
    } else {
      results.push({
        run_id: entryId,
        label: entryLabel,
        status: translateStatus((entry.phase ?? entry.status ?? 'running') as string, null),
        phase: entry.phase as string | undefined,
        session_id: entry.session_id as string | undefined,
      })
    }
  }

  return results
}
