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
import { brokerPeerStatus, lookupBrokerPeer, type BrokerPeerLookup } from './broker-peer.js'
import { ambiguousName } from '../message.js'
import { runtimeNotFoundMessage, statusReportsNoRuntime } from './message-provider.js'
import { isTerminalPhase, sentinelStatus, translate } from '../run-state.js'
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

/**
 * The status a run presents, from the supervisor's raw `status` and `phase` and
 * the run's own sentinel. Rules, in order:
 *
 * 1. Both raw values are lowercased; `translate` itself is case-sensitive.
 * 2. `translate` normalizes the pair, the same as the Go planes.
 * 3. A running runtime whose phase is `waiting` presents `waiting`. Go reports
 *    running plus a pending permission instead; this surface's callers wait on
 *    the status word.
 * 4. The sentinel overlays the status when it is `running` with no phase or a
 *    terminal phase, `unknown`, or `done` from a raw `ended`: the sentinel's
 *    outcome is `sentinelStatus` of its status word. It never overlays `waiting`,
 *    `running` in an active phase, nor a terminal status the raw status or phase
 *    produced directly.
 * 5. With no sentinel the translated status stands.
 */
export function translateStatus(
  rawStatus: string,
  rawPhase: string,
  sentinel: Record<string, string> | null,
): string {
  const status = rawStatus.toLowerCase()
  const phase = rawPhase.toLowerCase()
  const translated = translate(status, phase).status
  if (translated === 'running' && phase === 'waiting') return 'waiting'

  const word = sentinel?._status
  if (!word) return translated
  const settledByEnding = translated === 'done' && status === 'ended'
  // A running runtime in an active phase keeps running: a follow-up turn wakes a
  // parked runtime without rotating its sentinel, so the file can still hold the
  // previous turn's outcome.
  const settledWhileRunning = translated === 'running' && (phase === '' || isTerminalPhase(phase))
  if (settledWhileRunning || translated === 'unknown' || settledByEnding) return sentinelStatus(word)
  return translated
}

function presentStatus(source: Record<string, unknown> | null, sentinel: Record<string, string> | null): string {
  const fields = source ?? {}
  return translateStatus(stringField(fields, 'status') ?? '', stringField(fields, 'phase') ?? '', sentinel)
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

/** List form of the status tool result (no runId). structuredContent must be
 * an object, so the runs array is wrapped with a count. */
export interface StatusListResult {
  runs: StatusResult[]
  count: number
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
  runtime_output_available?: boolean
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
  const liveSentinel = stringField(liveStatus ?? {}, 'sentinel_file')
  const belongsToRun = !liveSentinel || liveSentinel === runInfo.sentinelPath
  if (liveStatus && belongsToRun) retainLiveIdentity(runInfo, liveStatus)
  let sentinel: Record<string, string> | null = null
  if (await sentinelExists(runInfo.sentinelPath)) {
    sentinel = await parseSentinel(runInfo.sentinelPath)
  }

  // When the live turn is not this run's, its session belongs to a newer
  // turn on the shared runtime; only the run's own registered session may
  // answer for this one.
  const source = belongsToRun ? liveStatus : null
  // A turn queued behind another on the shared runtime has no status of its own
  // yet, and is pending rather than unknown.
  const translated = !belongsToRun && !sentinel ? 'running' : presentStatus(source, sentinel)
  const result = buildBaseStatus(
    source ?? {},
    {
      run_id: runInfo.runId,
      label: runInfo.label,
      prefer_fallback_run_id: true,
      runtime_id: runInfo.runtimeId,
      session_id:
        (sentinel?.SESSION as string) ??
        (source?.session_id as string | undefined) ??
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
  result.label = runInfo.label
  if (!belongsToRun || !result.event_path) result.event_path = runInfo.eventLogPath
  if (!belongsToRun || !liveStatus) result.runtime_output_available = false
  return result
}

/**
 * Read one runtime's live status. `absent` is true only when the supervisor
 * answered that it runs no such runtime, so a refusal or a transport failure
 * is never read as the run being elsewhere.
 */
async function queryRuntimeStatus(
  client: Client,
  runtimeId: string | undefined,
): Promise<{ live: Record<string, unknown> | null; absent: boolean }> {
  if (!runtimeId) return { live: null, absent: false }
  try {
    return { live: await client.status(runtimeId), absent: false }
  } catch (error) {
    return { live: null, absent: statusReportsNoRuntime(error, runtimeId) }
  }
}

function unresolvedRunError(runId: string, lookup: Exclude<BrokerPeerLookup, { match: 'unique' }>): Error {
  return lookup.match === 'ambiguous'
    ? ambiguousName(runId, lookup.candidates)
    : new Error(runtimeNotFoundMessage(runId))
}

async function queryLiveStatus(
  client: Client,
  runtimeId: string | undefined,
): Promise<Record<string, unknown> | null> {
  return (await queryRuntimeStatus(client, runtimeId)).live
}

/**
 * The live status of ref when the supervisor does not know it by runtime id:
 * the runtime list entry whose runtime id, label or run id is ref, with its own
 * status where the supervisor answers one. `listed` reports whether the list
 * could be read, so a caller treats a run as absent only when it could.
 */
async function resolveListedStatus(
  client: Client,
  ref: string,
  live: Record<string, unknown> | null,
): Promise<{ liveStatus: Record<string, unknown> | null; listed: boolean }> {
  if (live) return { liveStatus: live, listed: false }
  try {
    const list = await client.list()
    const entry = list.find((item) => {
      const runtimeId = String(item.runtime_id ?? item.id ?? '')
      const label = String(item.label ?? '')
      const runId = String(item.run_id ?? '')
      return runtimeId === ref || label === ref || runId === ref
    })
    if (!entry) return { liveStatus: null, listed: true }
    const runtimeId = String(entry.runtime_id ?? entry.id ?? '')
    return { liveStatus: (await queryLiveStatus(client, runtimeId)) ?? entry, listed: true }
  } catch {
    return { liveStatus: null, listed: false }
  }
}

async function executeStatusTool(
  args: StatusToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<StatusResult | StatusListResult> {
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

        const probe = await queryRuntimeStatus(client, args.runId)
        const { liveStatus, listed } = await resolveListedStatus(client, args.runId, probe.live)

        const sentinelPath =
          (liveStatus?.sentinel_file as string | undefined) ??
          path.join(runsRoot(), args.runId, 'sentinel.done')
        let sentinel: Record<string, string> | null = null
        if (await sentinelExists(sentinelPath)) {
          sentinel = await parseSentinel(sentinelPath)
        } else if (probe.absent && listed && !liveStatus) {
          const lookup = await lookupBrokerPeer(client, args.runId)
          if (lookup.match === 'unique') return shapeStatusResult(brokerPeerStatus(lookup.peer), args.view)
          throw unresolvedRunError(args.runId, lookup)
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
          presentStatus(liveStatus, sentinel),
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
          presentStatus(entry, null),
        ), args.view))
      }
      return { runs: results, count: results.length }
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
    const probe = await queryRuntimeStatus(client, args.runId)
    const { liveStatus, listed } = await resolveListedStatus(client, args.runId, probe.live)
    const sentinelPath = path.join(runsRoot(), args.runId, 'sentinel.done')
    let sentinel: Record<string, string> | null = null
    if (await sentinelExists(sentinelPath)) {
      sentinel = await parseSentinel(sentinelPath)
    } else if (probe.absent && listed && !liveStatus) {
      const lookup = await lookupBrokerPeer(client, args.runId)
      if (lookup.match === 'unique') return shapeStatusResult(brokerPeerStatus(lookup.peer), args.view)
      throw unresolvedRunError(args.runId, lookup)
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
      presentStatus(liveStatus, sentinel),
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
        presentStatus(entry, null),
      ), args.view))
    }
  }

  return { runs: results, count: results.length }
}

export function createStatusTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: StatusToolArgs) => Promise<StatusResult | StatusListResult> {
  return args => executeStatusTool(args, getSupervisorClient)
}

export const statusTool = createStatusTool(realGetSupervisorClient)
