import * as fs from 'node:fs'
import * as readline from 'node:readline'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { type Client } from '../client.js'
import { asRecord } from '../value-fields.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { validateRunId } from './validate.js'

const MAX_EVENT_LIMIT = 1_000
const MAX_STRING_CHARS = 4_000
const MAX_ARRAY_ITEMS = 32
const MAX_OBJECT_KEYS = 64

interface EventsToolArgs {
  runId: string
  types?: string[]
  limit?: number
  supervisorId?: string
  after_seq?: number
}

interface EventsToolResult {
  events: Array<{ event?: string; type?: string; ts?: number; [key: string]: unknown }>
}

function findRunByLabel(sup: Supervisor, runId: string): RunInfo | undefined {
  const runs = (sup as any).runs as Map<string, RunInfo>
  const byKey = runs.get(runId)
  if (byKey) return byKey
  for (const info of runs.values()) {
    if (info.label === runId) return info
  }
  return undefined
}

function normalizeLimit(limit?: number): number {
  return Math.max(1, Math.min(MAX_EVENT_LIMIT, Math.trunc(limit ?? 50)))
}

function contextualError(context: string, error: unknown): Error {
  const message = error instanceof Error ? error.message : String(error)
  return new Error(`${context}: ${message}`)
}

function eventName(record: Record<string, unknown>): string {
  const event = record.event
  if (typeof event === 'string' && event.length > 0) return event
  const type = record.type
  return typeof type === 'string' ? type : ''
}

function eventSeq(record: Record<string, unknown>): number | undefined {
  const value = record.seq
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) return parsed
  }
  return undefined
}

function clipValue(value: unknown): unknown {
  if (typeof value === 'string') {
    if (value.length <= MAX_STRING_CHARS) return value
    return value.slice(value.length - MAX_STRING_CHARS)
  }
  if (Array.isArray(value)) {
    const slice = value.length <= MAX_ARRAY_ITEMS
      ? value
      : value.slice(value.length - MAX_ARRAY_ITEMS)
    return slice.map((item) => clipValue(item))
  }
  const record = asRecord(value)
  if (!record) return value
  const result: Record<string, unknown> = {}
  const entries = Object.entries(record).slice(0, MAX_OBJECT_KEYS)
  for (const [key, item] of entries) {
    result[key] = clipValue(item)
  }
  return result
}

function clipEvent(record: Record<string, unknown>): Record<string, unknown> {
  return clipValue(record) as Record<string, unknown>
}

function matchesFilters(
  record: Record<string, unknown>,
  args: { types?: string[]; after_seq?: number },
): boolean {
  const name = eventName(record)
  if (args.types && args.types.length > 0 && !args.types.includes(name)) {
    return false
  }
  const seq = eventSeq(record)
  if (args.after_seq !== undefined && seq !== undefined && seq <= args.after_seq) {
    return false
  }
  return true
}

async function readNdjsonEvents(
  filePath: string,
  args: { types?: string[]; limit: number; after_seq?: number },
): Promise<Array<Record<string, unknown>>> {
  const stream = fs.createReadStream(filePath, { encoding: 'utf-8' })
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity })
  const events: Array<Record<string, unknown>> = []

  try {
    for await (const line of rl) {
      if (!line.trim()) continue
      try {
        const parsed = JSON.parse(line)
        const record = asRecord(parsed)
        if (!record || !matchesFilters(record, args)) continue
        events.push(clipEvent(record))
        if (events.length > args.limit) {
          events.splice(0, events.length - args.limit)
        }
      } catch {
        // skip malformed lines
      }
    }
  } finally {
    rl.close()
    stream.close()
  }

  return events
}

async function resolveRun(
  args: { runId: string; supervisorId?: string },
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<{
  client: Client
  isSingleton: boolean
  runInfo?: RunInfo
  runtimeId?: string
  eventLogPath?: string
}> {
  if (args.supervisorId) {
    const { client, isSingleton, sup } = await getSupervisorClient(args.supervisorId)
    const runInfo = isSingleton && sup ? findRunByLabel(sup, args.runId) : undefined
    if (runInfo) {
      return {
        client,
        isSingleton,
        runInfo,
        runtimeId: runInfo.runtimeId,
        eventLogPath: runInfo.eventLogPath,
      }
    }

    let runtimeId: string | undefined
    let eventLogPath: string | undefined
    try {
      const direct = await client.status(args.runId)
      runtimeId = (direct.runtime_id as string | undefined) ?? args.runId
      eventLogPath =
        (direct.event_path as string | undefined) ??
        (direct.on_event as string | undefined)
    } catch (statusError) {
      let list: Array<Record<string, unknown>>
      try {
        list = await client.list()
      } catch (listError) {
        if (!isSingleton) client.close()
        throw new AggregateError(
          [
            contextualError(`status lookup for ${args.runId}`, statusError),
            contextualError('runtime list fallback', listError),
          ],
          `unable to resolve run ${args.runId}`,
        )
      }
      const match = list.find((entry) => {
        const runtime = String(entry.runtime_id ?? entry.id ?? '')
        const label = String(entry.label ?? '')
        const runId = String(entry.run_id ?? '')
        return runtime === args.runId || label === args.runId || runId === args.runId
      })
      if (!match) {
        if (!isSingleton) client.close()
        throw contextualError(`unable to resolve run ${args.runId}`, statusError)
      }
      runtimeId = String(match.runtime_id ?? match.id ?? '') || undefined
      eventLogPath =
        (match.event_path as string | undefined) ??
        (match.on_event as string | undefined)
    }

    return { client, isSingleton, runtimeId, eventLogPath }
  }

  const sup = await Supervisor.get()
  const runInfo = findRunByLabel(sup, args.runId)
  if (runInfo) {
    return {
      client: sup.getClient(),
      isSingleton: true,
      runInfo,
      runtimeId: runInfo.runtimeId,
      eventLogPath: runInfo.eventLogPath,
    }
  }

  validateRunId(args.runId)
  let runtimeId: string | undefined
  let eventLogPath: string | undefined
  try {
    const direct = await sup.getClient().status(args.runId)
    runtimeId = (direct.runtime_id as string | undefined) ?? args.runId
    eventLogPath =
      (direct.event_path as string | undefined) ??
      (direct.on_event as string | undefined)
  } catch (error) {
    throw contextualError(`unable to resolve run ${args.runId}`, error)
  }

  return { client: sup.getClient(), isSingleton: true, runtimeId, eventLogPath }
}

async function executeEventsTool(
  args: EventsToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<EventsToolResult> {
  const limit = normalizeLimit(args.limit)
  const resolved = await resolveRun(args, getSupervisorClient)

  try {
    const failures: Error[] = []

    // Preserve eventsTool's durable-history semantics when the supervisor
    // exposes the NDJSON path. The control-plane ring is intentionally small
    // and can no longer answer filters that match only rotated-out events.
    if (resolved.eventLogPath) {
      try {
        return {
          events: await readNdjsonEvents(resolved.eventLogPath, {
            types: args.types,
            limit,
            after_seq: args.after_seq,
          }),
        }
      } catch (error) {
        // The path may belong to another process namespace; try live history.
        failures.push(contextualError(`read event log ${resolved.eventLogPath}`, error))
      }
    }

    if (resolved.runtimeId) {
      try {
        const history = await resolved.client.history({
          runtime_id: resolved.runtimeId,
          limit,
          after_seq: args.after_seq,
        })
        const events = Array.isArray(history.events)
          ? history.events
              .map((event) => asRecord(event))
              .filter((event): event is Record<string, unknown> => event !== null)
              .filter((event) => matchesFilters(event, args))
              .map((event) => clipEvent(event))
          : []
        return { events }
      } catch (error) {
        failures.push(contextualError(`read history for ${resolved.runtimeId}`, error))
      }
    }

    if (failures.length > 0) {
      throw new AggregateError(failures, `unable to read events for run ${args.runId}`)
    }
    return { events: [] }
  } finally {
    if (!resolved.isSingleton) {
      resolved.client.close()
    }
  }
}

export function createEventsTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: EventsToolArgs) => Promise<EventsToolResult> {
  return args => executeEventsTool(args, getSupervisorClient)
}

export const eventsTool = createEventsTool(realGetSupervisorClient)
