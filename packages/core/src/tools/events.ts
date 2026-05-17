import * as fs from 'node:fs'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { dial } from '../client.js'

function findRunByLabel(sup: Supervisor, runId: string): RunInfo | undefined {
  const runs = (sup as any).runs as Map<string, RunInfo>
  for (const info of runs.values()) {
    if (info.label === runId) return info
  }
  return undefined
}

export async function eventsTool(args: {
  runId: string
  types?: string[]
  limit?: number
  supervisorId?: string
}): Promise<Array<{ type: string; ts: number; [key: string]: unknown }>> {
  if (args.supervisorId) {
    throw new Error(
      'eventsTool with explicit supervisorId not supported — ' +
        'event log path must be tracked by the singleton supervisor',
    )
  }

  const sup = await Supervisor.get()
  const runInfo = findRunByLabel(sup, args.runId)

  if (!runInfo) {
    throw new Error(`run not found: ${args.runId}`)
  }

  let raw: string
  try {
    raw = fs.readFileSync(runInfo.eventLogPath, 'utf-8')
  } catch {
    return []
  }

  const lines = raw.split('\n').filter((line) => line.trim())
  let events: Array<{ type: string; ts: number; [key: string]: unknown }> = []

  for (const line of lines) {
    try {
      events.push(JSON.parse(line))
    } catch {
      // skip malformed lines
    }
  }

  if (args.types && args.types.length > 0) {
    events = events.filter((e) => args.types!.includes(e.type ?? e.event ?? ''))
  }

  const limit = args.limit ?? 50
  if (events.length > limit) {
    events = events.slice(events.length - limit)
  }

  return events
}
