import * as fs from 'node:fs'
import { Supervisor, type RunInfo } from '../supervisor.js'

function findRunByLabel(sup: Supervisor, runId: string): RunInfo | undefined {
  const runs = (sup as any).runs as Map<string, RunInfo>
  const byKey = runs.get(runId)
  if (byKey) return byKey
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
}): Promise<{events: Array<{ type: string; ts: number; [key: string]: unknown }>}> {
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

  const limit = Math.max(1, Math.min(1000, Math.trunc(args.limit ?? 50)))
  let raw: string
  try {
    const stat = await fs.promises.stat(runInfo.eventLogPath)
    const tailBytes = Math.min(stat.size, 256 * 1024)
    const handle = await fs.promises.open(runInfo.eventLogPath, 'r')
    try {
      const buffer = Buffer.alloc(tailBytes)
      const start = Math.max(0, stat.size - tailBytes)
      const { bytesRead } = await handle.read(buffer, 0, tailBytes, start)
      raw = buffer.subarray(0, bytesRead).toString('utf-8')
    } finally {
      await handle.close()
    }
  } catch {
    return { events: [] }
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

  if (events.length > limit) {
    events = events.slice(events.length - limit)
  }

  return { events } as { events: Array<{ type: string; ts: number; [key: string]: unknown }> }
}
