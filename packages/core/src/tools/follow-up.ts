import * as fs from 'node:fs'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { ensureRunPaths, runsRoot } from '../paths.js'
import { validateRunId } from './validate.js'
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

export async function followUpTool(args: {
  runId: string
  message: string
  label?: string
  supervisorId?: string
}): Promise<{ run_id: string; label: string }> {
  if (args.supervisorId) {
    const { client, isSingleton } = await getSupervisorClient(args.supervisorId)
    try {
      validateRunId(args.runId)
      let liveStatus: Record<string, unknown> | null = null
      try {
        liveStatus = await client.status(args.runId)
      } catch {
        // ignore
      }

      const sentinelPath =
        (liveStatus?.sentinel_file as string | undefined) ??
        path.join(runsRoot(), args.runId, 'sentinel.done')
      const sentinel = await parseSentinel(sentinelPath)
      if (!sentinel?.SESSION) {
        throw new Error('run has no session to resume')
      }

      const agent = (liveStatus?.agent as string) ?? 'codex'

      const followUpRunId = crypto.randomUUID()
      const followUpLabel = args.label ?? `${args.runId}-followup`
      const { sentinelPath: followUpSentinelPath, eventLogPath } =
        ensureRunPaths(followUpRunId)
      await client.spawn({
        agent,
        prompt: args.message,
        label: followUpLabel,
        session_id: sentinel.SESSION,
        sentinel_file: followUpSentinelPath,
        on_event: eventLogPath,
      })
      return { run_id: followUpRunId, label: followUpLabel }
    } finally {
      if (!isSingleton) {
        client.close()
      }
    }
  }

  const sup = await Supervisor.get()
  const runInfo = findRunByLabel(sup, args.runId)

  if (!runInfo) {
    throw new Error(`run not found: ${args.runId}`)
  }

  const sentinel = await parseSentinel(runInfo.sentinelPath)

  if (!sentinel?.SESSION) {
    throw new Error('run has no session to resume')
  }

  const client = sup.getClient()

  let agent = 'codex'
  if (runInfo.runtimeId) {
    try {
      const liveStatus = await client.status(runInfo.runtimeId)
      agent = (liveStatus?.agent as string) ?? 'codex'
    } catch {
      // fallback to default
    }
  }

  const followUpLabel = args.label ?? `${runInfo.label}-followup`

  const followUpRun = await sup.spawn({
    agent,
    prompt: args.message,
    session_id: sentinel.SESSION,
    label: followUpLabel,
  })

  return { run_id: followUpRun.label, label: followUpRun.label }
}
