import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { dial } from '../client.js'
import { spawnTool } from './spawn.js'
import { validateRunId } from './validate.js'

function findRunByLabel(sup: Supervisor, runId: string): RunInfo | undefined {
  const runs = (sup as any).runs as Map<string, RunInfo>
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
    const client = await dial(args.supervisorId)
    try {
      validateRunId(args.runId)
      const sentinelPath = path.join(
        os.tmpdir(),
        `avenor-run-${args.runId}.done`,
      )
      const sentinel = await parseSentinel(sentinelPath)
      if (!sentinel?.SESSION) {
        throw new Error('run has no session to resume')
      }

      let liveStatus: Record<string, unknown> | null = null
      try {
        liveStatus = await client.status(args.runId)
      } catch {
        // ignore
      }

      const agent = (liveStatus?.agent as string) ?? 'codex'

      return spawnTool({
        agent,
        prompt: args.message,
        sessionId: sentinel.SESSION,
        label: args.label,
        supervisorId: args.supervisorId,
      })
    } finally {
      client.close()
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

  return spawnTool({
    agent,
    prompt: args.message,
    sessionId: sentinel.SESSION,
    label: followUpLabel,
    supervisorId: sup.supervisorId,
  })
}
