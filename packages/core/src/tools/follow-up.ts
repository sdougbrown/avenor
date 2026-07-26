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

function requireResumableAgent(agent: string | undefined): string {
  if (agent === undefined) {
    throw new Error('run has no agent to resume')
  }
  return agent
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
    const { client, isSingleton, sup } = await getSupervisorClient(args.supervisorId)
    try {
      validateRunId(args.runId)

      // Try to resolve the run via the local supervisor's run map first
      let runInfo: RunInfo | undefined
      if (isSingleton && sup) {
        runInfo = findRunByLabel(sup, args.runId)
      }
      const fallbackSessionId = runInfo?.sessionId

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
      const sessionId =
        sentinel?.SESSION ??
        (liveStatus?.session_id as string | undefined) ??
        fallbackSessionId
      if (!sessionId) {
        throw new Error('run has no session to resume')
      }

      // Prefer live supervisor metadata, then the local run map, so an
      // explicit supervisor retains the original runtime context as well.
      const agent = requireResumableAgent(
        (liveStatus?.agent as string | undefined) ?? runInfo?.agent,
      )
      const backend =
        (liveStatus?.backend as string | undefined) ?? runInfo?.backend
      const dir = (liveStatus?.dir as string | undefined) ?? runInfo?.dir
      const agentProfile =
        (liveStatus?.agent_profile as string | undefined) ?? runInfo?.agentProfile

      const followUpRunId = crypto.randomUUID()
      const followUpLabel = args.label ?? `${args.runId}-followup`
      const { sentinelPath: followUpSentinelPath, eventLogPath } =
        ensureRunPaths(followUpRunId)

      const spawnParams: Record<string, unknown> = {
        agent,
        prompt: args.message,
        label: followUpLabel,
        session_id: sessionId,
        sentinel_file: followUpSentinelPath,
        on_event: eventLogPath,
      }
      if (backend) spawnParams.backend = backend
      if (dir) spawnParams.dir = dir
      if (agentProfile) spawnParams.agent_profile = agentProfile

      const result = await client.spawn(spawnParams)
      // With an external supervisor there is no local Supervisor.run map to
      // translate our filesystem UUID back to the control-plane runtime. As
      // spawnTool does, expose the runtime ID so status/permission calls can
      // address the follow-up directly.
      return {
        run_id: (result.runtime_id as string | undefined) ?? followUpRunId,
        label: followUpLabel,
      }
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
  const agent = requireResumableAgent(
    (liveStatus?.agent as string | undefined) ?? runInfo.agent,
  )
  const backend = (liveStatus?.backend as string | undefined) ?? runInfo.backend
  const dir = (liveStatus?.dir as string | undefined) ?? runInfo.dir
  const agentProfile =
    (liveStatus?.agent_profile as string | undefined) ?? runInfo.agentProfile

  const followUpLabel = args.label ?? `${runInfo.label}-followup`

  const followUpRun = await sup.spawn({
    agent,
    backend,
    dir,
    agent_profile: agentProfile,
    prompt: args.message,
    session_id: sessionId,
    label: followUpLabel,
  })

  return { run_id: followUpRun.runId, label: followUpRun.label }
}
