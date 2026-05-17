import { Supervisor } from '../supervisor.js'
import { dial } from '../client.js'
import { validateSupervisorSocketPath } from './validate.js'

function findRunByLabel(sup: Supervisor, runId: string): { label: string; runtimeId?: string } | undefined {
  const runs = (sup as any).runs as Map<string, { label: string; runtimeId?: string }>
  const byKey = runs.get(runId)
  if (byKey) return byKey
  for (const info of runs.values()) {
    if (info.label === runId) return info
  }
  return undefined
}

export async function answerPermissionTool(args: {
  runId: string
  optionId: string
  requestId?: string
  supervisorId?: string
}): Promise<{ ok: boolean }> {
  let client
  let sup: Supervisor | null = null
  let requestId = args.requestId

  if (args.supervisorId) {
    client = await dial(validateSupervisorSocketPath(args.supervisorId))
  } else {
    sup = await Supervisor.get()
    client = sup.getClient()
  }

  try {
    const runInfo = sup ? findRunByLabel(sup, args.runId) : undefined
    const runtimeId = runInfo?.runtimeId ?? args.runId
    if (!requestId) {
      const liveStatus = await client.status(runtimeId)
      const pp = liveStatus?.pending_permission as
        | { request_id?: string }
        | undefined
      if (!pp?.request_id) {
        throw new Error('no pending permission request for this run')
      }
      requestId = pp.request_id
    }

    await client.answerPermission(runtimeId, requestId, args.optionId)

    return { ok: true }
  } finally {
    if (args.supervisorId) {
      client.close()
    }
  }
}
