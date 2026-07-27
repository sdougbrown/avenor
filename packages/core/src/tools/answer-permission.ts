import { Supervisor } from '../supervisor.js'
import { getSupervisorClient } from './get-supervisor-client.js'
import { asRecord, stringField } from '../value-fields.js'
import { validateRunId } from './validate.js'

function findRunByLabel(sup: Supervisor, runId: string): { label: string; runtimeId?: string } | undefined {
  const runs = (sup as any).runs as Map<string, { label: string; runtimeId?: string }>
  const byKey = runs.get(runId)
  if (byKey) return byKey
  for (const info of runs.values()) {
    if (info.label === runId) return info
  }
  return undefined
}

export function pendingPermissionRequestID(liveStatus: unknown): string | undefined {
  const status = asRecord(liveStatus)
  if (!status) return undefined
  const permission = asRecord(status.permission) ?? asRecord(status.pending_permission)
  return permission ? stringField(permission, 'request_id', 'requestId') : undefined
}

export async function answerPermissionTool(args: {
  runId: string
  optionId: string
  requestId?: string
  supervisorId?: string
  message?: string
}): Promise<{ ok: boolean }> {
  let client
  let sup: Supervisor | null = null
  let requestId = args.requestId
  let supervisorId: string | undefined
  let isSingleton = false

  if (args.supervisorId) {
    ({ client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId))
  } else {
    sup = await Supervisor.get()
    client = sup.getClient()
  }

  try {
    validateRunId(args.runId)
    const runInfo = sup ? findRunByLabel(sup, args.runId) : undefined
    const runtimeId = runInfo?.runtimeId ?? args.runId
    if (!requestId) {
      const liveStatus = await client.status(runtimeId)
      requestId = pendingPermissionRequestID(liveStatus)
      if (!requestId) {
        throw new Error('no pending permission request for this run')
      }
    }

    await client.answerPermission(runtimeId, requestId, args.optionId, args.message)

    return { ok: true }
  } finally {
    if (supervisorId && !isSingleton) {
      client.close()
    }
  }
}
