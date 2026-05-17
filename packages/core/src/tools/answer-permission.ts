import { Supervisor } from '../supervisor.js'
import { dial } from '../client.js'

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
    client = await dial(args.supervisorId)
  } else {
    sup = await Supervisor.get()
    client = sup.getClient()
  }

  try {
    if (!requestId) {
      const liveStatus = await client.status(args.runId)
      const pp = liveStatus?.pending_permission as
        | { request_id?: string }
        | undefined
      if (!pp?.request_id) {
        throw new Error('no pending permission request for this run')
      }
      requestId = pp.request_id
    }

    await client.answerPermission(args.runId, requestId, args.optionId)

    return { ok: true }
  } finally {
    if (args.supervisorId) {
      client.close()
    }
  }
}
