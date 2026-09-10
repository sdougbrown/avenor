import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { findExternalRun } from './run-registry.js'
import { findLocalRunByReference } from './run-resolution.js'

export interface AskToolArgs {
  toRunId: string
  message: string
  supervisorId?: string
}

export interface AskResult {
  reply: string
  from_run_id: string
}

function extractReplyMessage(result: Record<string, unknown>): string {
  if (result.timeout === true) return '(timeout: no reply received)'
  if (result.cancelled === true) return '(cancelled)'
  const payload = result.payload
  if (payload && typeof payload === 'object') {
    const msg = (payload as Record<string, unknown>).message
    if (typeof msg === 'string' && msg) return msg
  }
  return JSON.stringify(result)
}

async function executeAskTool(
  args: AskToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<AskResult> {
  const { client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId)
  try {
    // Resolve the caller's reference (public run ID, label, or broker runtime
    // ID) to the broker's runtime ID, mirroring the sibling tools. The broker
    // registers runs only under their internal runtime ID; addressing it by the
    // canonical spawn ID or a label would otherwise 404 "to run not found" even
    // for a fully active run.
    const runInfo = isSingleton && sup
      ? findLocalRunByReference(sup, args.toRunId)
      : findExternalRun(supervisorId, args.toRunId)
    const target = runInfo?.runtimeId ?? args.toRunId
    const result = await client.brokerAsk(target, args.message)
    return {
      reply: extractReplyMessage(result),
      from_run_id: (result.from_run_id as string) ?? '',
    }
  } finally {
    if (!isSingleton) {
      client.close()
    }
  }
}

export function createAskTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: AskToolArgs) => Promise<AskResult> {
  return args => executeAskTool(args, getSupervisorClient)
}

export const askTool = createAskTool(realGetSupervisorClient)
