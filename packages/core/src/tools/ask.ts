import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { findExternalRun } from './run-registry.js'
import { findLocalRunByReference } from './run-resolution.js'
import { RpcError } from '../client.js'
import { extractReplyMessage, normalizeAskTimeoutMs } from '../messaging-text.js'

export interface AskToolArgs {
  toRunId: string
  message: string
  supervisorId?: string
  timeoutMs?: number
}

export interface AskResult {
  reply: string
  from_run_id: string
}

export { extractReplyMessage, normalizeAskTimeoutMs }

/**
 * Build the failure message every ask path reports: the broker message id plus
 * whether the ask edge is still pending (withdrawable via avenor_cancel) or
 * already gone.
 */
export function formatAskFailure(messageId: string, pending: boolean, detail: string): string {
  return pending
    ? `ask failed (message_id: ${messageId} — pass to avenor_cancel to withdraw): ${detail}`
    : `ask failed (message_id: ${messageId} — the pending ask was already withdrawn): ${detail}`
}

async function executeAskTool(
  args: AskToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<AskResult> {
  const { client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId, { owner: true })
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
    const timeoutMs = normalizeAskTimeoutMs(args.timeoutMs)
    try {
      const result = await client.brokerAsk(target, args.message, undefined, timeoutMs)
      return {
        reply: extractReplyMessage(result),
        from_run_id: (result.from_run_id as string) ?? '',
      }
    } catch (error) {
      // A failed ask reports its message_id and whether the ask edge is still
      // pending on the broker (withdrawable via avenor_cancel) or already
      // withdrawn. Surface the matching guidance.
      if (error instanceof RpcError && error.data && typeof error.data === 'object') {
        const data = error.data as Record<string, unknown>
        const messageId = data.message_id
        if (typeof messageId === 'string' && messageId) {
          // pending === true, or absent/unknown (legacy shape): the ask edge
          // may still exist, so the caller can withdraw it.
          throw new Error(formatAskFailure(messageId, data.pending !== false, error.message))
        }
      }
      throw error
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
