import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface CancelToolArgs {
  messageId: string
  supervisorId?: string
}

export interface CancelResult {
  cancelled: boolean
}

async function executeCancelTool(
  args: CancelToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<CancelResult> {
  const { client, isSingleton, sup } = await getSupervisorClient(args.supervisorId)
  try {
    if (!sup?.brokerUrl) {
      throw new Error('no broker URL available')
    }

    const res = await fetch(`${sup.brokerUrl}/cancel_message`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        run_id: 'avenor-cancel-tool',
        token: 'external',
        cancel_message_id: args.messageId,
      }),
    })

    if (res.status === 404) {
      throw new Error(`no pending ask for message id "${args.messageId}"`)
    }
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText)
      throw new Error(`cancel_message: ${res.status} ${text}`)
    }

    return { cancelled: true }
  } finally {
    if (!isSingleton) {
      client.close()
    }
  }
}

export function createCancelTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: CancelToolArgs) => Promise<CancelResult> {
  return args => executeCancelTool(args, getSupervisorClient)
}

export const cancelTool = createCancelTool(realGetSupervisorClient)
