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
  const { client, isSingleton } = await getSupervisorClient(args.supervisorId)
  try {
    await client.brokerCancel(args.messageId)
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
