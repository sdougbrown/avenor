import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface BrokerReplyArgs {
  toRunId: string
  replyTo: string
  message: string
  supervisorId?: string
}

export interface BrokerReplyResult {
  ok: true
}

async function executeBrokerReply(
  args: BrokerReplyArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<BrokerReplyResult> {
  const { client, isSingleton } = await getSupervisorClient(args.supervisorId)
  try {
    await client.brokerReply(args.toRunId, args.replyTo, args.message)
    return { ok: true }
  } finally {
    if (!isSingleton) {
      client.close()
    }
  }
}

export function createBrokerReplyTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: BrokerReplyArgs) => Promise<BrokerReplyResult> {
  return args => executeBrokerReply(args, getSupervisorClient)
}

export const brokerReplyTool = createBrokerReplyTool(realGetSupervisorClient)
