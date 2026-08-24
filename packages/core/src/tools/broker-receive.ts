import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface BrokerReceiveArgs {
  supervisorId?: string
}

export interface InboundAsk {
  from_run_id: string
  message_id: string
  message: string
  role?: string
}

export interface BrokerReceiveResult {
  asks: InboundAsk[]
}

async function executeBrokerReceive(
  args: BrokerReceiveArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<BrokerReceiveResult> {
  const { client, isSingleton } = await getSupervisorClient(args.supervisorId)
  try {
    const asks = await client.brokerReceive()
    return { asks }
  } finally {
    if (!isSingleton) {
      client.close()
    }
  }
}

export function createBrokerReceiveTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: BrokerReceiveArgs) => Promise<BrokerReceiveResult> {
  return args => executeBrokerReceive(args, getSupervisorClient)
}

export const brokerReceiveTool = createBrokerReceiveTool(realGetSupervisorClient)
