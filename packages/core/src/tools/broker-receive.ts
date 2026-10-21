import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface BrokerReceiveArgs {
  supervisorId?: string
  /** The generation this poll is bound to; absent asks the tool to preflight once. */
  generation?: string
}

export interface InboundAsk {
  from_run_id: string
  message_id: string
  message: string
  role?: string
}

export interface BrokerReceiveResult {
  asks: InboundAsk[]
  /** The generation the receive ran under, so the caller can bind its next poll. */
  generation?: string
}

async function executeBrokerReceive(
  args: BrokerReceiveArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<BrokerReceiveResult> {
  const { client, isSingleton } = await getSupervisorClient(args.supervisorId)
  try {
    let generation = args.generation ?? ''
    if (!generation) {
      generation = (await client.messagePreflight()).generation
    }
    const received = await client.messageReceive({ generation })
    const asks = received.messages.map(message => {
      const payload = message.payload !== null && typeof message.payload === 'object'
        ? message.payload as Record<string, unknown>
        : undefined
      return {
        from_run_id: message.from_run_id,
        message_id: message.message_id,
        message: typeof payload?.message === 'string' ? payload.message : '',
        role: typeof payload?.role === 'string' ? payload.role : undefined,
      }
    })
    return { asks, generation: received.generation || generation }
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
