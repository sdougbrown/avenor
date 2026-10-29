import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface BrokerReceiveArgs {
  supervisorId?: string
  /** The generation this poll is bound to; absent asks the tool to preflight once. */
  generation?: string
  /** Bound on the receive wait; the caller owns it, so the tool adds no default. */
  timeout_ms?: number
}

export interface InboundAsk {
  from_run_id: string
  message_id: string
  message: string
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
    const received = await client.messageReceive({ generation, timeout_ms: args.timeout_ms })
    const asks = received.messages.map(message => {
      // The wire may carry the payload as a JSON string; a payload that does not
      // parse is treated as absent rather than as a message body.
      let raw = message.payload
      if (typeof raw === 'string') {
        try {
          raw = JSON.parse(raw)
        } catch {
          raw = undefined
        }
      }
      const payload = raw !== null && typeof raw === 'object'
        ? raw as Record<string, unknown>
        : undefined
      // Only the fields the store stamps. A payload's own role is the sender's
      // claim, and a reader that prints it is printing an authority nobody granted.
      return {
        from_run_id: message.from_run_id,
        message_id: message.message_id,
        message: typeof payload?.message === 'string' ? payload.message : '',
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
