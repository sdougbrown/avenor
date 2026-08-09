import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface AskToolArgs {
  toRunId: string
  message: string
  supervisorId?: string
}

export interface AskResult {
  reply: string
  from_run_id: string
}

async function brokerPost(brokerUrl: string, path: string, body: Record<string, unknown>): Promise<Record<string, unknown>> {
  body.run_id = body.run_id ?? 'external'
  body.token = body.token ?? 'external'
  const res = await fetch(`${brokerUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText)
    throw new Error(`broker ${path}: ${res.status} ${text}`)
  }
  return res.json() as Promise<Record<string, unknown>>
}

function makeMsgID(): string {
  const b = new Uint8Array(8)
  crypto.getRandomValues(b)
  return Array.from(b).map(x => x.toString(16).padStart(2, '0')).join('')
}

function extractReplyMessage(result: Record<string, unknown>): string {
  if (result.timeout === true) return '(timeout: no reply received)'
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
    let brokerUrl = sup?.brokerUrl

    if (!brokerUrl) {
      if (isSingleton || !sup) {
        throw new Error('no broker URL available — ask requires a connected supervisor with an active broker')
      }
      throw new Error('broker URL not found for supervisor')
    }

    const msgID = makeMsgID()

    // Send the ask
    await brokerPost(brokerUrl, '/send', {
      from_run_id: 'avenor-ask-tool',
      to_run_id: args.toRunId,
      type: 'agent_message',
      payload: {
        id: msgID,
        from: 'avenor-ask-tool',
        from_run_id: 'avenor-ask-tool',
        to_run_id: args.toRunId,
        message: args.message,
        role: 'agent',
        expects_reply: true,
      },
    })

    // Wait for the reply
    const replyResult = await brokerPost(brokerUrl, '/wait_reply', {
      waiting_for: msgID,
    })

    return {
      reply: extractReplyMessage(replyResult),
      from_run_id: (replyResult.from_run_id as string) ?? '',
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
