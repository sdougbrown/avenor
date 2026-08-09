import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface PeersToolArgs {
  supervisorId?: string
}

export interface PeerEntry {
  run_id: string
  label?: string
  status?: string
  backend?: string
  model?: string
  dir?: string
}

export interface PeersResult {
  peers: PeerEntry[]
}

async function executePeersTool(
  args: PeersToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<PeersResult> {
  const { client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId)
  try {
    if (!sup?.brokerUrl) {
      return { peers: [] }
    }

    const res = await fetch(`${sup.brokerUrl}/sessions`)
    if (!res.ok) {
      return { peers: [] }
    }

    const body = await res.json() as { sessions?: PeerEntry[] }
    return { peers: body.sessions ?? [] }
  } finally {
    if (!isSingleton) {
      client.close()
    }
  }
}

export function createPeersTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: PeersToolArgs) => Promise<PeersResult> {
  return args => executePeersTool(args, getSupervisorClient)
}

export const peersTool = createPeersTool(realGetSupervisorClient)
