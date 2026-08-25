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
  const { client, isSingleton } = await getSupervisorClient(args.supervisorId)
  try {

    const sessions = await client.brokerPeers()
    return { peers: sessions as PeerEntry[] }

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
