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
  const { client, isSingleton, sup } = await getSupervisorClient(args.supervisorId)
  try {
    if (!sup?.brokerUrl) {
      return { peers: [] }
    }

    // /sessions requires broker auth. A worker Pi session's parent is the
    // supervisor, whose broker run id is "supervisor" and whose token is
    // parentToken (EnsureRun(ParentRunID)). Reuse those to authenticate.
    const parentToken = sup.parentToken
    if (!sup.brokerUrl || !parentToken) {
      // No broker credential available; peers is unavailable until the
      // control-proxy path carries broker auth.
      return { peers: [] }
    }
    const qs = `?run_id=${encodeURIComponent('supervisor')}&token=${encodeURIComponent(parentToken)}`
    const res = await fetch(sup.brokerUrl + '/sessions' + qs)
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
