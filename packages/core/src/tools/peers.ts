import { executePeers, type PeersInput, type PeersRouterResult } from '../message.js'
import type { MessageProvider } from './message.js'

export interface PeersToolArgs extends PeersInput {}

export type PeersResult = PeersRouterResult

/** The one avenor_peers handler; it resolves and reads through the bound provider. */
export async function peersTool(args: PeersToolArgs, provider: MessageProvider): Promise<PeersResult> {
  return executePeers(provider, args)
}

/** Bind the peers handler to one provider. */
export function createPeersTool(
  provider: MessageProvider,
): (args: PeersToolArgs) => Promise<PeersResult> {
  return args => peersTool(args, provider)
}
