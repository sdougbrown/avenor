import { executePeers, type PeersInput, type PeersRouterResult } from '../message.js'
import type { MessageProvider } from './message.js'

export interface PeersToolArgs extends PeersInput {}

export type PeersResult = PeersRouterResult

/** The one avenor_peers handler; it resolves and reads through the bound provider.
 *
 * Admission belongs to the shared router for every surface, so the argument is
 * the untyped JSON a tool call carries and `executePeers` validates it first —
 * the same shape `messageTool` takes. */
export async function peersTool(args: unknown, provider: MessageProvider): Promise<PeersResult> {
  return executePeers(provider, args)
}

/** Bind the peers handler to one provider. */
export function createPeersTool(
  provider: MessageProvider,
): (args: unknown) => Promise<PeersResult> {
  return args => peersTool(args, provider)
}
