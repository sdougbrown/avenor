import type { Client } from '../client.js'
import { matchPeerRows, peerFacts, peerRecords } from './message-provider.js'
import type { PeerRecord } from '../message.js'
import type { StatusResult } from './status.js'

export const BROKER_PEER_NO_EVENTS =
  'this run is a broker peer with no event log; peers action last reads its last agent message'

export type BrokerPeerLookup =
  | { match: 'unique'; peer: PeerRecord }
  | { match: 'ambiguous'; candidates: string[] }
  | { match: 'none' }

/**
 * Resolve ref against the broker's peer listing, the one `peers list` reads: an
 * exact run id, else a label. A run a plugin session registered lives only
 * there and has no runtime on the supervisor. It answers one peer, several
 * peers that share the name, or none. A listing that cannot be read or is
 * partial answers none, so the caller keeps its own failure.
 */
export async function lookupBrokerPeer(
  client: Pick<Client, 'messagePreflight' | 'messagePeers'>,
  ref: string,
): Promise<BrokerPeerLookup> {
  try {
    const { generation } = await client.messagePreflight()
    const listing = await client.messagePeers({ generation })
    if (listing.peers_error) return { match: 'none' }
    const { resolution, row } = matchPeerRows(peerRecords(listing), ref)
    if (resolution.match === 'unique' && row) return { match: 'unique', peer: row }
    if (resolution.match === 'ambiguous') return { match: 'ambiguous', candidates: resolution.candidates ?? [] }
    return { match: 'none' }
  } catch {
    return { match: 'none' }
  }
}

/**
 * The status a broker peer can answer: its identity and the status `peers list`
 * shows for it, translated the way every peer row is. It has no runtime, phase
 * or event log to report, and says so, so a caller that harvests output does not
 * read a log or a runtime that is not there.
 */
export function brokerPeerStatus(peer: PeerRecord): StatusResult {
  return {
    run_id: peer.run_id,
    label: peer.label || peer.run_id,
    status: peerFacts(peer).status,
    runtime_output_available: false,
    ...(peer.backend && { backend: peer.backend }),
    ...(peer.model && { model: peer.model }),
    ...(peer.dir && { dir: peer.dir }),
  }
}
