import { matchPeerRows, type MessageProvider, type ProviderResult, ToolError } from '@dougbots/avenor-core'

/** Compose independent authorities without treating equal IDs as proof of equal identity. */
export function combinePeerProviders(localSource: MessageProvider | (() => MessageProvider | null), central: MessageProvider): MessageProvider {
  const local = () => typeof localSource === 'function' ? localSource() : localSource
  const providers = () => {
    const host = local()
    return host && host !== central ? [host, central] : [central]
  }
  const authorities = new WeakMap<MessageProvider, number>()
  let nextAuthority = 0
  function authority(provider: MessageProvider): number {
    let id = authorities.get(provider)
    if (id === undefined) { id = nextAuthority++; authorities.set(provider, id) }
    return id
  }
  // Both maps are keyed by something a caller can only act on soon after it was
  // issued — a resolved route is used by the call that resolved it, and a message
  // id is withdrawn within its ask window. Neither has a natural expiry, so each
  // keeps the most recent bindings and forgets the rest: an entry old enough to be
  // evicted names a route or an edge no live caller still holds.
  const ROUTE_MEMORY = 1024
  const routes = new Map<string, { provider: MessageProvider; id: string; publicId?: string }>()
  const outgoing = new Map<string, MessageProvider | null>()
  function remember<K, V>(memory: Map<K, V>, key: K, value: V) {
    memory.set(key, value)
    if (memory.size > ROUTE_MEMORY) {
      const oldest = memory.keys().next()
      if (!oldest.done) memory.delete(oldest.value)
    }
  }
  const claims = new Map<string, MessageProvider>()
  const resumed = new WeakMap<ProviderResult, MessageProvider>()

  function route(key: string) {
    const found = routes.get(key)
    if (!found) throw new ToolError('unknown_target', 'the resolved peer route is no longer available')
    return found
  }

  async function rememberProvider(provider: MessageProvider, result: Promise<ProviderResult>) {
    const answer = await result
    if (answer.messageId) {
      const previous = outgoing.get(answer.messageId)
      remember(outgoing, answer.messageId, previous === undefined || previous === provider ? provider : null)
    }
    return answer
  }

  return {
    ...central,
    surface: () => 'pi-host',
    role: () => 'host',
    onResumed: result => resumed.get(result)?.onResumed?.(result),
    registerResumed: result => resumed.get(result)?.registerResumed?.(result) ?? Promise.resolve(),
    caller: () => ({ kind: 'owner', runId: central.caller().runId }),
    async peers() {
      const results = await Promise.all(providers().map(p => p.peers()))
      return { peers: results.flatMap(r => r.peers), detail: results.map(r => r.detail).filter(Boolean).join('; ') || undefined }
    },
    async resolve(ref) {
      const snapshot = providers()
      const host = snapshot.find(provider => provider !== central)
      const results = await Promise.all(snapshot.map(p => p.peers()))
      if (results.some(r => r.detail)) throw new Error('the peer listing is incomplete: ' + results.map(r => r.detail).filter(Boolean).join('; '))
      const rows = results.flatMap((r, i) => r.peers.map(peer => ({ run_id: peer.run_id, label: peer.label, provider: snapshot[i] })))
      // The host's own runs are not rows in its own listing, so its own match rule
      // is the only thing that can name them. A public id it answers for joins the
      // merged rows as the one row those runs contribute.
      const own = host ? await host.resolve(ref) : undefined
      const publicResolution = own?.match === 'unique' && own.publicRunId === ref ? own : undefined
      if (publicResolution && host && !rows.some(row => row.provider === host && row.run_id === ref)) {
        rows.push({ run_id: ref, label: publicResolution.label, provider: host })
      }
      const { resolution: matched, row: hit } = matchPeerRows(rows, ref)
      if (matched.match !== 'unique' || !hit) return matched
      // The rule already answered from the row, which came from this same fetch.
      // Only the host can add a field the listing does not carry — the public run
      // id a caller reports — so only a host row is worth a second lookup. A
      // central row costs nothing more, and cannot fail as "changed during
      // resolution" for a peer that retired between two reads of one snapshot.
      const resolved = hit.provider === host
        ? publicResolution && hit.run_id === ref ? publicResolution : await hit.provider.resolve(hit.run_id)
        : matched
      if (resolved.match !== 'unique') throw new Error('the peer changed during resolution')
      const key = JSON.stringify([authority(hit.provider), resolved.runId, resolved.publicRunId])
      remember(routes, key, { provider: hit.provider, id: resolved.runId, publicId: resolved.publicRunId })
      return { ...resolved, routeKey: key, publicRunId: resolved.publicRunId ?? hit.run_id }
    },
    controlReply: (target, payload) => {
      const host = local()
      if (!host?.controlReply) throw new ToolError('not_permitted', 'no local supervisor accepts control replies')
      return host.controlReply(target, payload)
    },
    state: (key) => { const r = route(key); return r.provider.state(r.id, r.publicId) },
    tell: (key, message) => { const r = route(key); return r.provider.tell(r.id, message, r.publicId) },
    ask: (key, message, timeout, signal) => {
      const r = route(key)
      return rememberProvider(r.provider, r.provider.ask(r.id, message, timeout, signal, r.publicId))
    },
    async last(key) {
      const r = route(key)
      const result = await r.provider.last(r.id, r.publicId)
      return { ...result, runtimeId: result.runtimeId ?? r.id }
    },
    async resume(req) {
      const r = route(req.target)
      const result = await r.provider.resume({ ...req, target: r.id })
      resumed.set(result, r.provider)
      return result
    },
    rename: req => {
      if (!req.sessionId || req.sessionId === central.caller().runId) return central.rename(req)
      const host = local()
      if (!host) throw new ToolError('not_permitted', 'no local supervisor owns that session')
      return host.rename(req)
    },
    withdraw: id => {
      const owner = outgoing.get(id)
      if (!owner) throw new ToolError('not_permitted', 'this session does not own that outbound message')
      return owner.withdraw(id)
    },
    claimAsk(id) {
      const found = providers().flatMap(provider => {
        const asker = provider.claimAsk?.(id)
        return asker === undefined ? [] : [{ provider, asker }]
      })
      if (found.length !== 1) {
        for (const hit of found) hit.provider.restoreAsk?.(id, hit.asker)
        if (found.length > 1) throw new ToolError('not_permitted', 'multiple identities own that inbound message')
        return undefined
      }
      claims.set(id, found[0].provider)
      return found[0].asker
    },
    restoreAsk(id, asker) { claims.get(id)?.restoreAsk?.(id, asker); claims.delete(id) },
    releaseAsk(id) { claims.get(id)?.releaseAsk?.(id); claims.delete(id) },
    reply(id, message, asker) {
      const owner = claims.get(id)
      if (!owner) {
        // No local claim: the central broker routes a reply to a tell back to
        // its sender, and reports edge-absent when no route exists.
        return central.reply(id, message, asker)
      }
      return owner.reply(id, message, asker)
    },
  }
}
