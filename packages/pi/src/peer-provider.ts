import { type MessageProvider, type ProviderResult, type Resolution, ToolError } from '@dougbots/avenor-core'

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
  const routes = new Map<string, { provider: MessageProvider; id: string; publicId?: string }>()
  const outgoing = new Map<string, MessageProvider | null>()
  const claims = new Map<string, MessageProvider>()
  const resumed = new WeakMap<ProviderResult, MessageProvider>()

  function route(key: string) {
    const found = routes.get(key)
    if (!found) throw new ToolError('unknown_target', 'the resolved peer route is no longer available')
    return found
  }

  async function remember(provider: MessageProvider, result: Promise<ProviderResult>) {
    const answer = await result
    if (answer.messageId) {
      const previous = outgoing.get(answer.messageId)
      outgoing.set(answer.messageId, previous === undefined || previous === provider ? provider : null)
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
      const rows = results.flatMap((r, i) => r.peers.map(peer => ({ peer, provider: snapshot[i] })))
      const exact = rows.filter(r => r.peer.run_id === ref)
      let publicResolution: Resolution | undefined
      if (host) {
        const candidate = await host.resolve(ref)
        if (candidate.match === 'unique' && candidate.publicRunId === ref) publicResolution = candidate
      }
      if (publicResolution && host && !exact.some(r => r.provider === host && r.peer.run_id === publicResolution.runId)) {
        exact.push({ peer: { run_id: ref, label: publicResolution.label }, provider: host })
      }
      const hits = exact.length ? exact : rows.filter(r => r.peer.label === ref)
      if (!hits.length) return { runId: '', match: 'none' }
      if (hits.length > 1) return { runId: '', match: 'ambiguous', candidates: hits.map(r => r.peer.run_id) }
      const hit = hits[0]
      const resolved = hit.provider === host && publicResolution && hit.peer.run_id === ref
        ? publicResolution : await hit.provider.resolve(hit.peer.run_id)
      if (resolved.match !== 'unique') throw new Error('the peer changed during resolution')
      const key = JSON.stringify([authority(hit.provider), resolved.runId, resolved.publicRunId])
      routes.set(key, { provider: hit.provider, id: resolved.runId, publicId: resolved.publicRunId })
      return { ...resolved, runId: key, publicRunId: resolved.publicRunId ?? hit.peer.run_id }
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
      return remember(r.provider, r.provider.ask(r.id, message, timeout, signal, r.publicId))
    },
    last: (key) => { const r = route(key); return r.provider.last(r.id, r.publicId) },
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
      if (!owner) throw new ToolError('not_permitted', 'this session does not own that inbound message')
      return owner.reply(id, message, asker)
    },
  }
}
