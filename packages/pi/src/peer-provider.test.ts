import { expect, test } from 'bun:test'
import { createChildMessageProvider, type MessageProvider, type ChildBroker } from '@dougbots/avenor-core'
import { combinePeerProviders } from './peer-provider.js'

function provider(id: string, rows: Array<{ run_id: string; label?: string }>, sent: string[], detail?: string): MessageProvider {
  const broker = {
    peers: async () => ({ peers: rows, detail }),
    tell: async (target: string) => { sent.push(`${id}:${target}`); return { outcome: 'sent' as const } },
    ask: async () => ({ outcome: 'timed-out', messageId: `${id}-ask` }),
    reply: async () => ({ outcome: 'replied' }),
    withdraw: async messageId => { sent.push(`${id}:withdraw:${messageId}`); return { outcome: 'withdrawn' } },
    rename: async () => ({ outcome: 'renamed' }),
    last: async () => ({ outcome: 'sent' }),
  } satisfies ChildBroker
  return createChildMessageProvider({ surface: 'pi-child', runId: id, broker })
}

test('combined names resolve across providers and exact IDs take precedence', async () => {
  const sent: string[] = []
  const p = combinePeerProviders(provider('local', [{ run_id: 'child', label: 'scout' }], sent), provider('central', [{ run_id: 'remote', label: 'child' }], sent))
  const r = await p.resolve('child')
  await p.tell(r.routeKey ?? r.runId, 'hello', r.publicRunId)
  expect(sent).toEqual(['local:child'])
  expect((await p.resolve('scout')).match).toBe('unique')
})

test('duplicate names and IDs remain ambiguous without identity equivalence', async () => {
  const sent: string[] = []
  const a = provider('local', [{ run_id: 'same', label: 'scout' }], sent)
  const b = provider('central', [{ run_id: 'same', label: 'scout' }], sent)
  const p = combinePeerProviders(a, b)
  expect((await p.resolve('same')).match).toBe('ambiguous')
  expect((await p.resolve('scout')).match).toBe('ambiguous')
  expect(sent).toEqual([])
})

test('a label match routes to the authority whose row carried that label', async () => {
  const sent: string[] = []
  // Both authorities list the same run id under different labels, so the id alone
  // does not name which of them the label picked.
  const p = combinePeerProviders(
    provider('local', [{ run_id: 'X', label: 'bar' }], sent),
    provider('central', [{ run_id: 'X', label: 'foo' }], sent),
  )
  const r = await p.resolve('foo')
  expect(r.match).toBe('unique')
  // `runId` stays the authority's own id, so a consumer that reports it reports
  // something a reader can act on; only the route key is opaque.
  expect(r.runId).toBe('X')
  expect(r.routeKey).not.toBe(r.runId)
  await p.tell(r.routeKey ?? r.runId, 'hello')
  expect(sent).toEqual(['central:X'])
})

test('resume routes to the resolved authority, not to the caller-visible id', async () => {
  const local = provider('local', [{ run_id: 'rt_1', label: 'child' }], [])
  let resumedIn: string | undefined
  local.resume = async req => { resumedIn = `local:${req.target}`; return { outcome: 'sent', runId: 'rt_1' } }
  const p = combinePeerProviders(local, provider('central', [], []))
  const r = await p.resolve('child')
  expect(r.runId).toBe('rt_1')
  expect(r.routeKey).not.toBe(r.runId)
  await p.resume({ target: r.routeKey ?? r.runId, message: 'go' })
  expect(resumedIn).toBe('local:rt_1')
})

test('a central match resolves from the listing already fetched, without a second lookup', async () => {
  let listings = 0
  const broker = {
    // A second read of the same snapshot is the regression, so fail on one rather
    // than count fetches: an outcome a caller can see, not a mirrored mechanism.
    peers: async () => {
      listings++
      if (listings > 1) throw new Error('resolved the same target twice')
      return { peers: [{ run_id: 'remote', label: 'scout' }] }
    },
    tell: async () => ({ outcome: 'sent' as const }),
    ask: async () => ({ outcome: 'timed-out' }),
    reply: async () => ({ outcome: 'replied' }),
    withdraw: async () => ({ outcome: 'withdrawn' }),
    rename: async () => ({ outcome: 'renamed' }),
    last: async () => ({ outcome: 'sent' }),
  } satisfies ChildBroker
  const central = createChildMessageProvider({ surface: 'pi-child', runId: 'central', broker })
  const p = combinePeerProviders(() => null, central)
  const r = await p.resolve('scout')
  expect(r.runId).toBe('remote')
})

test('rename routes to the authority that owns the session', async () => {
  const sent: string[] = []
  const local = provider('local', [], sent)
  const central = provider('central', [], sent)
  central.rename = async req => { sent.push(`central:${req.sessionId}`); return { outcome: 'renamed', label: req.name } }
  local.rename = async req => { sent.push(`local:${req.sessionId}`); return { outcome: 'renamed', label: req.name } }

  const both = combinePeerProviders(local, central)
  // A run id with no session binding is the caller's own identity, so central owns it.
  await both.rename({ name: 'x' })
  expect(sent).toEqual(['central:undefined'])

  // A session this host runs is renamed through the host, which owns its record.
  await both.rename({ sessionId: 'ses_local', name: 'x' })
  expect(sent).toEqual(['central:undefined', 'local:ses_local'])

  // With no host provider bound there is nobody who can own another session.
  const centralOnly = combinePeerProviders(() => null, central)
  const refusal = (() => { try { centralOnly.rename({ sessionId: 'ses_other', name: 'x' }) } catch (error) { return error } })() as { code?: string }
  expect(refusal.code).toBe('not_permitted')
})

test('resume tracking follows the provider that executed the resume', async () => {
  const local = provider('local', [{ run_id: 'rt_1', label: 'child' }], [])
  const central = provider('central', [], [])
  const tracked: string[] = []
  local.onResumed = result => { tracked.push(`local:${result.runId}`) }
  local.registerResumed = async () => { tracked.push('local:registered') }
  central.onResumed = result => { tracked.push(`central:${result.runId}`) }
  central.registerResumed = async () => { tracked.push('central:registered') }
  local.resume = async req => ({ outcome: 'sent', runId: req.target })

  const p = combinePeerProviders(local, central)
  const r = await p.resolve('child')
  const resumed = await p.resume({ target: r.routeKey ?? r.runId, message: 'go' })
  p.onResumed?.(resumed)
  await p.registerResumed?.(resumed)
  expect(tracked).toEqual(['local:rt_1', 'local:registered'])
})

test('incomplete remote listings cannot fall back to a local match', async () => {
  const p = combinePeerProviders(provider('local', [{ run_id: 'child' }], []), provider('central', [], [], 'offline'))
  await expect(p.resolve('child')).rejects.toThrow('incomplete')
})


test('withdrawals retain the sending provider and refuse unknown ownership', async () => {
  const sent: string[] = []
  const p = combinePeerProviders(provider('local', [{ run_id: 'child' }], sent), provider('central', [{ run_id: 'remote' }], sent))
  const r = await p.resolve('remote')
  await p.ask(r.routeKey ?? r.runId, 'question')
  await p.withdraw('central-ask')
  expect(sent).toEqual(['central:withdraw:central-ask'])
  expect(() => p.withdraw('unknown')).toThrow('does not own')
})

test('transport errors refuse resolution even with an exact local ID', async () => {
  const remote = provider('central', [], [])
  remote.peers = async () => { throw new Error('connection lost') }
  const p = combinePeerProviders(provider('local', [{ run_id: 'child' }], []), remote)
  await expect(p.resolve('child')).rejects.toThrow('connection lost')
})

test('reply claims restore through their original provider', async () => {
  const local = provider('local', [], [])
  const central = provider('central', [], [])
  const pending = new Map([['ask', 'sender']])
  central.claimAsk = id => { const sender = pending.get(id); pending.delete(id); return sender }
  central.restoreAsk = (id, sender) => { pending.set(id, sender) }
  central.reply = async (id, message, sender) => ({ outcome: 'replied', reply: `${id}:${sender}:${message}` })
  const p = combinePeerProviders(local, central)
  expect(p.claimAsk?.('ask')).toBe('sender')
  p.restoreAsk?.('ask', 'sender')
  expect(p.claimAsk?.('ask')).toBe('sender')
  expect((await p.reply('ask', 'answer', 'sender')).reply).toBe('ask:sender:answer')
})

test('colliding inbound claims are restored without selecting an identity', () => {
  const local = provider('local', [], [])
  const central = provider('central', [], [])
  const restored: string[] = []
  for (const p of [local, central]) {
    p.claimAsk = () => p.caller().runId
    p.restoreAsk = (_id, sender) => { restored.push(sender) }
  }
  const p = combinePeerProviders(local, central)
  expect(() => p.claimAsk?.('same')).toThrow('multiple identities')
  expect(restored).toEqual(['local', 'central'])
})

test('a public local run ID remains addressable alongside central peers', async () => {
  const local = provider('local', [{ run_id: 'rt_1', label: 'child' }], [])
  local.resolve = async ref => ({ runId: 'rt_1', publicRunId: 'run-public', label: 'child', match: 'unique' })
  const p = combinePeerProviders(local, provider('central', [], []))
  expect((await p.resolve('run-public')).match).toBe('unique')
})

test('in-flight resolution keeps its authority when a local supervisor is replaced', async () => {
  const sent: string[] = []
  const first = provider('first', [{ run_id: 'child' }], sent)
  const second = provider('second', [{ run_id: 'child' }], sent)
  const listing = Promise.withResolvers<{ peers: Array<{ run_id: string }> }>()
  first.peers = () => listing.promise
  let active: MessageProvider | null = first
  const p = combinePeerProviders(() => active, provider('central', [], sent))
  const pending = p.resolve('child')
  active = second
  listing.resolve({ peers: [{ run_id: 'child' }] })
  const old = await pending
  const current = await p.resolve('child')
  await p.tell(old.routeKey ?? old.runId, 'old')
  await p.tell(current.routeKey ?? current.runId, 'new')
  expect(sent).toEqual(['first:child', 'second:child'])
})

test('peers last exposes actual central and local runtime IDs with public run identity intact', async () => {
  const { peersTool } = await import('@dougbots/avenor-core')
  const local = provider('local', [{ run_id: 'rt_1', label: 'child' }], [])
  local.resolve = async () => ({ runId: 'rt_1', publicRunId: 'run-public', label: 'child', match: 'unique' })
  const p = combinePeerProviders(local, provider('central', [{ run_id: 'central', label: 'self-name' }], []))
  const self = await peersTool({ action: 'last', target: 'self' }, p)
  expect(self.last?.runtime_id).toBe('central')
  expect(self.last?.run_id).toBe('central')
  const child = await peersTool({ action: 'last', target: 'child' }, p)
  expect(child.last?.runtime_id).toBe('rt_1')
  expect(child.last?.run_id).toBe('run-public')
})

test('state-denial text identifies the runtime instead of its internal routing key', async () => {
  const { messageTool } = await import('@dougbots/avenor-core')
  const local = provider('local', [{ run_id: 'rt_1', label: 'child' }], [])
  local.resolve = async () => ({ runId: 'rt_1', publicRunId: 'run-public', label: 'child', match: 'unique' })
  local.state = async () => ({ live: false, resumable: false, status: 'done' })
  const p = combinePeerProviders(local, provider('central', [], []))
  const error = await messageTool({ action: 'tell', target: 'child', message: 'hello' }, p).catch(error => error)
  expect(error.message).toContain('rt_1')
  expect(error.message).not.toContain('[0,')
})
