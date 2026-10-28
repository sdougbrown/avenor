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
  await p.tell(r.runId, 'hello', r.publicRunId)
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

test('incomplete remote listings cannot fall back to a local match', async () => {
  const p = combinePeerProviders(provider('local', [{ run_id: 'child' }], []), provider('central', [], [], 'offline'))
  await expect(p.resolve('child')).rejects.toThrow('incomplete')
})


test('withdrawals retain the sending provider and refuse unknown ownership', async () => {
  const sent: string[] = []
  const p = combinePeerProviders(provider('local', [{ run_id: 'child' }], sent), provider('central', [{ run_id: 'remote' }], sent))
  const r = await p.resolve('remote')
  await p.ask(r.runId, 'question')
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
  await p.tell(old.runId, 'old')
  await p.tell(current.runId, 'new')
  expect(sent).toEqual(['first:child', 'second:child'])
})
