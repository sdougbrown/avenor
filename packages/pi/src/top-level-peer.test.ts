import { expect, test } from 'bun:test'
import { createTopLevelPeerEnv, registerTopLevelPeer } from './subagent-broker.js'

test('registration retries retain a chosen identity and never accept changed credentials', async () => {
  const env = createTopLevelPeerEnv('https://broker.test')
  const bodies: unknown[] = []
  const fetchImpl = (async (_url: unknown, init: RequestInit) => {
    bodies.push(JSON.parse(String(init.body)))
    if (bodies.length === 1) throw new Error('connection lost')
    return Response.json({ run_id: env.runId, token: env.token, peer: true })
  }) as typeof fetch
  await expect(registerTopLevelPeer(env, fetchImpl)).rejects.toThrow('connection lost')
  await registerTopLevelPeer(env, fetchImpl)
  expect(bodies[0]).toEqual(bodies[1])
  expect(bodies[0]).toMatchObject({ peer: true, run_id: env.runId, token: env.token })
  await expect(registerTopLevelPeer(env, Object.assign(async () => Response.json({ run_id: 'other', token: 'secret' }), { preconnect() {} }))).rejects.toThrow('identity')
})

test('broker URL refuses embedded credentials and query strings', () => {
  expect(() => createTopLevelPeerEnv('https://secret@broker.test')).toThrow()
  expect(() => createTopLevelPeerEnv('https://broker.test?token=secret')).toThrow()
})

test('a connected central broker that never answers cannot hang local target resolution', async () => {
  const { createChildMessageProvider } = await import('@dougbots/avenor-core')
  const { createBrokerChildBroker } = await import('./subagent-broker.js')
  const { combinePeerProviders } = await import('./peer-provider.js')
  const { makeMessageProvider } = await import('./test-fixtures.js')
  let aborted = false
  let sent = false
  const blackhole = Object.assign(async (_url: string | URL | Request, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
    if (!init?.signal) { reject(new Error('missing transport deadline')); return }
    const onAbort = () => { aborted = true; reject(init.signal!.reason) }
    if (init.signal.aborted) onAbort()
    else init.signal.addEventListener('abort', onAbort, { once: true })
  }), { preconnect() {} }) as typeof fetch
  const central = createChildMessageProvider({
    surface: 'pi-child', runId: 'central-self',
    broker: createBrokerChildBroker(createTopLevelPeerEnv('https://broker.test'), blackhole, { strictPeers: true, requestTimeoutMs: 10 }),
  })
  const local = makeMessageProvider({
    peers: async () => ({ peers: [{ run_id: 'local-child' }] }),
    tell: async () => { sent = true; return { outcome: 'sent' } },
  })
  const combined = combinePeerProviders(local, central)
  await expect(combined.resolve('local-child')).rejects.toThrow('timed out')
  expect(aborted).toBe(true)
  expect(sent).toBe(false)
})

test('central ask waits retain their caller budget beyond short-request deadlines', async () => {
  const { createBrokerChildBroker } = await import('./subagent-broker.js')
  const complete = AbortSignal.timeout(30)
  const transport = Object.assign(async (url: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const path = new URL(String(url)).pathname.slice('/avenor'.length)
    if (path === '/sessions') return Response.json({ generation: 'gen', sessions: [] })
    if (path === '/wait_reply') return new Promise<Response>((resolve, reject) => {
      const onAbort = () => reject(init?.signal?.reason)
      init?.signal?.addEventListener('abort', onAbort, { once: true })
      complete.addEventListener('abort', () => {
        init?.signal?.removeEventListener('abort', onAbort)
        resolve(Response.json({ outcome: 'replied', payload: { message: 'patient reply' } }))
      }, { once: true })
    })
    return Response.json({})
  }, { preconnect() {} }) as typeof fetch
  const broker = createBrokerChildBroker(createTopLevelPeerEnv('https://broker.test/avenor'), transport, { requestTimeoutMs: 5 })
  const result = await broker.ask('peer', 'question', 'ask-budget', 200)
  expect(result.outcome).toBe('replied')
  expect(result.reply).toBe('patient reply')
})
