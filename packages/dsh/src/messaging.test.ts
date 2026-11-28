import { describe, it, expect } from 'bun:test'
import { createMessagingHost, HOST_RUN_ID, type MessagingClient } from './messaging.js'

function fakeClient() {
  const calls: string[] = []
  const client = {
    async messagePreflight() {
      calls.push('preflight')
      return { protocol: 1, generation: 'gen-1' }
    },
    async messagePeers() {
      calls.push('peers')
      return {
        generation: 'gen-1',
        peers: [{ run_id: 'run-1', label: 'one', status: 'running' }],
      }
    },
  } as unknown as MessagingClient
  return { client, calls }
}

describe('messaging host', () => {
  it('bootstraps the broker generation and answers as the supervisor', async () => {
    const { client, calls } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })

    expect(host.hostRunId).toBe(HOST_RUN_ID)
    expect(HOST_RUN_ID).toBe('supervisor')
    expect(calls).toEqual(['preflight'])
  })

  it('lists peers through the control client', async () => {
    const { client } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })

    const result = await host.peers({ action: 'list' })
    expect(JSON.stringify(result)).toContain('run-1')
  })

  it('refuses an unknown action through the shared router', async () => {
    const { client } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })

    await expect(host.message({ action: 'bogus' })).rejects.toThrow()
  })
})
