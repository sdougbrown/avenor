import { describe, it, expect } from 'bun:test'
import {
  MESSAGE_TOOL_NAME,
  PEERS_TOOL_NAME,
  messageToolDefinition,
  peersToolDefinition,
} from './message-tools.js'
import type { MessagingHost } from './messaging.js'

function fakeHost() {
  const calls: Array<{ tool: string; args: unknown }> = []
  const host: MessagingHost = {
    hostRunId: 'supervisor',
    async message(args) {
      calls.push({ tool: 'message', args })
      return { outcome: 'sent', text: 'delivered' }
    },
    async peers(args) {
      calls.push({ tool: 'peers', args })
      return { outcome: 'listed', text: 'run-1 one' }
    },
  }
  return { host, calls }
}

describe('avenor_message tool', () => {
  it('publishes the shared flat schema, action required', () => {
    const { host } = fakeHost()
    const tool = messageToolDefinition(async () => host)
    expect(tool.name).toBe(MESSAGE_TOOL_NAME)
    expect(tool.parameters.required).toContain('action')
    expect(Object.keys(tool.parameters.properties)).toContain('target')
    expect(tool.description).toContain('supervisor')
  })

  it('delegates the call to the host and renders its text', async () => {
    const { host, calls } = fakeHost()
    // The host is built on first call, not at registration.
    let builds = 0
    const tool = messageToolDefinition(async () => {
      builds += 1
      return host
    })
    expect(builds).toBe(0)

    const result = await tool.execute({ action: 'tell', target: 'run-1', message: 'hi' })
    expect(calls).toEqual([{ tool: 'message', args: { action: 'tell', target: 'run-1', message: 'hi' } }])
    expect(result).toEqual({ outcome: 'sent', text: 'delivered' })
    expect(tool.output.render({}, result)[0]).toEqual({ type: 'text', text: 'delivered' })
    expect(builds).toBe(1)
  })

  it('falls back to the outcome when the host sends no text', () => {
    const tool = messageToolDefinition(async () => fakeHost().host)
    expect(tool.output.render({}, { outcome: 'sent' })[0]).toEqual({
      type: 'text',
      text: 'message sent',
    })
  })
})

describe('avenor_peers tool', () => {
  it('publishes the peers schema and delegates', async () => {
    const { host, calls } = fakeHost()
    const tool = peersToolDefinition(async () => host)
    expect(tool.name).toBe(PEERS_TOOL_NAME)
    expect(Object.keys(tool.parameters.properties)).toContain('action')

    const result = await tool.execute({ action: 'list' })
    expect(calls).toEqual([{ tool: 'peers', args: { action: 'list' } }])
    expect(tool.output.render({}, result)[0]).toEqual({ type: 'text', text: 'run-1 one' })
  })
})
