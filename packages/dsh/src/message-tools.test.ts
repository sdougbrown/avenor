import { describe, it, expect } from 'bun:test'
import {
  MESSAGE_TOOL_NAME,
  PEERS_TOOL_NAME,
  messageToolDefinition,
  peersToolDefinition,
  sessionIdOf,
} from './message-tools.js'
import type { MessagingSurface } from './messaging.js'

function fakeSurface() {
  const calls: Array<{ tool: string; args: unknown; sessionId: string | undefined }> = []
  const surface: MessagingSurface = {
    async message(args, sessionId) {
      calls.push({ tool: 'message', args, sessionId })
      return { outcome: 'sent', text: 'delivered' }
    },
    async peers(args, sessionId) {
      calls.push({ tool: 'peers', args, sessionId })
      return { outcome: 'listed', text: 'run-1 one' }
    },
  }
  return { surface, calls }
}

const exec = { agent: { session: { id: 'sess-1' } } }

describe('avenor_message tool', () => {
  it('publishes the shared flat schema, action required', () => {
    const tool = messageToolDefinition(fakeSurface().surface)
    expect(tool.name).toBe(MESSAGE_TOOL_NAME)
    expect(tool.parameters.required).toContain('action')
    expect(Object.keys(tool.parameters.properties)).toContain('target')
    expect(tool.description).toContain('supervisor')
  })

  it('routes the call to the caller session and renders its text', async () => {
    const { surface, calls } = fakeSurface()
    const tool = messageToolDefinition(surface)

    const result = await tool.execute({ action: 'tell', target: 'run-1', message: 'hi' }, exec)
    expect(calls).toEqual([
      { tool: 'message', args: { action: 'tell', target: 'run-1', message: 'hi' }, sessionId: 'sess-1' },
    ])
    expect(result).toEqual({ outcome: 'sent', text: 'delivered' })
    expect(tool.output.render({}, result)[0]).toEqual({ type: 'text', text: 'delivered' })
  })

  it('falls back to the outcome when the surface sends no text', () => {
    const tool = messageToolDefinition(fakeSurface().surface)
    expect(tool.output.render({}, { outcome: 'sent' })[0]).toEqual({
      type: 'text',
      text: 'message sent',
    })
  })

  it('reads the session from the executor context only', () => {
    expect(sessionIdOf(exec)).toBe('sess-1')
    expect(sessionIdOf(undefined)).toBeUndefined()
    expect(sessionIdOf({})).toBeUndefined()
  })
})

describe('avenor_peers tool', () => {
  it('publishes the peers schema and routes by session', async () => {
    const { surface, calls } = fakeSurface()
    const tool = peersToolDefinition(surface)
    expect(tool.name).toBe(PEERS_TOOL_NAME)
    expect(Object.keys(tool.parameters.properties)).toContain('action')

    const result = await tool.execute({ action: 'list' }, exec)
    expect(calls).toEqual([{ tool: 'peers', args: { action: 'list' }, sessionId: 'sess-1' }])
    expect(tool.output.render({}, result)[0]).toEqual({ type: 'text', text: 'run-1 one' })
  })
})
