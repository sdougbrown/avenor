import { describe, it, expect } from 'bun:test'
import { apply, inject, register, type HostConfig } from './index.js'
import type { AvenorSeam } from './provider.js'
import type { MessagingHost } from './messaging.js'
import type { HostContext, SubagentProvider } from './types.js'

function fakeContext() {
  const providers: SubagentProvider[] = []
  const tools: unknown[] = []
  const ctx: HostContext = {
    subagents: {
      registerProvider(provider) {
        providers.push(provider)
        return () => {}
      },
    },
    tools: {
      register(definition) {
        tools.push(definition)
        return () => {}
      },
    },
  }
  return { ctx, providers, tools }
}

/** A seam that never runs anything, recording the request it was built from. */
function recordingSeamFactory() {
  const requests: unknown[] = []
  const seam: AvenorSeam = {
    async spawn() {
      throw new Error('not used in this test')
    },
  }
  return {
    requests,
    makeSeam(request: unknown) {
      requests.push(request)
      return seam
    },
  }
}

function fakeMessagingHost() {
  const calls: string[] = []
  const host: MessagingHost = {
    hostRunId: 'supervisor',
    async message() {
      calls.push('message')
      return { outcome: 'sent' }
    },
    async peers() {
      calls.push('peers')
      return { outcome: 'listed' }
    },
  }
  return { host, calls, getMessagingHost: async () => host }
}

const toolNames = (tools: unknown[]) =>
  tools.map((tool) => (tool as { name: string }).name)

describe('plugin entry', () => {
  it('waits for the subagent registry and the tool registry', () => {
    expect(inject).toEqual(['subagents', 'tools'])
  })

  it('registers the provider and the three tools', () => {
    const fake = fakeContext()
    const seams = recordingSeamFactory()
    const messaging = fakeMessagingHost()
    register(fake.ctx, {}, {
      answer: { answer: async () => ({ ok: true }) },
      makeSeam: seams.makeSeam,
      getMessagingHost: messaging.getMessagingHost,
    })

    expect(fake.providers.map((provider) => provider.name)).toEqual(['avenor'])
    expect(fake.providers[0].inheritsParentContext).toBe(false)
    expect(toolNames(fake.tools)).toEqual([
      'avenor_answer_permission',
      'avenor_message',
      'avenor_peers',
    ])
  })

  it('passes the row config through to the child selection', () => {
    const fake = fakeContext()
    const seams = recordingSeamFactory()
    const messaging = fakeMessagingHost()
    const config: HostConfig = {
      backend: 'dsh',
      model: 'grace/qwen',
      rosterEntry: 'reviewer',
      rosterFile: '/roster.json',
    }

    register(fake.ctx, config, {
      answer: { answer: async () => ({ ok: true }) },
      makeSeam: seams.makeSeam,
      getMessagingHost: messaging.getMessagingHost,
    })

    expect(seams.requests).toEqual([
      {
        selection: {
          backend: 'dsh',
          model: 'grace/qwen',
          rosterEntry: 'reviewer',
          rosterFile: '/roster.json',
        },
        messaging: false,
        parentRunId: 'supervisor',
      },
    ])
  })

  it('binds messaging to the supervisor by default when enabled', () => {
    const fake = fakeContext()
    const seams = recordingSeamFactory()
    const messaging = fakeMessagingHost()
    register(fake.ctx, { messaging: true }, {
      answer: { answer: async () => ({ ok: true }) },
      makeSeam: seams.makeSeam,
      getMessagingHost: messaging.getMessagingHost,
    })

    expect(seams.requests).toEqual([
      { selection: {}, messaging: true, parentRunId: 'supervisor' },
    ])
  })

  it('omits unset selection fields rather than passing empty strings', () => {
    const fake = fakeContext()
    const seams = recordingSeamFactory()
    const messaging = fakeMessagingHost()
    register(fake.ctx, { agent: 'planner' }, {
      answer: { answer: async () => ({ ok: true }) },
      makeSeam: seams.makeSeam,
      getMessagingHost: messaging.getMessagingHost,
    })
    expect((seams.requests[0] as { selection: unknown }).selection).toEqual({ agent: 'planner' })
  })

  it('registers everything from apply', () => {
    const fake = fakeContext()
    apply(fake.ctx)
    expect(fake.providers).toHaveLength(1)
    expect(fake.tools).toHaveLength(3)
  })
})
