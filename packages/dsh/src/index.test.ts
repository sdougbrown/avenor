import { describe, it, expect } from 'bun:test'
import { apply, inject, register, type HostConfig } from './index.js'
import type { AvenorSeam } from './provider.js'
import type { MessagingHost, MessagingRuntime } from './messaging.js'
import type { HostContext, ParentAgent, SubagentProvider } from './types.js'

function fakeContext() {
  const providers: SubagentProvider[] = []
  const tools: unknown[] = []
  const effects: Array<() => void> = []
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
    effect(scope) {
      effects.push((scope() ?? (() => {})) as () => void)
      return () => {}
    },
  }
  return { ctx, providers, tools, effects }
}

function recordingSeamFactory() {
  const requests: unknown[] = []
  const seam: AvenorSeam = {
    async spawn() {
      throw new Error('not used in this test')
    },
  }
  return {
    requests,
    makeSeam(selection: unknown) {
      requests.push(selection)
      return seam
    },
  }
}

function fakeRuntime() {
  const calls: string[] = []
  const parents = new Map<string, ParentAgent>()
  const host: MessagingHost = {
    hostRunId: 'supervisor',
    async message() {
      return { outcome: 'sent' }
    },
    async peers() {
      return { outcome: 'listed' }
    },
  }
  const runtime: MessagingRuntime = {
    host: async () => host,
    async bind(sessionId) {
      calls.push(`bind:${sessionId}`)
      return { sessionId, runId: 'bound-1', brokerUrl: 'http://127.0.0.1:1', token: 'tok' }
    },
    remember(runId, parent) {
      calls.push(`remember:${runId}`)
      parents.set(runId, parent)
    },
    forget(runId) {
      calls.push(`forget:${runId}`)
      parents.delete(runId)
    },
    parentFor: (runId) => parents.get(runId),
    registerAsk(messageId) {
      calls.push(`ask:${messageId}`)
    },
    ensureInbound(sessionId) {
      calls.push(`inbound:${sessionId}`)
    },
    stop() {
      calls.push('stop')
    },
  }
  return { runtime, calls }
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
    register(fake.ctx, {}, {
      answer: { answer: async () => ({ ok: true }) },
      makeSeam: seams.makeSeam,
      runtime: fakeRuntime().runtime,
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
    const config: HostConfig = { backend: 'dsh', model: 'grace/qwen' }

    register(fake.ctx, config, {
      answer: { answer: async () => ({ ok: true }) },
      makeSeam: seams.makeSeam,
      runtime: fakeRuntime().runtime,
    })

    expect(seams.requests).toEqual([{ backend: 'dsh', model: 'grace/qwen' }])
  })

  it('omits unset selection fields rather than passing empty strings', () => {
    const fake = fakeContext()
    const seams = recordingSeamFactory()
    register(fake.ctx, { agent: 'planner' }, {
      answer: { answer: async () => ({ ok: true }) },
      makeSeam: seams.makeSeam,
      runtime: fakeRuntime().runtime,
    })
    expect(seams.requests).toEqual([{ agent: 'planner' }])
  })

  it('publishes output schemas the tool registry accepts', () => {
    const fake = fakeContext()
    register(fake.ctx, {}, {
      answer: { answer: async () => ({ ok: true }) },
      makeSeam: recordingSeamFactory().makeSeam,
      runtime: fakeRuntime().runtime,
    })
    // tools.register asserts the raw JSON Schema subset; an authoring-only type
    // such as 'json' throws there and takes the plugin's other tools with it.
    const allowed = ['object', 'array', 'string', 'number', 'integer', 'boolean', 'null', undefined]
    for (const tool of fake.tools as Array<{ name: string; output: { schema: { type?: string } } }>) {
      expect(allowed).toContain(tool.output.schema.type)
    }
  })

  it('registers everything from apply and stops the runtime on unload', () => {
    const fake = fakeContext()
    const parts = fakeRuntime()
    apply(fake.ctx, {}, { runtime: parts.runtime })

    expect(fake.providers).toHaveLength(1)
    expect(fake.tools).toHaveLength(3)
    expect(parts.calls).toEqual([])

    for (const dispose of fake.effects) dispose()
    expect(parts.calls).toEqual(['stop'])
  })
})
