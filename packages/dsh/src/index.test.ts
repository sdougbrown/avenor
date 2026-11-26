import { describe, it, expect } from 'bun:test'
import { apply, inject, register } from './index.js'
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

describe('plugin entry', () => {
  it('waits for the subagent registry and the tool registry', () => {
    expect(inject).toEqual(['subagents', 'tools'])
  })

  it('registers the avenor provider and the answer tool', () => {
    const fake = fakeContext()
    register(fake.ctx, { answer: async () => ({ ok: true }) })

    expect(fake.providers.map((provider) => provider.name)).toEqual(['avenor'])
    expect(fake.providers[0].inheritsParentContext).toBe(false)
    expect(fake.tools).toHaveLength(1)
    expect((fake.tools[0] as { name: string }).name).toBe('avenor_answer_permission')
  })

  it('exposes an apply that registers both halves', () => {
    const fake = fakeContext()
    apply(fake.ctx)
    expect(fake.providers).toHaveLength(1)
    expect(fake.tools).toHaveLength(1)
  })
})
