import { describe, it, expect } from 'bun:test'
import { apply, inject, register, type HostConfig } from './index.js'
import type { AvenorSeam, AvenorSelection } from './provider.js'
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

/** A seam that never runs anything, recording the selection it was built with. */
function recordingSeamFactory() {
  const selections: AvenorSelection[] = []
  const seam: AvenorSeam = {
    async spawn() {
      throw new Error('not used in this test')
    },
  }
  return {
    selections,
    makeSeam(selection: AvenorSelection) {
      selections.push(selection)
      return seam
    },
  }
}

describe('plugin entry', () => {
  it('waits for the subagent registry and the tool registry', () => {
    expect(inject).toEqual(['subagents', 'tools'])
  })

  it('registers the avenor provider and the answer tool', () => {
    const fake = fakeContext()
    const seams = recordingSeamFactory()
    register(fake.ctx, {}, { answer: async () => ({ ok: true }) }, seams.makeSeam)

    expect(fake.providers.map((provider) => provider.name)).toEqual(['avenor'])
    expect(fake.providers[0].inheritsParentContext).toBe(false)
    expect(fake.tools).toHaveLength(1)
    expect((fake.tools[0] as { name: string }).name).toBe('avenor_answer_permission')
  })

  it('passes the row config through to the child selection', () => {
    const fake = fakeContext()
    const seams = recordingSeamFactory()
    const config: HostConfig = {
      backend: 'dsh',
      model: 'grace/qwen',
      rosterEntry: 'reviewer',
      rosterFile: '/roster.json',
    }

    register(fake.ctx, config, { answer: async () => ({ ok: true }) }, seams.makeSeam)

    expect(seams.selections).toEqual([
      {
        backend: 'dsh',
        model: 'grace/qwen',
        rosterEntry: 'reviewer',
        rosterFile: '/roster.json',
      },
    ])
  })

  it('omits unset selection fields rather than passing empty strings', () => {
    const fake = fakeContext()
    const seams = recordingSeamFactory()
    register(fake.ctx, { agent: 'planner' }, { answer: async () => ({ ok: true }) }, seams.makeSeam)
    expect(seams.selections).toEqual([{ agent: 'planner' }])
  })

  it('registers both halves from apply', () => {
    const fake = fakeContext()
    apply(fake.ctx)
    expect(fake.providers).toHaveLength(1)
    expect(fake.tools).toHaveLength(1)
  })
})
