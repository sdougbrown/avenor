import { describe, it, expect } from 'bun:test'
import { apply, inject } from './index.js'
import type { SubagentProvider } from './types.js'

describe('plugin entry', () => {
  it('waits for the subagent registry before activating', () => {
    expect(inject).toEqual(['subagents'])
  })

  it('registers the avenor provider on the host context', () => {
    let registered: SubagentProvider | undefined
    apply({
      subagents: {
        registerProvider(provider) {
          registered = provider
          return () => {}
        },
      },
    })
    expect(registered?.name).toBe('avenor')
    expect(registered?.inheritsParentContext).toBe(false)
  })
})
