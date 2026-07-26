import { describe, expect, it } from 'vitest'
import { resolveAgentProfile } from './agent-profile.js'

describe('resolveAgentProfile', () => {
  it('uses the latest optional pi-agents session selection', () => {
    expect(resolveAgentProfile([
      { type: 'custom', customType: 'pi-agents:profile', data: { name: 'cloud' } },
      { type: 'custom', customType: 'other', data: { name: 'ignored' } },
      { type: 'custom', customType: 'pi-agents:profile', data: { name: 'local' } },
    ], {})).toBe('local')
  })

  it('honors a persisted clear and ignores malformed entries', () => {
    expect(resolveAgentProfile([
      { type: 'custom', customType: 'pi-agents:profile', data: { name: 'cloud' } },
      { type: 'custom', customType: 'pi-agents:profile', data: { name: null } },
      null,
      { type: 'custom', customType: 'pi-agents:profile', data: null },
    ], {})).toBeUndefined()
  })

  it('gives the namespaced environment override precedence', () => {
    expect(resolveAgentProfile(
      [{ type: 'custom', customType: 'pi-agents:profile', data: { name: 'cloud' } }],
      { PI_AGENT_PROFILE: 'forced' },
    )).toBe('forced')
  })
})
