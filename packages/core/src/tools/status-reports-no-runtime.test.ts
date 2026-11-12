import { describe, expect, it } from 'bun:test'

import { RpcError } from '../client.js'
import { runtimeNotFoundMessage, statusReportsNoRuntime } from './message-provider.js'

describe('statusReportsNoRuntime', () => {
  it('prefers the structured code over the sentence', () => {
    // A supervisor new enough to carry the code is believed even when its
    // sentence is rewritten, which is the whole point of the code.
    const error = new RpcError(-32000, 'a reworded absence answer', {
      code: 'runtime_not_found',
      runtime_id: 'rt_1',
    })
    expect(statusReportsNoRuntime(error, 'rt_1')).toBe(true)
  })

  it('refuses a code that names another runtime', () => {
    const error = new RpcError(-32000, 'a reworded absence answer', {
      code: 'runtime_not_found',
      runtime_id: 'rt_2',
    })
    expect(statusReportsNoRuntime(error, 'rt_1')).toBe(false)
  })

  it('accepts a code with no runtime id, which can only mean this one', () => {
    const error = new RpcError(-32000, 'a reworded absence answer', { code: 'runtime_not_found' })
    expect(statusReportsNoRuntime(error, 'rt_1')).toBe(true)
  })

  it('falls back to the sentence from a supervisor that sends no code', () => {
    expect(statusReportsNoRuntime(new RpcError(-32000, runtimeNotFoundMessage('rt_1')), 'rt_1')).toBe(true)
    expect(statusReportsNoRuntime(new RpcError(-32000, runtimeNotFoundMessage('rt_2')), 'rt_1')).toBe(false)
    expect(statusReportsNoRuntime(new RpcError(-32000, 'a reworded absence answer'), 'rt_1')).toBe(false)
  })

  it('keeps a failure from another code as the caller\'s error', () => {
    const error = new RpcError(-32601, runtimeNotFoundMessage('rt_1'), {
      code: 'runtime_not_found',
      runtime_id: 'rt_1',
    })
    expect(statusReportsNoRuntime(error, 'rt_1')).toBe(false)
  })
})
