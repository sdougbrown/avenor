import { describe, expect, it } from 'bun:test'
import { decideCompletion } from './types.js'

describe('decideCompletion', () => {
  it('skips when blocking (avenor_result is actively waiting)', () => {
    expect(decideCompletion({ blocking: true, consumed: false })).toBe('skip')
    expect(decideCompletion({ blocking: true, consumed: true })).toBe('skip')
  })

  it('skips when consumed (avenor_result already returned the result)', () => {
    expect(decideCompletion({ blocking: false, consumed: true })).toBe('skip')
    expect(decideCompletion({ blocking: undefined, consumed: true })).toBe('skip')
  })

  it('sends on the first terminal tick when neither blocking nor consumed', () => {
    expect(decideCompletion({ blocking: false, consumed: false })).toBe('send')
    expect(decideCompletion({ blocking: undefined, consumed: undefined })).toBe('send')
  })

  it('treats an interrupted avenor_result as unconsumed (still sends)', () => {
    // An aborted/timeout avenor_result resets blocking and never sets
    // consumed, so the completion must still be delivered.
    expect(decideCompletion({ blocking: false, consumed: false })).toBe('send')
  })

  it('skips take priority over send in every combination', () => {
    expect(decideCompletion({ blocking: true, consumed: false })).toBe('skip')
    expect(decideCompletion({ blocking: false, consumed: true })).toBe('skip')
    expect(decideCompletion({ blocking: true, consumed: true })).toBe('skip')
  })
})