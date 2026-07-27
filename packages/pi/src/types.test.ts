import { describe, expect, it } from 'bun:test'
import { decideCompletion } from './types.js'

describe('decideCompletion', () => {
  it('skips when blocking (avenor_result is waiting)', () => {
    expect(decideCompletion({ blocking: true, completionPending: false })).toBe('skip')
    expect(decideCompletion({ blocking: true, completionPending: true })).toBe('skip')
  })

  it('defers on first terminal tick (no completionPending yet)', () => {
    expect(decideCompletion({ blocking: false, completionPending: false })).toBe('defer')
    expect(decideCompletion({ blocking: undefined, completionPending: undefined })).toBe('defer')
  })

  it('sends on second terminal tick (completionPending already set)', () => {
    expect(decideCompletion({ blocking: false, completionPending: true })).toBe('send')
  })

  it('skips take priority over defer and send', () => {
    // blocking=true with completionPending=true should still skip
    expect(decideCompletion({ blocking: true, completionPending: true })).toBe('skip')
  })

  it('simulates the full deferral lifecycle', () => {
    // Tick 1: run just reached terminal, not blocking, not pending
    let run = { blocking: false, completionPending: false }
    expect(decideCompletion(run)).toBe('defer')
    run.completionPending = true

    // Tick 2: agent didn't call avenor_result, still not blocking
    expect(decideCompletion(run)).toBe('send')

    // Alternative: between tick 1 and 2, avenor_result was called
    run = { blocking: false, completionPending: false }
    expect(decideCompletion(run)).toBe('defer')
    run.completionPending = true
    run.blocking = true // avenor_result acquired the run
    expect(decideCompletion(run)).toBe('skip')
    // avenor_result returns and deletes the run — tick 2 never fires
  })
})