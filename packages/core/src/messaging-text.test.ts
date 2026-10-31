import { describe, expect, it } from 'bun:test'

import messageInputSchema from '../../../schemas/message.input.json'
import {
  MAX_FINAL_OUTPUT_RUNES,
  MAX_WAIT_MS,
  boundFinalOutput,
  extractReplyId,
  extractReplyMessage,
  extractTurnText,
  newMsgID,
  normalizeAskTimeoutMs,
  normalizeTurnStopReason,
  renderAskFailure,
  substitute,
} from './messaging-text.js'

describe('substitute', () => {
  it('fills known facts and leaves an unknown placeholder intact', () => {
    expect(substitute('{a} and {b}', { a: 'one' })).toBe('one and {b}')
    expect(substitute('{a}', {})).toBe('{a}')
    expect(substitute(undefined)).toBe('')
  })
})

describe('newMsgID', () => {
  it('mints a versioned id whose deadline spans the shared wait window', () => {
    const now = 1_700_000_000_000
    const [version, deadline, nonce] = newMsgID(now).split('.')
    expect(version).toBe('av1')
    expect(Number.parseInt(deadline, 36)).toBe(now + MAX_WAIT_MS)
    expect(nonce).toMatch(/^[0-9a-f]{32}$/)
  })

  it('mints a different id every time', () => {
    expect(newMsgID()).not.toBe(newMsgID())
  })
})

describe('extractTurnText', () => {
  it('reads the last assistant message text and its camelCase stop reason', () => {
    expect(extractTurnText([
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: [{ type: 'text', text: 'the answer' }], stopReason: 'stop' },
    ])).toEqual({ text: 'the answer', stopReason: 'stop' })
  })

  it('accepts the snake_case stop reason and every text shape pi emits', () => {
    expect(extractTurnText([{ content: 'plain', stop_reason: 'length' }])).toEqual({
      text: 'plain',
      stopReason: 'length',
    })
    expect(extractTurnText([{ message: { content: [{ text: 'nested' }] } }]).text).toBe('nested')
  })

  it('reports no text for a turn that produced none', () => {
    expect(extractTurnText([{ role: 'assistant', content: [], stopReason: 'error' }])).toEqual({
      text: '',
      stopReason: 'error',
    })
    expect(extractTurnText([])).toEqual({ text: '', stopReason: '' })
  })
})

describe('normalizeTurnStopReason', () => {
  it('maps exactly the pi turn endings Go normalises', () => {
    expect(normalizeTurnStopReason('stop')).toBe('end_turn')
    expect(normalizeTurnStopReason('end_of_turn')).toBe('end_turn')
    expect(normalizeTurnStopReason('length')).toBe('max_tokens')
    expect(normalizeTurnStopReason('aborted')).toBe('cancelled')
    expect(normalizeTurnStopReason('error')).toBe('error')
  })

  it('passes through every spelling Go passes through, so host and child agree', () => {
    // internal/runtime/pi/events.go:normalizePiStopReason leaves these alone
    // (TestTranslateAgentEndMapsPiStopReasons pins the same words), so a Pi
    // host that rewrote them would disagree with the same turn seen as a child.
    for (const raw of ['end-turn', 'tool-calls', 'tool_calls', 'toolUse', 'end_turn']) {
      expect(normalizeTurnStopReason(raw)).toBe(raw)
    }
  })
})

describe('boundFinalOutput', () => {
  it('keeps a short turn and bounds a long one at the recorder limit', () => {
    expect(boundFinalOutput('hello')).toBe('hello')
    const long = 'é'.repeat(MAX_FINAL_OUTPUT_RUNES + 10)
    const bounded = boundFinalOutput(long)
    expect(Array.from(bounded)).toHaveLength(MAX_FINAL_OUTPUT_RUNES)
    expect(bounded).toBe('é'.repeat(MAX_FINAL_OUTPUT_RUNES))
  })
})

describe('shared bounds', () => {
  it('matches the ask deadline the contract caps at', () => {
    // The deadline the message id encodes must equal the shared wait cap, which
    // the Go router pins to this same schema.
    const variants = (
      messageInputSchema as unknown as {
        oneOf: { properties?: { action?: { const?: string }; timeout_ms?: { maximum?: number } } }[]
      }
    ).oneOf
    const askVariant = variants.find(variant => variant.properties?.action?.const === 'ask')
    expect(askVariant?.properties?.timeout_ms?.maximum).toBe(MAX_WAIT_MS)
  })

  it('pins the final output rune bound', () => {
    // internal/events.MaxFinalOutputRunes carries the same literal; the two
    // must change together.
    expect(MAX_FINAL_OUTPUT_RUNES).toBe(4096)
  })
})

describe('reply helpers', () => {
  it('extracts a payload message and falls back to the full payload', () => {
    expect(extractReplyMessage({ payload: { message: 'OK' } })).toBe('OK')
    expect(extractReplyMessage({ timeout: true })).toBe('(timeout: no reply received)')
    expect(extractReplyMessage({ payload: { other: 1 } })).toBe('{"payload":{"other":1}}')
  })

  it('extracts the reply route key so the asker can answer the answer', () => {
    expect(extractReplyId({ payload: { id: 'm.reply', message: 'OK' } })).toBe('m.reply')
    expect(extractReplyId({ payload: { message: 'OK' } })).toBe('')
    expect(extractReplyId({})).toBe('')
  })

  it('renders the shared ask failure sentence instead of composing guidance', () => {
    expect(renderAskFailure('av1.m.1', 'mesh-a', 'possibly-pending')).toBe(
      'ask "av1.m.1" to "mesh-a" may still be pending; if no reply arrives, withdraw it with avenor_message action="withdraw" message_id="av1.m.1"',
    )
    expect(renderAskFailure('av1.m.1', 'mesh-a', 'withdrawn')).toBe(
      'ask "av1.m.1" to "mesh-a" was withdrawn',
    )
  })

  it('reduces a timeout to a positive finite value', () => {
    expect(normalizeAskTimeoutMs(5000)).toBe(5000)
    for (const value of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, undefined]) {
      expect(normalizeAskTimeoutMs(value)).toBeUndefined()
    }
  })
})
