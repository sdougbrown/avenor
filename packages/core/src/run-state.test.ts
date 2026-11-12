import { describe, expect, it } from 'bun:test'

import fixture from '../../../schemas/runstate.conformance.json'
import {
  isResumableSentinel,
  isTerminalPhase,
  isTerminalSentinel,
  isTerminalStatus,
  sentinelStatus,
  translate,
  type Translation,
} from './run-state.js'

type RunStateFixture = {
  translate: Array<{
    name: string
    status: string
    phase: string
    want: Translation
  }>
  sentinelStatus: Array<{ word: string; want: string }>
  terminalStatus: Array<{ status: string; want: boolean }>
  terminalSentinel: Array<{ word: string; want: boolean }>
  resumableSentinel: Array<{ word: string; want: boolean }>
}

const cases = fixture as RunStateFixture

describe('the shared fixture', () => {
  it('has cases in every section', () => {
    for (const [section, list] of Object.entries(cases)) {
      if (Array.isArray(list)) expect(list.length, section).toBeGreaterThan(0)
    }
    expect(cases.terminalStatus.length).toBeGreaterThan(0)
  })
})

describe('run-state conformance with the shared fixture', () => {
  for (const c of cases.translate) {
    it(`translate: ${c.name}`, () => {
      expect(translate(c.status, c.phase)).toEqual(c.want)
    })
  }

  for (const c of cases.sentinelStatus) {
    it(`sentinelStatus(${JSON.stringify(c.word)})`, () => {
      expect(sentinelStatus(c.word)).toBe(c.want)
    })
  }

  for (const c of cases.terminalSentinel) {
    it(`isTerminalSentinel(${JSON.stringify(c.word)})`, () => {
      expect(isTerminalSentinel(c.word)).toBe(c.want)
    })
  }

  for (const c of cases.resumableSentinel) {
    it(`isResumableSentinel(${JSON.stringify(c.word)})`, () => {
      expect(isResumableSentinel(c.word)).toBe(c.want)
    })
  }
})

describe('terminal predicates', () => {
  for (const c of cases.terminalStatus) {
    it(`isTerminalStatus and isTerminalPhase(${JSON.stringify(c.status)})`, () => {
      expect(isTerminalStatus(c.status)).toBe(c.want)
      expect(isTerminalPhase(c.status)).toBe(c.want)
    })
  }

  it('treats an absent status as not terminal', () => {
    expect(isTerminalStatus(undefined)).toBe(false)
  })
})
