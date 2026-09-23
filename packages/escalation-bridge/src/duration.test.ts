import { describe, expect, test } from 'bun:test'

import { parseDuration } from './duration.js'

describe('parseDuration', () => {
  test('units', () => {
    expect(parseDuration('30s')).toBe(30)
    expect(parseDuration('5m')).toBe(300)
    expect(parseDuration('1h')).toBe(3600)
  })

  test('rejects garbage', () => {
    for (const bad of ['', '30', '30x', 'm5', '-5m']) {
      expect(() => parseDuration(bad)).toThrow(Error)
    }
  })
})
