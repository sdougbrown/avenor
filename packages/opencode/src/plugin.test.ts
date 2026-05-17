import { describe, it, expect } from 'bun:test'
import { AvenorPlugin } from './plugin.js'

describe('AvenorPlugin', () => {
  it('exports a function', () => {
    expect(typeof AvenorPlugin).toBe('function')
  })
})
