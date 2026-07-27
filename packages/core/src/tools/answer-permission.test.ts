import { describe, expect, it } from 'bun:test'
import { pendingPermissionRequestID } from './answer-permission.js'

describe('pendingPermissionRequestID', () => {
  it('reads the stable control permission object', () => {
    expect(pendingPermissionRequestID({
      pending_permission: true,
      permission: { request_id: 'req-control' },
    })).toBe('req-control')
  })

  it('accepts normalized pending permission objects', () => {
    expect(pendingPermissionRequestID({
      pending_permission: { request_id: 'req-normalized' },
    })).toBe('req-normalized')
  })

  it('rejects missing or malformed permission metadata', () => {
    expect(pendingPermissionRequestID({ pending_permission: true })).toBeUndefined()
    expect(pendingPermissionRequestID(null)).toBeUndefined()
  })
})
