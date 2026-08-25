import { afterEach, describe, expect, it, mock } from 'bun:test'

const brokerCancelMock = mock(async () => {})
const closeMock = mock(() => {})
const getSupervisorClientMock = mock(async () => ({
  client: { brokerCancel: brokerCancelMock, close: closeMock },
  isSingleton: false,
  sup: null,
  supervisorId: '/tmp/avenor-cancel-test.sock',
}))

const { createCancelTool } = await import('./cancel.js')
const cancelTool = createCancelTool(getSupervisorClientMock)

describe('cancelTool', () => {
  afterEach(() => {
    brokerCancelMock.mockClear()
    closeMock.mockClear()
    getSupervisorClientMock.mockClear()
  })

  it('calls brokerCancel with the message id and returns { cancelled: true }', async () => {
    const res = await cancelTool({ messageId: 'ask-1' })
    expect(brokerCancelMock).toHaveBeenCalledWith('ask-1')
    expect(res).toEqual({ cancelled: true })
  })

  it('propagates errors from brokerCancel', async () => {
    brokerCancelMock.mockRejectedValueOnce(new Error('not the sender'))
    await expect(cancelTool({ messageId: 'ask-1' })).rejects.toThrow('not the sender')
  })

  it('closes a non-singleton client in the finally block on success', async () => {
    await cancelTool({ messageId: 'ask-1' })
    expect(closeMock).toHaveBeenCalled()
  })

  it('closes a non-singleton client in the finally block on error', async () => {
    brokerCancelMock.mockRejectedValueOnce(new Error('boom'))
    await expect(cancelTool({ messageId: 'ask-1' })).rejects.toThrow('boom')
    expect(closeMock).toHaveBeenCalled()
  })
})
