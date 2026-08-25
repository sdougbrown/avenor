import { afterEach, describe, expect, it, mock } from 'bun:test'

const brokerAskMock = mock(async () => ({ payload: { message: 'hello back' }, from_run_id: 'sender' }))
const closeMock = mock(() => {})
const getSupervisorClientMock = mock(async () => ({
  client: { brokerAsk: brokerAskMock, close: closeMock },
  isSingleton: false,
  sup: null,
  supervisorId: '/tmp/avenor-ask-test.sock',
}))

const { createAskTool } = await import('./ask.js')
const askTool = createAskTool(getSupervisorClientMock)

describe('askTool', () => {
  afterEach(() => {
    brokerAskMock.mockClear()
    closeMock.mockClear()
    getSupervisorClientMock.mockClear()
  })

  it('extracts payload.message and from_run_id', async () => {
    const res = await askTool({ toRunId: 'rt_1', message: 'hello' })
    expect(res.reply).toBe('hello back')
    expect(res.from_run_id).toBe('sender')
    expect(brokerAskMock).toHaveBeenCalledWith('rt_1', 'hello')
  })

  it('returns a timeout notice when the broker times out', async () => {
    brokerAskMock.mockResolvedValueOnce({ timeout: true, message_id: 'ask-1' })
    const res = await askTool({ toRunId: 'rt_1', message: 'hello' })
    expect(res.reply).toBe('(timeout: no reply received)')
  })

  it('returns a cancelled notice when the ask is cancelled', async () => {
    brokerAskMock.mockResolvedValueOnce({ cancelled: true, message_id: 'ask-1' })
    const res = await askTool({ toRunId: 'rt_1', message: 'hello' })
    expect(res.reply).toBe('(cancelled)')
  })

  it('falls back to a JSON rendering for an unexpected reply shape', async () => {
    brokerAskMock.mockResolvedValueOnce({ weird: 1 })
    const res = await askTool({ toRunId: 'rt_1', message: 'hello' })
    expect(res.reply).toContain('weird')
  })

  it('closes a non-singleton client in the finally block', async () => {
    await askTool({ toRunId: 'rt_1', message: 'hello' })
    expect(closeMock).toHaveBeenCalled()
  })
})
