import { afterEach, beforeAll, afterAll, beforeEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { ensureRunPaths } from '../paths.js'
import { RpcError } from '../client.js'

const brokerAskMock = mock(async () => ({ payload: { message: 'hello back' }, from_run_id: 'sender' }))
const closeMock = mock(() => {})
const getSupervisorClientMock = mock(async () => ({
  client: { brokerAsk: brokerAskMock, close: closeMock },
  isSingleton: false,
  sup: null,
  supervisorId: '/tmp/avenor-ask-test.sock',
}))

const { Supervisor } = await import('../supervisor.js')
const { createAskTool, askTool: realAskTool } = await import('./ask.js')
const { forgetExternalRuns, registerExternalRun } = await import('./run-registry.js')
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
    expect(brokerAskMock).toHaveBeenCalledWith('rt_1', 'hello', undefined, undefined)
  })

  it('passes timeoutMs through to brokerAsk', async () => {
    await askTool({ toRunId: 'rt_1', message: 'hello', timeoutMs: 5000 })
    expect(brokerAskMock).toHaveBeenCalledWith('rt_1', 'hello', undefined, 5000)
  })

  it('does not forward timeoutMs=0 to brokerAsk', async () => {
    await askTool({ toRunId: 'rt_1', message: 'hello', timeoutMs: 0 })
    expect(brokerAskMock).toHaveBeenCalledWith('rt_1', 'hello', undefined, undefined)
  })

  it('does not forward a negative timeoutMs to brokerAsk', async () => {
    await askTool({ toRunId: 'rt_1', message: 'hello', timeoutMs: -1 })
    expect(brokerAskMock).toHaveBeenCalledWith('rt_1', 'hello', undefined, undefined)
  })

  it('does not forward a NaN timeoutMs to brokerAsk', async () => {
    await askTool({ toRunId: 'rt_1', message: 'hello', timeoutMs: NaN })
    expect(brokerAskMock).toHaveBeenCalledWith('rt_1', 'hello', undefined, undefined)
  })

  it('surfaces the message_id from an RpcError with data', async () => {
    const rpcError = new RpcError(-32000, 'ask failed', { message_id: 'abc123' })
    brokerAskMock.mockRejectedValueOnce(rpcError)
    const err = await askTool({ toRunId: 'rt_1', message: 'hello' }).catch((e) => e)
    expect(err).toBeInstanceOf(Error)
    expect(err).not.toBe(rpcError)
    expect(err.message).toContain('message_id: abc123')
    expect(err.message).toContain('avenor_cancel')
    expect(err.message).toContain('ask failed')
  })

  it('surfaces withdraw guidance when the failed ask is still pending', async () => {
    const rpcError = new RpcError(-32000, 'ask failed', { message_id: 'abc123', pending: true })
    brokerAskMock.mockRejectedValueOnce(rpcError)
    const err = await askTool({ toRunId: 'rt_1', message: 'hello' }).catch((e) => e)
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toContain('message_id: abc123')
    expect(err.message).toContain('pass to avenor_cancel to withdraw')
  })

  it('reports an already-withdrawn ask instead of suggesting a cancel', async () => {
    const rpcError = new RpcError(-32000, 'ask failed', { message_id: 'abc123', pending: false })
    brokerAskMock.mockRejectedValueOnce(rpcError)
    const err = await askTool({ toRunId: 'rt_1', message: 'hello' }).catch((e) => e)
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toContain('message_id: abc123')
    expect(err.message).toContain('the pending ask was already withdrawn')
    expect(err.message).not.toContain('pass to avenor_cancel to withdraw')
  })

  it('keeps the withdraw guidance when pending is absent (legacy shape)', async () => {
    const rpcError = new RpcError(-32000, 'ask failed', { message_id: 'abc123' })
    brokerAskMock.mockRejectedValueOnce(rpcError)
    const err = await askTool({ toRunId: 'rt_1', message: 'hello' }).catch((e) => e)
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toContain('message_id: abc123')
    expect(err.message).toContain('pass to avenor_cancel to withdraw')
  })

  it('propagates an RpcError without a message_id unchanged', async () => {
    const rpcError = new RpcError(-32000, 'ask failed', { other: 1 })
    brokerAskMock.mockRejectedValueOnce(rpcError)
    await expect(askTool({ toRunId: 'rt_1', message: 'hello' })).rejects.toBe(rpcError)
  })

  it('propagates non-RpcError errors unchanged', async () => {
    const plain = new Error('something else')
    brokerAskMock.mockRejectedValueOnce(plain)
    await expect(askTool({ toRunId: 'rt_1', message: 'hello' })).rejects.toBe(plain)
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

describe('askTool without supervisor_id (singleton fallback)', () => {
  const singletonBrokerAsk = mock(async () => ({ payload: { message: 'singleton reply' }, from_run_id: 'sender' }))
  const singletonClose = mock(() => {})
  const originalSupervisorGet = Supervisor.get

  beforeAll(() => {
    // Regression: an omitted supervisor_id previously hit
    // path.dirname(undefined) in validateSupervisorSocketPath and threw
    // "The \"path\" argument must be of type string". It must resolve the
    // in-process singleton instead.
    Supervisor.get = mock(async () => ({
      runs: new Map(),
      aliases: new Map(),
      getRunByReference: () => undefined,
      getRunByRuntimeId: () => undefined,
      getClient: () => ({ brokerAsk: singletonBrokerAsk, close: singletonClose }),
      supervisorId: '/tmp/avenor-ask-singleton.sock',
    })) as any
  })

  afterAll(() => {
    Supervisor.get = originalSupervisorGet
  })

  afterEach(() => {
    singletonBrokerAsk.mockClear()
    singletonClose.mockClear()
  })

  it('resolves the in-process singleton instead of validating an undefined socket path', async () => {
    const res = await realAskTool({ toRunId: 'rt_1', message: 'hello' })
    expect(res.reply).toBe('singleton reply')
    expect(res.from_run_id).toBe('sender')
    expect(singletonBrokerAsk).toHaveBeenCalledWith('rt_1', 'hello', undefined, undefined)
    expect(singletonClose).not.toHaveBeenCalled()
  })
})

describe('askTool resolves run references to broker runtime ids', () => {
  const externalBrokerAsk = mock(async () => ({ payload: { message: 'resolved reply' }, from_run_id: 'sender' }))
  const externalClose = mock(() => {})
  const supervisorId = '/tmp/avenor-ask-resolve-test.sock'
  const externalGetClient = mock(async () => ({
    client: { brokerAsk: externalBrokerAsk, close: externalClose },
    isSingleton: false,
    sup: null,
    supervisorId,
  }))
  const externalAskTool = createAskTool(externalGetClient)

  let previousHome: string | undefined
  let home = ''

  beforeEach(() => {
    previousHome = process.env.AVENOR_HOME
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-ask-resolve-'))
    process.env.AVENOR_HOME = home
    forgetExternalRuns(supervisorId)
    registerExternalRun({
      runId: 'public-run',
      label: 'demo',
      supervisorId,
      runtimeId: 'rt-9',
      ...ensureRunPaths('public-run'),
    })
  })

  afterEach(() => {
    forgetExternalRuns(supervisorId)
    if (previousHome === undefined) delete process.env.AVENOR_HOME
    else process.env.AVENOR_HOME = previousHome
    fs.rmSync(home, { recursive: true, force: true })
    externalBrokerAsk.mockClear()
    externalClose.mockClear()
    externalGetClient.mockClear()
  })

  it('addresses an external run by its public run id', async () => {
    await externalAskTool({ toRunId: 'public-run', message: 'hello' })
    expect(externalBrokerAsk).toHaveBeenCalledWith('rt-9', 'hello', undefined, undefined)
  })

  it('addresses an external run by its label alias', async () => {
    await externalAskTool({ toRunId: 'demo', message: 'hello' })
    expect(externalBrokerAsk).toHaveBeenCalledWith('rt-9', 'hello', undefined, undefined)
  })

  it('passes an unresolved id through unchanged (e.g. supervisor)', async () => {
    await externalAskTool({ toRunId: 'supervisor', message: 'hello' })
    expect(externalBrokerAsk).toHaveBeenCalledWith('supervisor', 'hello', undefined, undefined)
  })

  it('falls back to the disk-read registry when in-memory state is cleared', async () => {
    // registerExternalRun persisted run.json in beforeEach; drop the in-memory
    // index so findExternalRun must resolve 'public-run' via readPersistedRun.
    forgetExternalRuns(supervisorId)
    await externalAskTool({ toRunId: 'public-run', message: 'hello' })
    expect(externalBrokerAsk).toHaveBeenCalledWith('rt-9', 'hello', undefined, undefined)
  })
})

describe('askTool resolves local singleton run references', () => {
  const localBrokerAsk = mock(async () => ({ payload: { message: 'local reply' }, from_run_id: 'sender' }))
  const localClose = mock(() => {})
  const localSup = Object.create(Supervisor.prototype) as any
  localSup.runs = new Map<string, any>([
    ['local-run', { runId: 'local-run', label: 'my-label', runtimeId: 'rt-77' }],
  ])
  localSup.aliases = new Map<string, any>([['my-label', localSup.runs.get('local-run')]])
  const localGetClient = mock(async () => ({
    client: { brokerAsk: localBrokerAsk, close: localClose },
    isSingleton: true,
    sup: localSup,
    supervisorId: '/tmp/avenor-ask-local.sock',
  }))
  const localAskTool = createAskTool(localGetClient)

  afterEach(() => {
    localBrokerAsk.mockClear()
    localClose.mockClear()
    localGetClient.mockClear()
  })

  it('substitutes the resolved runtime id for a local public run id', async () => {
    await localAskTool({ toRunId: 'local-run', message: 'hello' })
    expect(localBrokerAsk).toHaveBeenCalledWith('rt-77', 'hello', undefined, undefined)
  })

  it('substitutes the resolved runtime id for a local label alias', async () => {
    await localAskTool({ toRunId: 'my-label', message: 'hello' })
    expect(localBrokerAsk).toHaveBeenCalledWith('rt-77', 'hello', undefined, undefined)
  })
})
