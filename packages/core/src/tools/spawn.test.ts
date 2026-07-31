import { afterAll, afterEach, describe, expect, it, mock } from 'bun:test'
import type { SpawnParams } from '../client.js'

const clientSpawnMock = mock(async (_params: SpawnParams) => ({
  runtime_id: 'rt-explicit',
}))
const closeMock = mock(() => {})
const getSupervisorClientMock = mock(async () => ({
  client: {
    spawn: clientSpawnMock,
    close: closeMock,
  },
  isSingleton: false,
  sup: null,
  supervisorId: '/tmp/avenor-explicit.sock',
}))

mock.module('./get-supervisor-client.js', () => ({
  getSupervisorClient: getSupervisorClientMock,
}))

const { spawnTool } = await import('./spawn.js')
const { Supervisor } = await import('../supervisor.js')

const forwardingCases = [
  { name: 'neither supplied', args: {}, model: undefined, agent: undefined },
  { name: 'model only', args: { model: 'sonnet' }, model: 'sonnet', agent: undefined },
  { name: 'both supplied', args: { agent: 'codex', model: 'sonnet' }, model: 'sonnet', agent: 'codex' },
  { name: 'empty agent', args: { agent: '', model: 'sonnet' }, model: 'sonnet', agent: undefined },
] as const

type SpawnForwardingParams = Parameters<typeof spawnTool>[0]

function assertForwardedParams(
  received: SpawnParams | undefined,
  expected: { model?: string; agent?: string },
): void {
  expect(received).toBeDefined()
  if (!received) return

  if (expected.agent === undefined) {
    expect(Object.hasOwn(received, 'agent')).toBe(false)
  } else {
    expect(received.agent).toBe(expected.agent)
  }
  if (expected.model !== undefined) {
    expect(received.model).toBe(expected.model)
  }
}

describe('spawnTool with an explicit supervisor', () => {
  afterEach(() => {
    clientSpawnMock.mockClear()
    closeMock.mockClear()
    getSupervisorClientMock.mockClear()
  })

  for (const testCase of forwardingCases) {
    it(`forwards ${testCase.name} without fabricating an agent`, async () => {
      const args: SpawnForwardingParams = {
        supervisorId: '/tmp/avenor-explicit.sock',
        ...testCase.args,
      }
      await spawnTool(args)

      expect(clientSpawnMock).toHaveBeenCalledTimes(1)
      assertForwardedParams(clientSpawnMock.mock.calls[0]?.[0], testCase)
      expect(closeMock).toHaveBeenCalledTimes(1)
    })
  }
})

describe('spawnTool with the singleton supervisor', () => {
  const localSpawnMock = mock(async (params: SpawnParams, runId: string) => ({
    runId,
    label: params.label as string,
    sentinelPath: '/tmp/sentinel.done',
    eventLogPath: '/tmp/events.ndjson',
    runtimeId: 'rt-singleton',
  }))
  const originalSupervisorGet = Supervisor.get

  afterAll(() => {
    Supervisor.get = originalSupervisorGet
  })

  afterEach(() => {
    localSpawnMock.mockClear()
  })

  for (const testCase of forwardingCases) {
    it(`forwards ${testCase.name} without fabricating an agent`, async () => {
      Supervisor.get = mock(async () => ({
        spawn: localSpawnMock,
        supervisorId: '/tmp/avenor-singleton.sock',
      })) as any

      const args: SpawnForwardingParams = { ...testCase.args }
      await spawnTool(args)

      expect(localSpawnMock).toHaveBeenCalledTimes(1)
      assertForwardedParams(localSpawnMock.mock.calls[0]?.[0], testCase)
    })
  }
})
