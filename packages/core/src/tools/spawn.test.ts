import { afterEach, describe, expect, it, mock } from 'bun:test'
import * as net from 'node:net'
import * as os from 'node:os'
import * as path from 'node:path'
import type { SpawnParams } from '../client.js'
import { socketsRoot } from '../paths.js'

const { spawnTool } = await import('./spawn.js')
const { Supervisor } = await import('../supervisor.js')

type SpawnForwardingParams = Parameters<typeof spawnTool>[0]

type ForwardingCase = {
  name: string
  args: SpawnForwardingParams
  model?: string
  agent?: string
  thinking?: SpawnParams['thinking']
}

const forwardingCases: ForwardingCase[] = [
  { name: 'neither supplied', args: {}, model: undefined, agent: undefined },
  { name: 'model only', args: { model: 'sonnet' }, model: 'sonnet', agent: undefined },
  { name: 'both supplied', args: { agent: 'codex', model: 'sonnet' }, model: 'sonnet', agent: 'codex' },
  { name: 'empty agent', args: { agent: '', model: 'sonnet' }, model: 'sonnet', agent: undefined },
  { name: 'thinking', args: { thinking: 'xhigh' }, thinking: 'xhigh' },
]

function assertForwardedParams(
  received: SpawnParams | undefined,
  expected: { model?: string; agent?: string; thinking?: SpawnParams['thinking'] },
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
  } else {
    expect(received.model).toBeUndefined()
  }
  if (expected.thinking === undefined) {
    expect(Object.hasOwn(received, 'thinking')).toBe(false)
  } else {
    expect(received.thinking).toBe(expected.thinking)
  }
}

async function startSpawnServer(): Promise<{
  server: net.Server
  socketPath: string
  received: SpawnParams[]
}> {
  const socketPath = path.join(
    socketsRoot(),
    `avenor-mcp-spawn-test-${process.pid}-${Math.random().toString(36).slice(2)}.sock`,
  )
  const received: SpawnParams[] = []
  const server = net.createServer((socket) => {
    let buffer = ''
    socket.on('data', (chunk) => {
      buffer += chunk.toString()
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        if (!line.trim()) continue
        const request = JSON.parse(line) as { id: number; params: SpawnParams }
        received.push(request.params)
        socket.write(JSON.stringify({
          jsonrpc: '2.0',
          id: request.id,
          result: { runtime_id: 'rt-explicit' },
        }) + '\n')
      }
    })
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(socketPath, resolve)
  })
  return { server, socketPath, received }
}

async function stopSpawnServer(server: net.Server, socketPath: string): Promise<void> {
  await new Promise<void>((resolve) => server.close(() => resolve()))
  try {
    await os.promises.unlink(socketPath)
  } catch {
    // The server may have removed the socket during close.
  }
}

describe('spawnTool with an explicit supervisor', () => {
  for (const testCase of forwardingCases) {
    it(`forwards ${testCase.name} without fabricating an agent`, async () => {
      const { server, socketPath, received } = await startSpawnServer()
      try {
        await spawnTool({ supervisorId: socketPath, ...testCase.args })
        expect(received).toHaveLength(1)
        assertForwardedParams(received[0], testCase)
      } finally {
        await stopSpawnServer(server, socketPath)
      }
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

  afterEach(() => {
    Supervisor.get = originalSupervisorGet
    localSpawnMock.mockClear()
  })

  for (const testCase of forwardingCases) {
    it(`forwards ${testCase.name} without fabricating an agent`, async () => {
      Supervisor.get = mock(async () => ({
        spawn: localSpawnMock,
        supervisorId: '/tmp/avenor-singleton.sock',
      })) as any

      await spawnTool(testCase.args)

      expect(localSpawnMock).toHaveBeenCalledTimes(1)
      assertForwardedParams(localSpawnMock.mock.calls[0]?.[0], testCase)
    })
  }
})
