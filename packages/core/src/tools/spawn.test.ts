import { afterEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as net from 'node:net'
import * as os from 'node:os'
import * as path from 'node:path'
import type { SpawnParams } from '../client.js'
import { socketsRoot } from '../paths.js'

const { createSpawnTool, spawnTool } = await import('./spawn.js')
const { Supervisor } = await import('../supervisor.js')

type SpawnForwardingParams = Parameters<typeof spawnTool>[0]

type ForwardingCase = {
  name: string
  args: SpawnForwardingParams
  model?: string
  agent?: string
  thinking?: SpawnParams['thinking']
  rosterFile?: string
  rosterEntry?: string
}

const forwardingCases: ForwardingCase[] = [
  { name: 'neither supplied', args: {}, model: undefined, agent: undefined },
  { name: 'model only', args: { model: 'sonnet' }, model: 'sonnet', agent: undefined },
  { name: 'both supplied', args: { agent: 'codex', model: 'sonnet' }, model: 'sonnet', agent: 'codex' },
  { name: 'empty agent', args: { agent: '', model: 'sonnet' }, model: 'sonnet', agent: undefined },
  { name: 'thinking', args: { thinking: 'xhigh' }, thinking: 'xhigh' },
  {
    name: 'roster selector',
    args: { rosterFile: '/repo/roster.json', rosterEntry: 'planner' },
    rosterFile: '/repo/roster.json',
    rosterEntry: 'planner',
  },
]

function assertForwardedParams(
  received: SpawnParams | undefined,
  expected: {
    model?: string
    agent?: string
    thinking?: SpawnParams['thinking']
    rosterFile?: string
    rosterEntry?: string
  },
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
  if (expected.rosterFile === undefined) {
    expect(Object.hasOwn(received, 'roster_file')).toBe(false)
    expect(Object.hasOwn(received, 'roster_entry')).toBe(false)
  } else {
    expect(received.roster_file).toBe(expected.rosterFile)
    expect(received.roster_entry).toBe(expected.rosterEntry)
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
  const capabilityId = 'c'.repeat(32)
  const capabilitySecret = 'd'.repeat(64)
  const received: SpawnParams[] = []
  const server = net.createServer((socket) => {
    let buffer = ''
    let authenticated = false
    socket.on('data', (chunk) => {
      buffer += chunk.toString()
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        if (!line.trim()) continue
        const request = JSON.parse(line) as {
          id: number
          method: string
          params?: Record<string, unknown> & SpawnParams
        }
        if (request.method === 'identity') {
          socket.write(JSON.stringify({ jsonrpc: '2.0', id: request.id, result: { token: '' } }) + '\n')
          continue
        }
        if (request.method === 'authenticate_owner') {
          const params = request.params ?? {}
          if (
            params.version === 1 &&
            params.capability_id === capabilityId &&
            params.secret === capabilitySecret
          ) {
            authenticated = true
            socket.write(JSON.stringify({
              jsonrpc: '2.0',
              id: request.id,
              result: { version: 1, authenticated: true, capability_id: capabilityId },
            }) + '\n')
          } else {
            socket.write(JSON.stringify({
              jsonrpc: '2.0',
              id: request.id,
              error: { code: -32011, message: 'unauthenticated', data: { version: 1, reason: 'capability-rejected' } },
            }) + '\n')
          }
          continue
        }
        if (!authenticated) {
          socket.write(JSON.stringify({
            jsonrpc: '2.0',
            id: request.id,
            error: { code: -32010, message: 'permission_denied' },
          }) + '\n')
          continue
        }
        received.push(request.params ?? {})
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
  const info = fs.lstatSync(socketPath, { bigint: true })
  fs.writeFileSync(
    `${socketPath}.owner.json`,
    JSON.stringify({
      version: 1,
      socket: socketPath,
      socket_identity: { dev: String(info.dev), ino: String(info.ino) },
      capability_id: capabilityId,
      secret: capabilitySecret,
      owner_pid: process.pid,
    }),
    { mode: 0o600 },
  )
  fs.chmodSync(`${socketPath}.owner.json`, 0o600)
  return { server, socketPath, received }
}

async function stopSpawnServer(server: net.Server, socketPath: string): Promise<void> {
  await new Promise<void>((resolve) => server.close(() => resolve()))
  for (const file of [socketPath, `${socketPath}.owner.json`]) {
    try {
      await os.promises.unlink(file)
    } catch {
      // The server may have removed the socket during close.
    }
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

  it('does not close a client marked singleton on the RPC fallback', async () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-mcp-spawn-singleton-test-'))
    const previousHome = process.env.AVENOR_HOME
    const spawn = mock(async () => ({ runtime_id: 'rt-singleton-fallback' }))
    const close = mock(() => {})
    const singletonFallbackTool = createSpawnTool(mock(async () => ({
      client: { spawn, close },
      isSingleton: true,
      sup: null,
      supervisorId: '/tmp/avenor-mcp-singleton.sock',
    })) as any)

    try {
      process.env.AVENOR_HOME = home
      const result = await singletonFallbackTool({ supervisorId: '/tmp/avenor-mcp-singleton.sock' })
      expect(result.runtime_id).toBe('rt-singleton-fallback')
      expect(close).not.toHaveBeenCalled()
    } finally {
      if (previousHome === undefined) delete process.env.AVENOR_HOME
      else process.env.AVENOR_HOME = previousHome
      fs.rmSync(home, { recursive: true, force: true })
    }
  })
})

describe('spawnTool selector validation', () => {
  it('rejects invalid selectors before acquiring a supervisor or making RPC', async () => {
    await expect(spawnTool({
      supervisorId: '/tmp/not-a-supervisor.sock',
      rosterFile: '/repo/roster.json',
      rosterEntry: 'planner',
      backend: 'pi',
    })).rejects.toThrow()
  })

  it('rejects a missing roster entry before RPC', async () => {
    await expect(spawnTool({
      supervisorId: '/tmp/not-a-supervisor.sock',
      rosterFile: '/repo/roster.json',
    })).rejects.toThrow()
  })
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
