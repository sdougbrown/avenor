import { describe, it, expect, afterEach, afterAll } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  findAvenorBinary,
  Supervisor,
  type RunInfo,
} from './supervisor.js'

function useFakeAvenorUnlessRealIntegration(): void {
  if (process.env.AVENOR_REAL_INTEGRATION === '1') return
  process.env.AVENOR_HOME = path.join(os.tmpdir(), `avenor-core-test-${process.pid}`)
  process.env.AVENOR_BIN = path.resolve(import.meta.dir, '..', 'test', 'fixtures', 'fake-avenor.ts')
}

useFakeAvenorUnlessRealIntegration()

function expectedAvenorHome(): string {
  return process.env.AVENOR_HOME ?? path.join(os.homedir(), '.avenor')
}

function hasAvenorBinary(): boolean {
  try {
    findAvenorBinary()
    return true
  } catch {
    return false
  }
}

const skipIfNoBinary = !hasAvenorBinary()

describe.skipIf(skipIfNoBinary)('Supervisor lifecycle', () => {
  let supervisor: Supervisor

  afterAll(async () => {
    await supervisor?.close().catch(() => {})
  })

  it('Supervisor.get starts the process', async () => {
    supervisor = await Supervisor.get()
    expect(supervisor).toBeTruthy()
    expect(supervisor.supervisorId).toContain(path.join(expectedAvenorHome(), 'sockets', 'avenor-mcp-'))
  })

  it('supervisor.getClient().status() works', async () => {
    const client = supervisor.getClient()
    const status = await client.status()
    expect(status).toBeObject()
  })

  it('supervisor.spawn() returns a run entry', async () => {
    const run = await supervisor.spawn({ prompt: 'exit 0' } as any)
    expect(run).toBeObject()
    expect(run.runtimeId).toBeString()
    expect(run.sentinelPath).toBeString()
    expect(run.eventLogPath).toBeString()
  }, 15_000)

  it('supervisor.close() terminates cleanly', async () => {
    await supervisor.close()
    try {
      fs.accessSync(supervisor.supervisorId, fs.constants.F_OK)
      throw new Error('socket still exists after close')
    } catch (err: any) {
      expect(err.code).toBe('ENOENT')
    }
  })
})

describe.skipIf(skipIfNoBinary)('Supervisor singleton', () => {
  let sup: Supervisor

  afterAll(async () => {
    await sup?.close().catch(() => {})
  })

  it('reuses existing instance', async () => {
    sup = await Supervisor.get()
    const sup2 = await Supervisor.get()
    expect(sup2).toBe(sup)
  })

  it('starts fresh after close', async () => {
    await sup.close()
    const sup2 = await Supervisor.get()
    expect(sup2).not.toBe(sup)
    sup = sup2
    expect(sup2.supervisorId).toContain(path.join(expectedAvenorHome(), 'sockets', 'avenor-mcp-'))
  }, 15_000)
})

describe('findAvenorBinary', () => {
  it.skipIf(!hasAvenorBinary())('returns a valid path', () => {
    const bin = findAvenorBinary()
    expect(bin).toBeString()
    expect(bin.length).toBeGreaterThan(0)
  })

  it('throws when AVENOR_BIN points to a missing path', () => {
    const prev = process.env.AVENOR_BIN
    try {
      process.env.AVENOR_BIN = '/tmp/nonexistent-avenor-xyz123'
      expect(() => findAvenorBinary()).toThrow('AVENOR_BIN path not executable')
    } finally {
      if (prev) {
        process.env.AVENOR_BIN = prev
      } else {
        delete process.env.AVENOR_BIN
      }
    }
  })
})
