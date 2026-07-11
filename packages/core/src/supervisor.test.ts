import { describe, it, expect, afterEach, afterAll } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  findAvenorBinary,
  installerBinaryPath,
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

describe.skipIf(skipIfNoBinary)('Supervisor.close with skipShutdown', () => {
  let sup: Supervisor
  let shutdownCalled = false

  afterAll(async () => {
    await sup?.close().catch(() => {})
  })

  it('skips client.shutdown but still cleans up process/socket/client', async () => {
    sup = await Supervisor.get()
    const client = sup.getClient()
    const originalShutdown = client.shutdown.bind(client)
    client.shutdown = async (reason: string) => {
      shutdownCalled = true
      return originalShutdown(reason)
    }

    await sup.close({ skipShutdown: true })

    expect(shutdownCalled).toBe(false)
    try {
      fs.accessSync(sup.supervisorId, fs.constants.F_OK)
      throw new Error('socket still exists after close')
    } catch (err: any) {
      expect(err.code).toBe('ENOENT')
    }
    // Runs map should be cleared
    expect((sup as any).runs.size).toBe(0)
    // Client reference should be nullified
    expect((sup as any).client).toBeNull()
  })
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

  it('falls back to installer-managed path when AVENOR_BIN unset and avenor not on PATH', () => {
    const prevBin = process.env.AVENOR_BIN
    const prevInstallDir = process.env.AVENOR_INSTALL_DIR
    const prevPath = process.env.PATH
    const tmpDir = path.join(os.tmpdir(), `avenor-test-install-${process.pid}`)
    const binaryPath = path.join(tmpDir, 'avenor')

    delete process.env.AVENOR_BIN

    try {
      const emptyPath = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-empty-path-'))
      fs.writeFileSync(path.join(emptyPath, 'which'), '#!/bin/sh\nexit 1\n', { mode: 0o755 })
      process.env.PATH = emptyPath
      // Create a fake executable at the installer path
      fs.mkdirSync(tmpDir, { recursive: true })
      fs.writeFileSync(binaryPath, '#!/bin/sh\nexit 0')
      fs.chmodSync(binaryPath, 0o755)
      process.env.AVENOR_INSTALL_DIR = tmpDir

      const result = findAvenorBinary()
      expect(result).toBe(binaryPath)
    } finally {
      if (prevBin) {
        process.env.AVENOR_BIN = prevBin
      } else {
        delete process.env.AVENOR_BIN
      }
      if (prevInstallDir) {
        process.env.AVENOR_INSTALL_DIR = prevInstallDir
      } else {
        delete process.env.AVENOR_INSTALL_DIR
      }
      if (prevPath) {
        process.env.PATH = prevPath
      } else {
        delete process.env.PATH
      }
      try {
        fs.unlinkSync(binaryPath)
        fs.rmdirSync(tmpDir)
      } catch {
        // ignore cleanup errors
      }
    }
  })
})

describe('installerBinaryPath', () => {
  it('returns AVENOR_INSTALL_DIR/avenor when AVENOR_INSTALL_DIR is set', () => {
    const prev = process.env.AVENOR_INSTALL_DIR
    try {
      process.env.AVENOR_INSTALL_DIR = '/opt/avenor/bin'
      expect(installerBinaryPath()).toBe(path.join('/opt/avenor/bin', 'avenor'))
    } finally {
      if (prev) {
        process.env.AVENOR_INSTALL_DIR = prev
      } else {
        delete process.env.AVENOR_INSTALL_DIR
      }
    }
  })

  it('returns the versioned default cache binary when AVENOR_INSTALL_DIR is unset', () => {
    const prev = process.env.AVENOR_INSTALL_DIR
    const prevVersion = process.env.AVENOR_VERSION
    try {
      delete process.env.AVENOR_INSTALL_DIR
      process.env.AVENOR_VERSION = '2.3.4'
      const expected = path.join(os.homedir(), '.cache', 'avenor', 'bin', 'avenor', '2.3.4', 'avenor')
      expect(installerBinaryPath()).toBe(expected)
    } finally {
      if (prev) {
        process.env.AVENOR_INSTALL_DIR = prev
      }
      if (prevVersion) {
        process.env.AVENOR_VERSION = prevVersion
      } else {
        delete process.env.AVENOR_VERSION
      }
    }
  })
})
