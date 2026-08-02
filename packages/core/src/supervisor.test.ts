import { describe, it, expect, afterEach, afterAll, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  findAvenorBinary,
  installerBinaryPath,
  Supervisor,
  type RunInfo,
} from './supervisor.js'
import { followUpTool } from './tools/follow-up.js'

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

describe('Supervisor roster metadata', () => {
  it('forwards roster selectors and records effective identity without rereading the roster', async () => {
    const spawnMock = mock(async () => ({
      runtime_id: 'rt-roster',
      session_id: 'ses-roster',
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      effective_agent: 'planner-agent',
      effective_model: 'planner-model',
      effective_backend: 'agy',
    }))
    const client = {
      spawn: spawnMock,
      status: mock(async () => ({})),
      isClosed: mock(() => false),
    }
    const sup = Object.create(Supervisor.prototype) as Supervisor
    ;(sup as any).client = client
    ;(sup as any).crashed = false
    ;(sup as any).runs = new Map()
    ;(sup as any).aliases = new Map()

    const run = await sup.spawn({
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      prompt: 'plan',
    }, 'roster-run')

    expect(spawnMock).toHaveBeenCalledWith(expect.objectContaining({
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
    }))
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('agent')
    expect(run).toMatchObject({
      rosterFile: '/repo/roster.json',
      rosterEntry: 'planner',
      agent: 'planner-agent',
      model: 'planner-model',
      backend: 'agy',
      effectiveAgent: 'planner-agent',
      effectiveModel: 'planner-model',
      effectiveBackend: 'agy',
    })
  })

  it('rejects an invalid selector before RPC', async () => {
    const spawnMock = mock(async () => ({ runtime_id: 'should-not-spawn' }))
    const sup = Object.create(Supervisor.prototype) as Supervisor
    ;(sup as any).client = { spawn: spawnMock }
    ;(sup as any).crashed = false
    ;(sup as any).runs = new Map()
    ;(sup as any).aliases = new Map()

    await expect(sup.spawn({ roster_file: '/repo/roster.json', prompt: 'invalid' }, 'invalid-run'))
      .rejects.toThrow()
    expect(spawnMock).not.toHaveBeenCalled()
  })
})

describe.skipIf(skipIfNoBinary)('Supervisor lifecycle', () => {
  let supervisor: Supervisor
  let originalRun: RunInfo

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
    originalRun = await supervisor.spawn({
      agent: 'explore',
      backend: 'pi',
      dir: '/tmp/original-repo',
      auto_approve: true,
      prompt: 'exit 0',
    })
    expect(originalRun).toBeObject()
    expect(originalRun.runtimeId).toBeString()
    expect(originalRun.sentinelPath).toBeString()
    expect(originalRun.eventLogPath).toBeString()
    expect(originalRun.agent).toBe('explore')
    expect(originalRun.backend).toBe('pi')
    expect(originalRun.dir).toBe('/tmp/original-repo')
    expect(originalRun.autoApprove).toBe(true)
  }, 15_000)

  it('retains auto-approval across singleton follow-ups when no sentinel exists', async () => {
    fs.rmSync(originalRun.sentinelPath, { force: true })

    const result = await followUpTool({
      runId: originalRun.runId,
      message: 'continue',
    })
    const followUpRun = (supervisor as any).runs.get(result.run_id) as RunInfo

    expect(followUpRun.sessionId).toBe(originalRun.sessionId)
    expect(followUpRun.agent).toBe('explore')
    expect(followUpRun.backend).toBe('pi')
    expect(followUpRun.dir).toBe('/tmp/original-repo')
    expect(followUpRun.autoApprove).toBe(true)
    expect(await supervisor.getClient().status(followUpRun.runtimeId)).toMatchObject({
      auto_approve: true,
    })

    const chainedResult = await followUpTool({
      runId: followUpRun.runId,
      message: 'continue again',
    })
    const chainedRun = (supervisor as any).runs.get(chainedResult.run_id) as RunInfo

    expect(chainedRun.autoApprove).toBe(true)
    expect(await supervisor.getClient().status(chainedRun.runtimeId)).toMatchObject({
      auto_approve: true,
    })
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

  it('coalesces concurrent cold starts', async () => {
    const [first, second] = await Promise.all([Supervisor.get(), Supervisor.get()])
    sup = first
    expect(second).toBe(first)
  })

  it('reuses existing instance', async () => {
    sup = await Supervisor.get()
    const sup2 = await Supervisor.get()
    expect(sup2).toBe(sup)
  })

  it('starts fresh after close', async () => {
    await sup.close()
    const [sup2, sup3] = await Promise.all([Supervisor.get(), Supervisor.get()])
    expect(sup2).toBe(sup3)
    expect(sup2).not.toBe(sup)
    sup = sup2
    expect(sup2.supervisorId).toContain(path.join(expectedAvenorHome(), 'sockets', 'avenor-mcp-'))
  }, 20_000)

  it('invalidates singleton when process is alive but client is closed', async () => {
    sup = await Supervisor.get()
    // Close the client while the process keeps running
    ;(sup as any).client.close()

    // Next get() should detect the stale state and start a fresh instance.
    const replacement = await Supervisor.get()
    expect(replacement).not.toBe(sup)
    expect(await replacement.getClient().status()).toBeObject()
    sup = replacement
  }, 15_000)

  it('getClient throws "connection closed" when client socket closes before exit handler', async () => {
    sup = await Supervisor.get()
    const client = sup.getClient()
    // Destroy the underlying socket directly so isClosed() returns true
    // but the child process is still running (no exit event yet)
    ;(client as any).socket.destroy()

    expect(() => sup.getClient()).toThrow('avenor supervisor connection closed')
    expect((sup as any).crashed).toBe(true)
    expect((sup as any).client).toBeNull()
  })

  it('failed cold startup does not poison future calls', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-fail-test-'))
    const failingScript = path.join(tmpDir, 'failing-avenor.sh')
    fs.writeFileSync(failingScript, '#!/bin/sh\nexit 1\n', { mode: 0o755 })

    try {
      await sup.close()
      await expect(Supervisor.get({ binaryPath: failingScript })).rejects.toThrow(
        'avenor exited with code 1 during startup',
      )

      const sup2 = await Supervisor.get()
      expect(sup2).not.toBe(sup)
      expect(await sup2.getClient().status()).toBeObject()
      sup = sup2
    } finally {
      try {
        fs.unlinkSync(failingScript)
        fs.rmdirSync(tmpDir)
      } catch {
        // ignore cleanup errors
      }
    }
  })

  it('reconnects after an external clean shutdown', async () => {
    const previous = sup
    const exited = new Promise<void>(resolve => {
      ;(previous as any).childProcess.on('exit', () => resolve())
    })
    await previous.getClient().shutdown('graceful')
    await exited
    expect((previous as any).crashed).toBe(true)

    const replacement = await Supervisor.get()
    expect(replacement).not.toBe(previous)
    expect(await replacement.getClient().status()).toBeObject()
    sup = replacement
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
    // Run indexes should be cleared
    expect((sup as any).runs.size).toBe(0)
    expect((sup as any).aliases.size).toBe(0)
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
