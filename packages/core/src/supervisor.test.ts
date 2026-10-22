import { describe, it, expect, afterEach, afterAll, mock } from 'bun:test'
import { spawn } from 'node:child_process'
import * as fs from 'node:fs'
import * as net from 'node:net'
import * as os from 'node:os'
import * as path from 'node:path'
import { createOwnerGate } from '../test/fixtures/owner-gate.js'
import {
  findAvenorBinary,
  installerBinaryPath,
  Supervisor,
  type RunInfo,
} from './supervisor.js'
import { RpcError, OwnerAuthError } from './client.js'
import { loadOwnerRecord, OwnerRecordMissingError, probeControlSocket } from './owner-attach.js'
import { followUpTool } from './tools/follow-up.js'
import { getSupervisorClient } from './tools/get-supervisor-client.js'
import { shutdownTool } from './tools/shutdown.js'
import { socketsRoot } from './paths.js'

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

describe('run lookup planes', () => {
  function bareSupervisor(): Supervisor {
    const sup = Object.create(Supervisor.prototype) as Supervisor
    ;(sup as any).runs = new Map()
    ;(sup as any).aliases = new Map()
    return sup
  }

  function runInfo(runId: string, label: string): RunInfo {
    return {
      runId,
      label,
      sentinelPath: `/tmp/${runId}.sentinel`,
      eventLogPath: `/tmp/${runId}.events`,
    } as RunInfo
  }

  it('never resolves a label on the ID plane', () => {
    const sup = bareSupervisor()
    const alpha = runInfo('run-a', 'alpha')
    sup.registerResumed(alpha)
    sup.registerResumed(runInfo('run-b', 'run-a'))

    expect(sup.getRunById('run-a')).toBe(alpha)
    expect(sup.getRunById('run-a')?.runId).toBe('run-a')
    expect(sup.getRunByReference('alpha')).toBe(alpha)
  })

  it('does not reach a run by a label that is not any run ID', () => {
    const sup = bareSupervisor()
    const run = runInfo('run-b', 'run-a')
    sup.registerResumed(run)

    expect(
      sup.getRunById('run-a'),
      'a label that is not any run ID must not resolve on the ID plane',
    ).toBeUndefined()
    expect(sup.getRunByReference('run-a')).toBe(run)
  })

  it('leaves both planes resolvable after registerResumed', () => {
    const sup = bareSupervisor()
    const run = runInfo('run-c', 'gamma')
    sup.registerResumed(run)

    expect(sup.getRunById(run.runId)).toBe(run)
    expect(sup.getRunByReference(run.runId)).toBe(run)
    expect(sup.getRunByReference(run.label)).toBe(run)
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
    // The run must be complete before each follow-up: resuming a still-running
    // session is rejected by the supervisor's session-ownership guard. Against
    // the real binary, wait for the sentinel, then remove it to exercise the
    // no-sentinel fallback. The fake fixture never writes sentinels.
    const awaitSentinel = async (sentinelPath: string) => {
      if (process.env.AVENOR_REAL_INTEGRATION !== '1') return
      for (let i = 0; i < 60; i++) {
        if (fs.existsSync(sentinelPath)) break
        await new Promise(resolve => setTimeout(resolve, 1_000))
      }
      expect(fs.existsSync(sentinelPath)).toBe(true)
      fs.rmSync(sentinelPath, { force: true })
    }
    await awaitSentinel(originalRun.sentinelPath)

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
    await awaitSentinel(followUpRun.sentinelPath)
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

describe('Supervisor singleton and run accessors', () => {
  afterEach(async () => {
    await Supervisor.currentInstance()?.close().catch(() => {})
    ;(Supervisor as any).instance = null
  })

  it('getInstance returns the singleton only when it owns the id', async () => {
    const sup = await Supervisor.get()
    expect(Supervisor.getInstance(sup.supervisorId)).toBe(sup)
    expect(Supervisor.getInstance('/tmp/not-the-singleton.sock')).toBeNull()
  })

  it('getInstance returns null when no singleton is running', () => {
    ;(Supervisor as any).instance = null
    expect(Supervisor.getInstance('/tmp/anything.sock')).toBeNull()
    expect(Supervisor.isCurrentInstance('/tmp/anything.sock')).toBe(false)
  })

  it('isCurrentInstance stays a boolean predicate over getInstance', async () => {
    const sup = await Supervisor.get()
    expect(Supervisor.isCurrentInstance(sup.supervisorId)).toBe(true)
    expect(Supervisor.isCurrentInstance('/tmp/other.sock')).toBe(false)
  })

  it('lets the server finish its own cleanup before signalling the child', async () => {
    process.env.AVENOR_TEST_IGNORE_SIGTERM = '1'
    try {
      const sup = await Supervisor.get()
      const socketPath = sup.supervisorId
      await sup.close()
      expect(fs.existsSync(socketPath)).toBe(false)
      expect(fs.existsSync(`${socketPath}.owner.json`)).toBe(false)
    } finally {
      delete process.env.AVENOR_TEST_IGNORE_SIGTERM
    }
  })

  it('forEachRun iterates stored runs without exposing the map', async () => {
    const sup = await Supervisor.get()
    const first: RunInfo = {
      runId: 'run-iter-1',
      label: 'iter-one',
      sentinelPath: '/tmp/iter-1.done',
      eventLogPath: '/tmp/iter-1.log',
      runtimeId: 'rt-iter-1',
      sessionId: 'ses-iter-1',
    } as RunInfo
    const second: RunInfo = { ...first, runId: 'run-iter-2', label: 'iter-two' }
    ;(sup as any).runs = new Map([[first.runId, first], [second.runId, second]])
    ;(sup as any).aliases = new Map()

    const seen: string[] = []
    sup.forEachRun(info => seen.push(info.runId))
    expect(seen).toEqual(['run-iter-1', 'run-iter-2'])
  })
})


describe.skipIf(process.env.AVENOR_REAL_INTEGRATION === '1')('Owned start against a published record', () => {
  afterEach(async () => {
    await Supervisor.currentInstance()?.close().catch(() => {})
    ;(Supervisor as any).instance = null
  })

  it('refuses to replace a live supervisor whose record is gone', async () => {
    const spawnLog = path.join(os.tmpdir(), `avenor-fake-spawns-${process.pid}.log`)
    fs.rmSync(spawnLog, { force: true })
    process.env.AVENOR_TEST_SPAWN_LOG = spawnLog
    let sup: Supervisor | undefined
    try {
      sup = await Supervisor.get()
      const socketPath = sup.supervisorId
      fs.rmSync(`${socketPath}.owner.json`)
      // Another process finding this path sees a live listener with no record.
      ;(Supervisor as any).instance = null

      await expect(Supervisor.get()).rejects.toBeInstanceOf(OwnerRecordMissingError)
      expect(await probeControlSocket(socketPath)).toBe('live')
      expect(fs.readFileSync(spawnLog, 'utf8').trim().split('\n')).toHaveLength(1)
    } finally {
      delete process.env.AVENOR_TEST_SPAWN_LOG
      await sup?.close().catch(() => {})
      fs.rmSync(spawnLog, { force: true })
    }
  })

  it('treats a socket that vanishes after a live probe as uncertain, not dead', async () => {
    const spawnLog = path.join(os.tmpdir(), `avenor-fake-spawns-vanished-${process.pid}.log`)
    fs.rmSync(spawnLog, { force: true })
    process.env.AVENOR_TEST_SPAWN_LOG = spawnLog
    const socketPath = path.join(socketsRoot(), `avenor-mcp-${process.pid}.sock`)
    fs.mkdirSync(socketsRoot(), { recursive: true, mode: 0o700 })
    const gate = createOwnerGate({ socketPath })
    const connections = new Set<net.Socket>()
    // A listener that answers the probe and then takes the path away.
    const holder = net.createServer((socket) => {
      connections.add(socket)
      fs.unlinkSync(socketPath)
    })
    await new Promise<void>((resolve, reject) => {
      holder.once('error', reject)
      holder.listen(socketPath, resolve)
    })
    gate.publish()
    try {
      const failure = await Supervisor.get().catch((err: unknown) => err)
      expect((failure as NodeJS.ErrnoException).code).toBe('ENOENT')
      expect(fs.existsSync(spawnLog)).toBe(false)
    } finally {
      delete process.env.AVENOR_TEST_SPAWN_LOG
      holder.close()
      for (const socket of connections) socket.destroy()
      gate.remove()
      fs.rmSync(spawnLog, { force: true })
    }
  })

  it('fails at once and reaps the child when its own capability is rejected', async () => {
    const spawnLog = path.join(os.tmpdir(), `avenor-fake-spawns-rejected-${process.pid}.log`)
    fs.rmSync(spawnLog, { force: true })
    process.env.AVENOR_TEST_SPAWN_LOG = spawnLog
    process.env.AVENOR_TEST_OWNER_SECRET_MISMATCH = '1'
    try {
      const startedAt = Date.now()
      const failure = await Supervisor.get().catch((err: unknown) => err)
      // The attempt token proved the child and the record named its socket, so a
      // rejected capability is terminal rather than something to retry.
      expect(failure).toBeInstanceOf(OwnerAuthError)
      expect((failure as OwnerAuthError).kind).toBe('unauthenticated')
      expect(Date.now() - startedAt).toBeLessThan(5_000)
      const pid = Number(fs.readFileSync(spawnLog, 'utf8').trim())
      expect(pid).toBeGreaterThan(0)
      expect(() => process.kill(pid, 0)).toThrow()
    } finally {
      delete process.env.AVENOR_TEST_SPAWN_LOG
      delete process.env.AVENOR_TEST_OWNER_SECRET_MISMATCH
      fs.rmSync(spawnLog, { force: true })
    }
  })

  it('attaches the winner when the child exits during startup', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-exit-startup-'))
    const launcher = path.join(tmpDir, 'exit-during-startup.sh')
    const socketPath = path.join(socketsRoot(), `avenor-mcp-${process.pid}.sock`)
    // The attempt's child starts a competing supervisor that outlives it, waits
    // until that supervisor has published, then exits during startup.
    fs.writeFileSync(launcher, [
      '#!/bin/sh',
      `"${process.execPath}" "${process.env.AVENOR_BIN}" "$@" >/dev/null 2>&1 &`,
      'i=0',
      'while [ "$i" -lt 400 ]; do',
      '  [ -f "$3.owner.json" ] && break',
      '  sleep 0.025',
      '  i=$((i+1))',
      'done',
      'exit 1',
    ].join('\n'), { mode: 0o755 })
    try {
      const sup = await Supervisor.get({ binaryPath: launcher })
      // The winner answers, not the exited child: the launcher attached it
      // through the record it published and left the socket in place.
      expect(await sup.getClient().status()).toBeObject()
      expect(fs.existsSync(socketPath)).toBe(true)
    } finally {
      await Supervisor.currentInstance()?.close().catch(() => {})
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  it('retries a predecessor record until the child publishes its own', async () => {
    const socketPath = path.join(socketsRoot(), `avenor-mcp-${process.pid}.sock`)
    fs.mkdirSync(socketsRoot(), { recursive: true, mode: 0o700 })
    // A predecessor's record: valid shape, an inode no live socket has.
    const recordPath = `${socketPath}.owner.json`
    fs.writeFileSync(recordPath, JSON.stringify({
      version: 1,
      socket: socketPath,
      socket_identity: { dev: '0', ino: '0' },
      capability_id: 'e'.repeat(32),
      secret: 'f'.repeat(64),
      owner_pid: process.pid,
    }), { mode: 0o600 })
    fs.chmodSync(recordPath, 0o600)
    process.env.AVENOR_TEST_OWNER_PUBLISH_DELAY = '600'
    try {
      const sup = await Supervisor.get()
      // The owned start only attaches after the child replaced the predecessor's
      // record and answered on the connection that proved its attempt token.
      expect(loadOwnerRecord(socketPath).capability_id).not.toBe('e'.repeat(32))
      await sup.getClient().cancel()
    } finally {
      delete process.env.AVENOR_TEST_OWNER_PUBLISH_DELAY
    }
  })
})

describe.skipIf(process.env.AVENOR_REAL_INTEGRATION !== '1')('Owner attach against a real supervisor', () => {
  function useIsolatedHome(): { restore: () => void } {
    const previous = process.env.AVENOR_HOME
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-owner-real-'))
    process.env.AVENOR_HOME = home
    fs.mkdirSync(socketsRoot(), { recursive: true, mode: 0o700 })
    return {
      restore: () => {
        fs.rmSync(home, { recursive: true, force: true })
        if (previous === undefined) delete process.env.AVENOR_HOME
        else process.env.AVENOR_HOME = previous
      },
    }
  }

  async function waitForRecord(socketPath: string): Promise<void> {
    for (let i = 0; i < 100; i++) {
      try {
        fs.lstatSync(socketPath)
        fs.lstatSync(`${socketPath}.owner.json`)
        return
      } catch {
        await new Promise(resolve => setTimeout(resolve, 100))
      }
    }
    throw new Error(`supervisor did not publish ${socketPath}`)
  }

  it('an owned start attaches and reaches a gated method', async () => {
    const home = useIsolatedHome()
    let sup: Supervisor | undefined
    try {
      sup = await Supervisor.get()
      const socketPath = sup.supervisorId
      // cancel is owner-gated and needs no spawned runtime.
      await sup.getClient().cancel()
      expect(await sup.getClient().status()).toBeObject()
      // The server removes its own artifacts once it shuts down cleanly, which
      // only happens if the launcher waits instead of signalling it early.
      await sup.close()
      expect(fs.existsSync(socketPath)).toBe(false)
      expect(fs.existsSync(`${socketPath}.owner.json`)).toBe(false)
    } finally {
      await sup?.close().catch(() => {})
      await Supervisor.currentInstance()?.close().catch(() => {})
      home.restore()
    }
  }, 30_000)

  it('attaches a gated call to a supervisor this process did not start', async () => {
    const home = useIsolatedHome()
    const socketPath = path.join(socketsRoot(), `avenor-mcp-external-${process.pid}.sock`)
    const child = spawn(findAvenorBinary(), ['stable', '--control-socket', socketPath, '--idle-timeout', '2m'], {
      stdio: 'ignore',
    })
    const exited = new Promise<void>(resolve => child.once('exit', () => resolve()))
    try {
      await waitForRecord(socketPath)

      // Read-only access stays credential-free and keeps working.
      const readOnly = await getSupervisorClient(socketPath)
      expect(await readOnly.client.status()).toBeObject()
      await expect(readOnly.client.cancel()).rejects.toMatchObject({ code: -32010 })
      readOnly.client.close()

      // The gated path attaches with the published capability.
      const owner = await getSupervisorClient(socketPath, { owner: true })
      await owner.client.cancel()
      owner.client.close()

      // A gated tool also reaches a supervisor started by someone else.
      expect(await shutdownTool({ supervisorId: socketPath })).toMatchObject({ ok: true })
      await exited
      expect(() => fs.lstatSync(socketPath)).toThrow()
      expect(() => fs.lstatSync(`${socketPath}.owner.json`)).toThrow()
    } finally {
      child.kill('SIGKILL')
      await exited
      home.restore()
    }
  }, 30_000)
})
