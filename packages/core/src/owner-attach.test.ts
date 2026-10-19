import { afterEach, describe, expect, it } from 'bun:test'
import * as fs from 'node:fs'
import * as net from 'node:net'
import * as os from 'node:os'
import * as path from 'node:path'

import {
  attachOwner,
  ForeignPeerError,
  OwnerSocketAbsentError,
  ownerRecordPath,
  StaleOwnerRecordError,
} from './owner-attach.js'

interface ListeningSocket {
  server: net.Server
  connections: number
  bytes: number
}

const cleanups: Array<() => void> = []

afterEach(() => {
  while (cleanups.length > 0) cleanups.pop()?.()
})

function temporaryDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-owner-attach-'))
  cleanups.push(() => fs.rmSync(dir, { recursive: true, force: true }))
  return dir
}

async function listenSocket(socketPath: string): Promise<ListeningSocket> {
  const state: ListeningSocket = { server: net.createServer(), connections: 0, bytes: 0 }
  state.server.on('connection', (socket) => {
    state.connections++
    socket.on('data', (chunk) => {
      state.bytes += chunk.length
    })
  })
  await new Promise<void>((resolve, reject) => {
    state.server.once('error', reject)
    state.server.listen(socketPath, () => resolve())
  })
  cleanups.push(() => state.server.close())
  return state
}

function publishRecord(socketPath: string): void {
  const info = fs.lstatSync(socketPath, { bigint: true })
  fs.writeFileSync(
    ownerRecordPath(socketPath),
    JSON.stringify({
      version: 1,
      socket: socketPath,
      socket_identity: { dev: String(info.dev), ino: String(info.ino) },
      capability_id: 'a'.repeat(32),
      secret: 'b'.repeat(64),
      owner_pid: process.pid,
    }),
    { mode: 0o600 },
  )
  fs.chmodSync(ownerRecordPath(socketPath), 0o600)
}

function euid(): number {
  return process.geteuid?.() ?? 0
}

describe('owner socket binding', () => {
  it('refuses a socket owned by another uid before dialing', async () => {
    const dir = temporaryDir()
    const socketPath = path.join(dir, 'foreign.sock')
    const listening = await listenSocket(socketPath)
    publishRecord(socketPath)

    await expect(attachOwner(socketPath, { euid: euid() + 1 })).rejects.toBeInstanceOf(ForeignPeerError)
    expect(listening.connections).toBe(0)
  })

  it('fails closed when the socket directory is group- or other-writable without the sticky bit', async () => {
    const dir = temporaryDir()
    fs.chmodSync(dir, 0o777)
    const socketPath = path.join(dir, 'open-dir.sock')
    const listening = await listenSocket(socketPath)
    publishRecord(socketPath)

    await expect(attachOwner(socketPath)).rejects.toBeInstanceOf(ForeignPeerError)
    expect(listening.connections).toBe(0)
  })

  it('reports a socket that disappeared after the probe as replaceable, not stale', async () => {
    const dir = temporaryDir()
    const socketPath = path.join(dir, 'vanished.sock')
    const listening = await listenSocket(socketPath)
    publishRecord(socketPath)
    fs.unlinkSync(socketPath)

    const failure = await attachOwner(socketPath).catch((err: unknown) => err)
    expect(failure).toBeInstanceOf(OwnerSocketAbsentError)
    expect((failure as OwnerSocketAbsentError).code).toBe('ENOENT')
    expect(listening.connections).toBe(0)
  })

  it('rejects a socket replaced between validation and the dial without sending credentials', async () => {
    const dir = temporaryDir()
    const socketPath = path.join(dir, 'stable.sock')
    const replacementPath = path.join(dir, 'replacement.sock')
    const original = await listenSocket(socketPath)
    const replacement = await listenSocket(replacementPath)
    publishRecord(socketPath)

    const beforeDial = () => fs.renameSync(replacementPath, socketPath)
    await expect(attachOwner(socketPath, { beforeDial })).rejects.toBeInstanceOf(StaleOwnerRecordError)

    expect(replacement.connections).toBe(1)
    expect(replacement.bytes).toBe(0)
    expect(original.connections).toBe(0)
  })
})

describe('owner record validation', () => {
  function rewriteRecord(
    recordPath: string,
    change: (record: Record<string, unknown>) => Record<string, unknown>,
  ): void {
    const record = JSON.parse(fs.readFileSync(recordPath, 'utf8')) as Record<string, unknown>
    fs.writeFileSync(recordPath, JSON.stringify(change(record)))
    fs.chmodSync(recordPath, 0o600)
  }

  const cases: Array<{
    name: string
    corrupt: (recordPath: string, socketPath: string) => void
  }> = [
    {
      name: 'a record another uid can read',
      corrupt: (recordPath) => fs.chmodSync(recordPath, 0o644),
    },
    {
      name: 'a symlinked record',
      corrupt: (recordPath) => {
        fs.renameSync(recordPath, `${recordPath}.target`)
        fs.symlinkSync(`${recordPath}.target`, recordPath)
      },
    },
    {
      name: 'an unknown field',
      corrupt: (recordPath) => rewriteRecord(recordPath, record => ({ ...record, extra: true })),
    },
    {
      name: 'a record version the client does not speak',
      corrupt: (recordPath) => rewriteRecord(recordPath, record => ({ ...record, version: 2 })),
    },
    {
      name: 'a record naming another socket',
      corrupt: (recordPath, socketPath) =>
        rewriteRecord(recordPath, record => ({ ...record, socket: `${socketPath}.other` })),
    },
  ]

  for (const testCase of cases) {
    it(`rejects ${testCase.name} without sending credentials`, async () => {
      const dir = temporaryDir()
      const socketPath = path.join(dir, 'record.sock')
      const listening = await listenSocket(socketPath)
      publishRecord(socketPath)
      testCase.corrupt(ownerRecordPath(socketPath), socketPath)

      await expect(attachOwner(socketPath)).rejects.toBeInstanceOf(StaleOwnerRecordError)
      expect(listening.connections).toBe(0)
    })
  }
})
