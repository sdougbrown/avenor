import { afterEach, describe, expect, it } from 'bun:test'
import * as fs from 'node:fs'
import * as net from 'node:net'
import * as os from 'node:os'
import * as path from 'node:path'

import {
  attachOwner,
  ForeignPeerError,
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

  it('rejects a socket replaced between validation and the dial without sending credentials', async () => {
    const dir = temporaryDir()
    const socketPath = path.join(dir, 'stable.sock')
    const replacementPath = path.join(dir, 'replacement.sock')
    const original = await listenSocket(socketPath)
    const replacement = await listenSocket(replacementPath)
    publishRecord(socketPath)

    await expect(
      attachOwner(socketPath, {
        beforeDial: () => fs.renameSync(replacementPath, socketPath),
      }),
    ).rejects.toBeInstanceOf(StaleOwnerRecordError)

    expect(replacement.connections).toBe(1)
    expect(replacement.bytes).toBe(0)
    expect(original.connections).toBe(0)
  })
})
