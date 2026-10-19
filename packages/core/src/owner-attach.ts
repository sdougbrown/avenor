import * as fs from 'node:fs'
import * as net from 'node:net'
import * as path from 'node:path'

import { CallTimeoutError, Client } from './client.js'

const OWNER_RECORD_VERSION = 1
const OWNER_RECORD_MAX_BYTES = 64 * 1024
const OWNER_ATTACH_TIMEOUT_MS = 5_000
const OWNER_PROBE_TIMEOUT_MS = 250

export interface SocketIdentity {
  dev: string
  ino: string
}

export interface OwnerRecord {
  version: number
  socket: string
  socket_identity: SocketIdentity
  capability_id: string
  secret: string
  owner_pid: number
}

/** The owner record published beside a control socket is absent. */
export class OwnerRecordMissingError extends Error {
  constructor(readonly socketPath: string) {
    super(`owner record missing for control socket ${socketPath}`)
    this.name = 'OwnerRecordMissingError'
  }
}

/**
 * The control socket path has no directory entry, so no listener holds it and
 * a replacement is authorized.
 */
export class OwnerSocketAbsentError extends Error {
  readonly code = 'ENOENT'

  constructor(readonly socketPath: string) {
    super(`control socket ${socketPath} is absent`)
    this.name = 'OwnerSocketAbsentError'
  }
}

/** The record or the socket it names no longer describes the live path. */
export class StaleOwnerRecordError extends Error {
  constructor(readonly socketPath: string, readonly reason: string) {
    super(`stale owner record for control socket ${socketPath}: ${reason}`)
    this.name = 'StaleOwnerRecordError'
  }
}

/**
 * The control socket path is not bound to this euid, or its directory is
 * writable by another user. No credential byte may be sent to that peer.
 */
export class ForeignPeerError extends Error {
  constructor(
    readonly socketPath: string,
    readonly euid: number,
    readonly reason: string,
    readonly peerUID?: number,
  ) {
    super(`control socket ${socketPath} ${reason}, not uid ${euid}: refusing to send owner credentials`)
    this.name = 'ForeignPeerError'
  }
}

export type ControlSocketProbe = 'live' | 'dead' | 'uncertain'

export interface OwnerAttachOptions {
  timeoutMs?: number
  /** Binding identity; production uses the process euid. */
  euid?: number
}

// Package hook, mirroring the Go client's beforeOwnerAttachDial. A test may
// substitute the step between validation and the dial; production leaves it a
// no-op.
let beforeOwnerAttachDial: () => void = () => {}

export function setOwnerAttachDialHook(hook: () => void): void {
  beforeOwnerAttachDial = hook
}

export function ownerRecordPath(socketPath: string): string {
  return `${socketPath}.owner.json`
}

function lstatBig(path: string): fs.BigIntStats {
  return fs.lstatSync(path, { bigint: true })
}

function processEUID(): number {
  const geteuid = process.geteuid
  if (!geteuid) {
    throw new Error('control socket ownership requires a POSIX effective uid')
  }
  return geteuid()
}

function stale(socketPath: string, reason: string): StaleOwnerRecordError {
  return new StaleOwnerRecordError(socketPath, reason)
}

function inspectOwnerRecord(recordPath: string, socketPath: string): fs.BigIntStats {
  let info: fs.BigIntStats
  try {
    info = lstatBig(recordPath)
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      throw new OwnerRecordMissingError(socketPath)
    }
    throw stale(socketPath, `stat ${recordPath}: ${errorMessage(err)}`)
  }
  if (info.isSymbolicLink()) {
    throw stale(socketPath, `${recordPath} is a symlink`)
  }
  if (!info.isFile()) {
    throw stale(socketPath, `${recordPath} is not a regular file`)
  }
  if (Number(info.mode & 0o777n) !== 0o600) {
    throw stale(socketPath, `${recordPath} has mode ${(Number(info.mode) & 0o777).toString(8)}, want 600`)
  }
  if (Number(info.uid) !== processEUID()) {
    throw stale(socketPath, `${recordPath} is not owned by uid ${processEUID()}`)
  }
  if (info.size > BigInt(OWNER_RECORD_MAX_BYTES)) {
    throw stale(socketPath, `${recordPath} is too large`)
  }
  return info
}

function rejectSymlinkedParent(recordPath: string, socketPath: string): void {
  const parent = path.dirname(recordPath)
  let info: fs.BigIntStats
  try {
    info = lstatBig(parent)
  } catch (err) {
    throw stale(socketPath, `stat ${parent}: ${errorMessage(err)}`)
  }
  if (info.isSymbolicLink()) {
    throw stale(socketPath, `${recordPath} has a symlinked parent ${parent}`)
  }
}

function decodeOwnerRecord(body: string, socketPath: string): OwnerRecord {
  let parsed: unknown
  try {
    parsed = JSON.parse(body)
  } catch (err) {
    throw stale(socketPath, `decode ${ownerRecordPath(socketPath)}: ${errorMessage(err)}`)
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw stale(socketPath, `${ownerRecordPath(socketPath)} is not an object`)
  }
  const value = parsed as Record<string, unknown>
  const expected = ['version', 'socket', 'socket_identity', 'capability_id', 'secret', 'owner_pid']
  for (const key of Object.keys(value)) {
    if (!expected.includes(key)) {
      throw stale(socketPath, `${ownerRecordPath(socketPath)} has unknown field ${key}`)
    }
  }
  const identity = value.socket_identity
  if (
    typeof value.version !== 'number' ||
    typeof value.socket !== 'string' ||
    typeof value.capability_id !== 'string' ||
    typeof value.secret !== 'string' ||
    typeof value.owner_pid !== 'number' ||
    typeof identity !== 'object' ||
    identity === null ||
    typeof (identity as Record<string, unknown>).dev !== 'string' ||
    typeof (identity as Record<string, unknown>).ino !== 'string'
  ) {
    throw stale(socketPath, `${ownerRecordPath(socketPath)} is malformed`)
  }
  if (value.version !== OWNER_RECORD_VERSION) {
    throw stale(socketPath, `${ownerRecordPath(socketPath)} has version ${value.version}`)
  }
  if (value.socket !== socketPath) {
    throw stale(socketPath, `${ownerRecordPath(socketPath)} names socket ${value.socket}`)
  }
  return {
    version: value.version,
    socket: value.socket,
    socket_identity: {
      dev: (identity as Record<string, string>).dev,
      ino: (identity as Record<string, string>).ino,
    },
    capability_id: value.capability_id,
    secret: value.secret,
    owner_pid: value.owner_pid,
  }
}

/** Load and validate the owner record published beside socketPath. */
export function loadOwnerRecord(socketPath: string): OwnerRecord {
  const recordPath = ownerRecordPath(socketPath)
  rejectSymlinkedParent(recordPath, socketPath)
  const inspected = inspectOwnerRecord(recordPath, socketPath)
  const fd = fs.openSync(recordPath, 'r')
  try {
    const opened = fs.fstatSync(fd, { bigint: true })
    // A directory another uid can write allows the path to be replaced between
    // the lstat and the open, so the opened descriptor must be the inspected file.
    if (opened.dev !== inspected.dev || opened.ino !== inspected.ino) {
      throw stale(socketPath, `${recordPath} changed while reading`)
    }
    return decodeOwnerRecord(fs.readFileSync(fd, 'utf8'), socketPath)
  } finally {
    fs.closeSync(fd)
  }
}

function socketFileIdentity(socketPath: string): fs.BigIntStats {
  let info: fs.BigIntStats
  try {
    info = lstatBig(socketPath)
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      throw new OwnerSocketAbsentError(socketPath)
    }
    throw stale(socketPath, `stat ${socketPath}: ${errorMessage(err)}`)
  }
  if (!info.isSocket()) {
    throw stale(socketPath, `${socketPath} is not a socket`)
  }
  return info
}

/** Require identity to be the socket the record names. */
function requireRecordedSocket(
  socketPath: string,
  record: OwnerRecord,
  identity: SocketIdentity,
): SocketIdentity {
  if (identity.dev !== record.socket_identity.dev || identity.ino !== record.socket_identity.ino) {
    throw stale(socketPath, `${socketPath} does not match the published record`)
  }
  return identity
}

/**
 * Require the live socket to be the one the record names. Returns the socket's
 * device and inode as decimal strings.
 */
export function validateOwnerRecordSocket(socketPath: string, record: OwnerRecord): SocketIdentity {
  const live = socketFileIdentity(socketPath)
  return requireRecordedSocket(socketPath, record, { dev: String(live.dev), ino: String(live.ino) })
}

/**
 * Bind the socket path to this euid before any credential byte: the socket file
 * must be a socket this euid owns, in a directory this euid or root owns that is
 * not group- or other-writable unless it is sticky.
 */
export function bindOwnerSocket(socketPath: string, euid = processEUID()): SocketIdentity {
  const info = socketFileIdentity(socketPath)
  if (Number(info.uid) !== euid) {
    throw new ForeignPeerError(socketPath, euid, `is owned by uid ${info.uid}`, Number(info.uid))
  }
  const parent = path.dirname(socketPath)
  let parentInfo: fs.BigIntStats
  try {
    parentInfo = lstatBig(parent)
  } catch (err) {
    throw new ForeignPeerError(socketPath, euid, `has an unreadable directory ${parent}: ${errorMessage(err)}`)
  }
  if (parentInfo.isSymbolicLink()) {
    throw new ForeignPeerError(socketPath, euid, `is reached through the symlink ${parent}`)
  }
  const parentUID = Number(parentInfo.uid)
  if (parentUID !== euid && parentUID !== 0) {
    throw new ForeignPeerError(socketPath, euid, `lives in ${parent}, owned by uid ${parentUID}`)
  }
  const writable = (parentInfo.mode & 0o022n) !== 0n
  const sticky = (parentInfo.mode & 0o1000n) !== 0n
  if (writable && !sticky) {
    throw new ForeignPeerError(socketPath, euid, `lives in ${parent}, which is group/other-writable without the sticky bit`)
  }
  return { dev: String(info.dev), ino: String(info.ino) }
}

/** Probe a control socket path without presenting any credential. */
export function probeControlSocket(
  socketPath: string,
  timeoutMs = OWNER_PROBE_TIMEOUT_MS,
): Promise<ControlSocketProbe> {
  return new Promise<ControlSocketProbe>((resolve) => {
    const socket = net.createConnection({ path: socketPath })
    let done = false
    const finish = (state: ControlSocketProbe) => {
      if (done) return
      done = true
      clearTimeout(timer)
      socket.destroy()
      resolve(state)
    }
    const timer = setTimeout(() => finish('uncertain'), timeoutMs)
    socket.once('connect', () => finish('live'))
    socket.once('error', (err: NodeJS.ErrnoException) => {
      finish(err.code === 'ECONNREFUSED' || err.code === 'ENOENT' ? 'dead' : 'uncertain')
    })
  })
}

function connectOwnerSocket(socketPath: string, timeoutMs: number): Promise<net.Socket> {
  return new Promise<net.Socket>((resolve, reject) => {
    const socket = net.createConnection({ path: socketPath })
    const timer = setTimeout(() => {
      socket.destroy()
      reject(new CallTimeoutError(`dial ${socketPath}: timed out`))
    }, timeoutMs)
    socket.once('connect', () => {
      clearTimeout(timer)
      resolve(socket)
    })
    socket.once('error', (err) => {
      clearTimeout(timer)
      reject(err)
    })
  })
}

function remainingMs(deadline: number): number {
  return Math.max(1, deadline - Date.now())
}

/**
 * Load the owner record, bind the socket path to this euid and require it to be
 * the record's socket, dial, re-check the socket identity and authenticate. The
 * whole attach is bounded by the timeout.
 */
export async function attachOwner(
  socketPath: string,
  options: OwnerAttachOptions = {},
): Promise<Client> {
  const deadline = Date.now() + (options.timeoutMs ?? OWNER_ATTACH_TIMEOUT_MS)
  const euid = options.euid ?? processEUID()
  const record = loadOwnerRecord(socketPath)
  requireRecordedSocket(socketPath, record, bindOwnerSocket(socketPath, euid))
  beforeOwnerAttachDial()
  const socket = await connectOwnerSocket(socketPath, remainingMs(deadline))
  let client: Client
  try {
    // The path can be rebound between the checks and the dial, so the socket
    // this dial reached must still be the record's before any credential byte.
    validateOwnerRecordSocket(socketPath, record)
    client = new Client(socket)
  } catch (err) {
    socket.destroy()
    throw err
  }
  try {
    await client.authenticateOwner(
      { id: record.capability_id, secret: record.secret },
      remainingMs(deadline),
    )
  } catch (err) {
    client.close()
    throw err
  }
  return client
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}
