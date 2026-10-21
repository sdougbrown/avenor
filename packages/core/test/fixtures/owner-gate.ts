import * as crypto from 'node:crypto'
import * as fs from 'node:fs'

/** Methods the control server refuses before owner authentication. */
export const GATED_METHODS: readonly string[] = [
  'spawn', 'spawn_mcp', 'cancel', 'prompt', 'interrupt_and_prompt', 'answer_permission',
  'shutdown', 'message_send', 'message_wait', 'message_withdraw', 'message_reply',
]

export interface OwnerGateRequest {
  id?: number
  method: string
  params?: Record<string, unknown>
}

export interface OwnerGateOptions {
  socketPath: string
  /** The launcher's per-attempt token, exposed through identity. */
  attemptToken?: string
  /** The secret published in the record; differs to model an unusable record. */
  recordSecret?: string
}

export interface OwnerGateSession {
  readonly authenticated: boolean
  /** Answer identity or authenticate_owner; true when the line was handled. */
  handle(request: OwnerGateRequest): boolean
}

export interface OwnerGate {
  /** Write the 0600 owner record naming the live socket. */
  publish(): void
  /** Remove the socket and the owner record. */
  remove(): void
  /** Per-connection authentication state for one accepted socket. */
  session(write: (line: string) => void): OwnerGateSession
  /** The -32010 refusal a gated method gets before authentication. */
  deny(id: number | undefined): string
  isGated(method: string): boolean
}

/** One JSON-RPC result line, as the control protocol writes it. */
export function respond(id: number | undefined, result: unknown): string {
  return JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n'
}

/** One JSON-RPC error line, as the control protocol writes it. */
export function fail(id: number | undefined, code: number, message: string, data?: unknown): string {
  return JSON.stringify({ jsonrpc: '2.0', id, error: { code, message, ...(data ? { data } : {}) } }) + '\n'
}

/**
 * The owner half of a fake control server: the identity token, the published
 * record and the authenticate_owner handshake a launcher needs before it may
 * call a gated method.
 */
export function createOwnerGate(options: OwnerGateOptions): OwnerGate {
  const capabilityId = crypto.randomBytes(16).toString('hex')
  const secret = crypto.randomBytes(32).toString('hex')
  const recordSecret = options.recordSecret ?? secret
  const recordPath = `${options.socketPath}.owner.json`

  return {
    publish(): void {
      const info = fs.statSync(options.socketPath, { bigint: true })
      const body = {
        version: 1,
        socket: options.socketPath,
        socket_identity: { dev: String(info.dev), ino: String(info.ino) },
        capability_id: capabilityId,
        secret: recordSecret,
        owner_pid: process.pid,
      }
      const temp = `${recordPath}.${crypto.randomBytes(8).toString('hex')}`
      fs.writeFileSync(temp, JSON.stringify(body), { mode: 0o600 })
      fs.chmodSync(temp, 0o600)
      fs.renameSync(temp, recordPath)
    },
    remove(): void {
      for (const file of [options.socketPath, recordPath]) {
        try {
          fs.unlinkSync(file)
        } catch {}
      }
    },
    deny: id => fail(id, -32010, 'permission_denied'),
    isGated: method => GATED_METHODS.includes(method),
    session(write): OwnerGateSession {
      let authenticated = false
      return {
        get authenticated() {
          return authenticated
        },
        handle(request: OwnerGateRequest): boolean {
          if (request.method === 'identity') {
            write(respond(request.id, { token: options.attemptToken ?? '' }))
            return true
          }
          if (request.method !== 'authenticate_owner') return false
          const params = request.params ?? {}
          const version = params.version ?? 0
          if (version !== 1) {
            write(fail(request.id, -32012, 'unsupported owner authentication', { version: 1, reason: `version-${String(version)}` }))
            return true
          }
          if (params.capability_id !== capabilityId || params.secret !== secret) {
            write(fail(request.id, -32011, 'unauthenticated', { version: 1, reason: 'capability-mismatch' }))
            return true
          }
          authenticated = true
          write(respond(request.id, { version: 1, authenticated: true, capability_id: capabilityId }))
          return true
        },
      }
    },
  }
}
