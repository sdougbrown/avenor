#!/usr/bin/env bun
import * as crypto from 'node:crypto'
import * as fs from 'node:fs'
import * as net from 'node:net'

type Request = {
  jsonrpc: '2.0'
  id?: number
  method: string
  params?: Record<string, any>
}

type Runtime = {
  runtime_id: string
  session_id: string
  label: string
  status: string
  on_event?: string
  sentinel_file?: string
  auto_approve: boolean
}

function argValue(name: string): string | undefined {
  const index = process.argv.indexOf(name)
  if (index === -1) return undefined
  return process.argv[index + 1]
}

function respond(id: number | undefined, result: unknown) {
  return JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n'
}

function fail(id: number | undefined, code: number, message: string, data?: unknown) {
  return JSON.stringify({ jsonrpc: '2.0', id, error: { code, message, ...(data ? { data } : {}) } }) + '\n'
}

if (process.argv[2] !== 'stable') {
  console.error('fake avenor only supports stable')
  process.exit(1)
}

const socketPath = argValue('--control-socket')
if (!socketPath) {
  console.error('--control-socket is required')
  process.exit(1)
}

// The launcher hands its per-attempt token through the environment; the real
// server exposes it through identity only after it publishes its own record.
const attemptToken = process.env.AVENOR_CONTROL_IDENTITY_TOKEN ?? ''
delete process.env.AVENOR_CONTROL_IDENTITY_TOKEN

const capabilityID = crypto.randomBytes(16).toString('hex')
const capabilitySecret = crypto.randomBytes(32).toString('hex')
const recordPath = `${socketPath}.owner.json`
const gatedMethods = new Set([
  'spawn',
  'spawn_mcp',
  'cancel',
  'prompt',
  'interrupt_and_prompt',
  'answer_permission',
  'shutdown',
  'broker_send',
  'broker_ask',
  'broker_cancel',
  'broker_reply',
])

try {
  fs.unlinkSync(socketPath)
} catch {}

function publishOwnerRecord(): void {
  const info = fs.statSync(socketPath, { bigint: true })
  const body = {
    version: 1,
    socket: socketPath,
    socket_identity: { dev: String(info.dev), ino: String(info.ino) },
    capability_id: capabilityID,
    secret: capabilitySecret,
    owner_pid: process.pid,
  }
  const temp = `${recordPath}.${crypto.randomBytes(8).toString('hex')}`
  fs.writeFileSync(temp, JSON.stringify(body), { mode: 0o600 })
  fs.chmodSync(temp, 0o600)
  fs.renameSync(temp, recordPath)
}

let nextRuntime = 0
const runtimes = new Map<string, Runtime>()

function removeArtifacts(): void {
  for (const file of [socketPath, recordPath]) {
    try {
      fs.unlinkSync(file)
    } catch {}
  }
}

const server = net.createServer((socket) => {
  let buffer = ''
  let authenticated = false

  socket.on('data', (chunk) => {
    buffer += chunk.toString()
    const lines = buffer.split('\n')
    buffer = lines.pop() ?? ''

    for (const line of lines) {
      if (!line.trim()) continue

      let req: Request
      try {
        req = JSON.parse(line)
      } catch {
        socket.write(fail(undefined, -32700, 'parse error'))
        continue
      }

      if (req.method === 'identity') {
        socket.write(respond(req.id, { token: attemptToken }))
        continue
      }

      if (req.method === 'authenticate_owner') {
        const params = req.params ?? {}
        if (params.version !== 1) {
          socket.write(fail(req.id, -32012, 'unsupported owner authentication', { version: 1, reason: 'unsupported-version' }))
          continue
        }
        if (params.capability_id !== capabilityID || params.secret !== capabilitySecret) {
          socket.write(fail(req.id, -32011, 'unauthenticated', { version: 1, reason: 'capability-rejected' }))
          continue
        }
        authenticated = true
        socket.write(respond(req.id, { version: 1, authenticated: true, capability_id: capabilityID }))
        continue
      }

      if (gatedMethods.has(req.method) && !authenticated) {
        socket.write(fail(req.id, -32010, 'permission_denied'))
        continue
      }

      switch (req.method) {
        case 'status': {
          const runtimeId = req.params?.runtime_id
          if (runtimeId) {
            const runtime = runtimes.get(runtimeId)
            if (!runtime) {
              socket.write(fail(req.id, -32000, `runtime ${runtimeId} not found`))
              break
            }
            socket.write(respond(req.id, runtime))
            break
          }

          socket.write(respond(req.id, {
            phase: 'idle',
            status: 'idle',
            runtimes: runtimes.size,
          }))
          break
        }

        case 'list':
          socket.write(respond(req.id, Array.from(runtimes.values())))
          break

        case 'cancel':
          socket.write(respond(req.id, { ok: true }))
          break

        case 'spawn': {
          nextRuntime++
          const runtimeId = `rt_${nextRuntime}`
          const sessionId = req.params?.session_id ?? `ses_${nextRuntime}`
          const runtime: Runtime = {
            runtime_id: runtimeId,
            session_id: sessionId,
            label: req.params?.label ?? runtimeId,
            status: 'running',
            on_event: req.params?.on_event,
            sentinel_file: req.params?.sentinel_file,
            auto_approve: req.params?.auto_approve === true,
          }
          runtimes.set(runtimeId, runtime)
          if (runtime.on_event) {
            fs.writeFileSync(
              runtime.on_event,
              JSON.stringify({
                type: 'agent.status',
                ts: Date.now(),
                runtime_id: runtimeId,
                session_id: sessionId,
              }) + '\n',
            )
          }
          socket.write(respond(req.id, runtime))
          break
        }

        case 'shutdown':
          socket.write(respond(req.id, { ok: true }))
          setTimeout(() => {
            removeArtifacts()
            server.close()
            process.exit(0)
          }, 0)
          break

        default:
          socket.write(fail(req.id, -32601, 'method not found'))
      }
    }
  })
})

for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  if (signal === 'SIGTERM' && process.env.AVENOR_TEST_IGNORE_SIGTERM === '1') continue
  process.on(signal, () => {
    removeArtifacts()
    server.close()
    process.exit(0)
  })
}

server.listen(socketPath, () => {
  publishOwnerRecord()
})
