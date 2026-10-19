#!/usr/bin/env bun
import * as fs from 'node:fs'
import * as net from 'node:net'

import { createOwnerGate, fail, respond } from './owner-gate.js'

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

const gate = createOwnerGate({ socketPath, attemptToken })

try {
  fs.unlinkSync(socketPath)
} catch {}

let nextRuntime = 0
const runtimes = new Map<string, Runtime>()

const server = net.createServer((socket) => {
  let buffer = ''
  const session = gate.session(line => socket.write(line))

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

      if (session.handle(req)) continue

      if (gate.isGated(req.method) && !session.authenticated) {
        socket.write(gate.deny(req.id))
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
            gate.remove()
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
    gate.remove()
    server.close()
    process.exit(0)
  })
}

server.listen(socketPath, () => {
  // A test that must prove no second child started records every started pid.
  const spawnLog = process.env.AVENOR_TEST_SPAWN_LOG
  if (spawnLog) fs.appendFileSync(spawnLog, `${process.pid}\n`)
  // Publication after the listen is what makes a launcher's first attach find a
  // stale or absent record; a delay widens that window for tests.
  const publishDelayMs = Number(process.env.AVENOR_TEST_OWNER_PUBLISH_DELAY ?? 0)
  if (publishDelayMs > 0) {
    setTimeout(() => gate.publish(), publishDelayMs)
    return
  }
  gate.publish()
})
