#!/usr/bin/env bun
import * as net from 'node:net'
import * as fs from 'node:fs'

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
}

function argValue(name: string): string | undefined {
  const index = process.argv.indexOf(name)
  if (index === -1) return undefined
  return process.argv[index + 1]
}

function respond(id: number | undefined, result: unknown) {
  return JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n'
}

function fail(id: number | undefined, code: number, message: string) {
  return JSON.stringify({ jsonrpc: '2.0', id, error: { code, message } }) + '\n'
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

try {
  fs.unlinkSync(socketPath)
} catch {}

let nextRuntime = 0
const runtimes = new Map<string, Runtime>()

const server = net.createServer((socket) => {
  let buffer = ''

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
            server.close()
            try {
              fs.unlinkSync(socketPath)
            } catch {}
            process.exit(0)
          }, 0)
          break

        default:
          socket.write(fail(req.id, -32601, 'method not found'))
      }
    }
  })
})

server.listen(socketPath)
