import { describe, it, expect, afterEach } from 'bun:test'
import * as net from 'node:net'
import * as path from 'node:path'
import * as os from 'node:os'
import { dial, type Event } from './client.js'

function tempSocketPath(): string {
  const rand = Math.random().toString(36).substring(2, 10)
  return path.join(os.tmpdir(), `avenor-client-test-${rand}.sock`)
}

function startMockServer(
  socketPath: string,
  handler: (req: any, socket: net.Socket) => void,
): Promise<net.Server> {
  return new Promise((resolve) => {
    const server = net.createServer((sock) => {
      let buffer = ''
      sock.on('data', (chunk: Buffer) => {
        buffer += chunk.toString()
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''
        for (const line of lines) {
          if (!line.trim()) continue
          try {
            const req = JSON.parse(line)
            handler(req, sock)
          } catch {
            // skip malformed lines
          }
        }
      })
    })
    server.listen(socketPath, () => resolve(server))
  })
}

describe('Client.status', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('returns correct snapshot', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: { phase: 'idle', runtimes: 0 },
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      const result = await client.status()
      expect(result).toEqual({ phase: 'idle', runtimes: 0 })
    } finally {
      client.close()
    }
  })

  it('passes runtime_id when provided', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      expect(req.params).toEqual({ runtime_id: 'rt-123' })
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: { phase: 'thinking' },
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      const result = await client.status('rt-123')
      expect(result).toEqual({ phase: 'thinking' })
    } finally {
      client.close()
    }
  })
})

describe('Client.events', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('yields test event after subscribe', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      if (req.method === 'subscribe') {
        const rpcRes = {
          jsonrpc: '2.0',
          id: req.id,
          result: { subscribed: true },
        }
        sock.write(JSON.stringify(rpcRes) + '\n')

        const notification = {
          jsonrpc: '2.0',
          method: 'event',
          params: { event: 'agent.status', phase: 'thinking' },
        }
        sock.write(JSON.stringify(notification) + '\n')
      }
    })

    const client = await dial(socketPath)
    try {
      const events: Event[] = []
      const iter = client.events()

      const ev = await iter.next()
      if (!ev.done) {
        events.push(ev.value)
      }

      client.close()

      expect(events).toHaveLength(1)
      expect(events[0].event).toBe('agent.status')
      expect(events[0].phase).toBe('thinking')
    } finally {
      client.close()
    }
  })
})

describe('Client.list', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('returns runtime array', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      expect(req.method).toBe('list')
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: [
          { id: 'rt-1', agent: 'codex', status: 'running' },
          { id: 'rt-2', agent: 'gemini', status: 'idle' },
        ],
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      const result = await client.list()
      expect(result).toHaveLength(2)
      expect(result[0].id).toBe('rt-1')
      expect(result[1].agent).toBe('gemini')
    } finally {
      client.close()
    }
  })
})

describe('Client.spawn', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('sends params, receives result', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      expect(req.method).toBe('spawn')
      expect(req.params.agent).toBe('codex')
      expect(req.params.model).toBe('sonnet')

      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: { runtime_id: 'rt-42', status: 'spawning' },
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      const result = await client.spawn({ agent: 'codex', model: 'sonnet' })
      expect(result.runtime_id).toBe('rt-42')
      expect(result.status).toBe('spawning')
    } finally {
      client.close()
    }
  })
})

describe('Client.shutdown', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('sends shutdown with mode', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      expect(req.method).toBe('shutdown')
      expect(req.params).toEqual({ mode: 'graceful' })
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: null,
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      await client.shutdown('graceful')
    } finally {
      client.close()
    }
  })
})

describe('Client.answerPermission', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('sends answer_permission with correct params', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      expect(req.method).toBe('answer_permission')
      expect(req.params).toEqual({
        runtime_id: 'rt-1',
        request_id: 'req-42',
        option_id: 'opt-7',
      })
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: null,
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      await client.answerPermission('rt-1', 'req-42', 'opt-7')
    } finally {
      client.close()
    }
  })
})

describe('Client.prompt', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('sends prompt with runtime_id and text', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      expect(req.method).toBe('prompt')
      expect(req.params).toEqual({
        runtime_id: 'rt-1',
        text: 'hello',
      })
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: null,
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      await client.prompt('rt-1', 'hello')
    } finally {
      client.close()
    }
  })
})

describe('Client.cancel', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('sends cancel with runtime_id', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      expect(req.method).toBe('cancel')
      expect(req.params).toEqual({ runtime_id: 'rt-1' })
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: null,
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      await client.cancel('rt-1')
    } finally {
      client.close()
    }
  })

  it('sends cancel without runtime_id', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      expect(req.method).toBe('cancel')
      expect(req.params).toBeUndefined()
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: null,
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      await client.cancel()
    } finally {
      client.close()
    }
  })
})

describe('Client.call error handling', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('rejects on error response', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        error: { code: -32000, message: 'something went wrong' },
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      await expect(client.status()).rejects.toThrow(
        'rpc error [-32000]: something went wrong',
      )
    } finally {
      client.close()
    }
  })
})

describe('Client.call timeout', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('rejects with timeout when server does not respond', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (_req, _sock) => {
      // intentionally never responds
    })

    const client = await dial(socketPath, { callTimeoutMs: 100 })
    try {
      await expect(client.status()).rejects.toThrow('read response: timeout')
    } finally {
      client.close()
    }
  })
})

describe('Client.close', () => {
  it('rejects subsequent calls after close', async () => {
    const socketPath = tempSocketPath()

    const server = await startMockServer(socketPath, (req, sock) => {
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: { ok: true },
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    const result = await client.status()
    expect(result).toEqual({ ok: true })

    client.close()
    await expect(client.status()).rejects.toThrow()
    server.close()
  })
})

describe('Client.dial failure', () => {
  it('rejects on ENOENT', async () => {
    await expect(dial('/tmp/no-such-socket-12345.sock')).rejects.toThrow(
      'dial control socket:',
    )
  })
})

describe('Client.call with multiple concurrent calls', () => {
  let server: net.Server

  afterEach(() => {
    server?.close()
  })

  it('routes responses to correct callers by id', async () => {
    const socketPath = tempSocketPath()

    server = await startMockServer(socketPath, (req, sock) => {
      const rpcRes = {
        jsonrpc: '2.0',
        id: req.id,
        result: { method: req.method, echo: req.params?.key },
      }
      sock.write(JSON.stringify(rpcRes) + '\n')
    })

    const client = await dial(socketPath)
    try {
      const [a, b] = await Promise.all([
        client.call('alpha', { key: 'a' }),
        client.call('beta', { key: 'b' }),
      ])
      expect((a as any).method).toBe('alpha')
      expect((a as any).echo).toBe('a')
      expect((b as any).method).toBe('beta')
      expect((b as any).echo).toBe('b')
    } finally {
      client.close()
    }
  })
})

describe('Client.event lagged', () => {
  it('emits client.lagged when queue overflows', async () => {
    const queueMax = 256
    const socketPath = tempSocketPath()

    const server = await startMockServer(socketPath, (req, sock) => {
      if (req.method === 'subscribe' || req.method === 'status') {
        sock.write(JSON.stringify({ jsonrpc: '2.0', id: req.id, result: {} }) + '\n')
      }
    })

    const client = await dial(socketPath)
    try {
      // Register the read loop handler without starting event consumption
      await client.status()

      const rl = (client as any).rl

      // Fill queue with 256 events + overflow 1 more (dropped = 1)
      for (let i = 0; i < queueMax + 1; i++) {
        rl.emit(
          'line',
          JSON.stringify({
            jsonrpc: '2.0',
            method: 'event',
            params: { event: 'tick', n: i },
          }),
        )
      }

      // Now start the consumer
      const gen = client.events()
      const events: Event[] = []

      // Drain the 256 queued events
      for (let i = 0; i < queueMax; i++) {
        const { value, done } = await gen.next()
        if (done) break
        events.push(value as Event)
      }

      // Consumer is waiting. Emit one more event to trigger the lagged notification
      rl.emit(
        'line',
        JSON.stringify({
          jsonrpc: '2.0',
          method: 'event',
          params: { event: 'trigger' },
        }),
      )

      // Collect remaining (lagged + trigger)
      for await (const ev of gen) {
        events.push(ev)
        if (ev.event === 'trigger') break
      }

      const lagged = events.filter((e) => e.event === 'client.lagged')
      expect(lagged.length).toBeGreaterThan(0)
      expect(lagged[0].dropped_count).toBe(1)
    } finally {
      client.close()
      server.close()
    }
  })
})
