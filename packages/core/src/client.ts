import * as net from 'node:net'
import * as readline from 'node:readline'

export interface JsonRpcRequest {
  jsonrpc: '2.0'
  id?: number
  method: string
  params?: unknown
}

export interface JsonRpcResponse {
  jsonrpc: '2.0'
  id: number
  result?: unknown
  error?: { code: number; message: string; data?: unknown }
}

export interface Event {
  event: string
  session_id?: string
  runtime_id?: string
  [key: string]: unknown
}

export interface SpawnParams {
  agent: string
  prompt_file?: string
  prompt?: string
  dir?: string
  label?: string
  timeout?: number
  model?: string
  backend?: string
  server_url?: string
  session_id?: string
  sentinel_file?: string
  on_event?: string
  [key: string]: unknown
}

type PendingCall = {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: ReturnType<typeof setTimeout>
}

export class Client {
  private socket: net.Socket
  private rl: readline.Interface
  private nextID = 0
  private pending = new Map<number, PendingCall>()
  private started = false
  private callTimeout: number

  private eventQueue: Event[] = []
  private eventResolvers: Array<(value: IteratorResult<Event>) => void> = []
  private eventDone = false
  private dropped = 0
  private subscribed = false

  constructor(
    socket: net.Socket,
    opts?: { callTimeoutMs?: number },
  ) {
    this.socket = socket
    this.rl = readline.createInterface({ input: socket, crlfDelay: Infinity })
    this.callTimeout = opts?.callTimeoutMs ?? 30_000
    this.socket.on('error', (err: Error) => {
      this.pushEvent({ event: 'protocol-error', message: err.message })
    })
  }

  private startReadLoop(): void {
    if (this.started) return
    this.started = true

    this.rl.on('line', (line: string) => {
      let parsed: any
      try {
        parsed = JSON.parse(line)
      } catch {
        return
      }

      if (parsed.id !== undefined && parsed.id !== null) {
        const id = Number(parsed.id)
        const pc = this.pending.get(id)
        if (pc) {
          clearTimeout(pc.timer)
          this.pending.delete(id)
          if (parsed.error) {
            pc.reject(
              new Error(`rpc error [${parsed.error.code}]: ${parsed.error.message}`),
            )
          } else {
            pc.resolve(parsed.result)
          }
        }
        return
      }

      if (parsed.method === 'event' && parsed.params) {
        const ev: Event = { ...parsed.params }
        if (this.dropped > 0) {
          const lag = this.dropped
          this.dropped = 0
          const lagEvent: Event = { event: 'client.lagged', dropped_count: lag }
          this.pushEvent(lagEvent)
        }
        this.pushEvent(ev)
      }
    })

    this.rl.on('close', () => {
      for (const [, pc] of this.pending) {
        clearTimeout(pc.timer)
        pc.reject(new Error('read response: connection closed'))
      }
      this.pending.clear()
      this.eventDone = true
      for (const resolve of this.eventResolvers) {
        resolve({ value: undefined, done: true })
      }
      this.eventResolvers = []
    })
  }

  private pushEvent(ev: Event): void {
    if (this.eventResolvers.length > 0) {
      const resolve = this.eventResolvers.shift()!
      resolve({ value: ev, done: false })
    } else if (this.eventQueue.length < 256) {
      this.eventQueue.push(ev)
    } else {
      this.dropped++
    }
  }

  async call(method: string, params?: unknown): Promise<unknown> {
    this.startReadLoop()

    const id = ++this.nextID
    const req: JsonRpcRequest = { jsonrpc: '2.0', id, method }
    if (params !== undefined) {
      req.params = params
    }

    const data = JSON.stringify(req) + '\n'

    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error('read response: timeout'))
      }, this.callTimeout)

      this.pending.set(id, { resolve, reject, timer })

      this.socket.write(data, (err?: Error | null) => {
        if (err) {
          clearTimeout(timer)
          this.pending.delete(id)
          reject(new Error(`write request: ${err.message}`))
        }
      })
    })
  }

  private async subscribe(): Promise<void> {
    if (this.subscribed) return
    this.subscribed = true
    await this.call('subscribe')
  }

  async *events(): AsyncIterable<Event> {
    await this.subscribe()
    while (true) {
      if (this.eventDone) break

      if (this.eventQueue.length > 0) {
        yield this.eventQueue.shift()!
        continue
      }

      const result = await new Promise<IteratorResult<Event>>((resolve) => {
        this.eventResolvers.push(resolve)
      })

      if (result.done) break
      yield result.value
    }
  }

  async status(runtimeId?: string): Promise<Record<string, unknown>> {
    const params = runtimeId ? { runtime_id: runtimeId } : undefined
    return this.call('status', params) as Promise<Record<string, unknown>>
  }

  async list(): Promise<Array<Record<string, unknown>>> {
    return this.call('list') as Promise<Array<Record<string, unknown>>>
  }

  async spawn(params: SpawnParams): Promise<Record<string, unknown>> {
    return this.call('spawn', params) as Promise<Record<string, unknown>>
  }

  async shutdown(mode: string): Promise<void> {
    await this.call('shutdown', { mode })
  }

  async answerPermission(
    runtimeId: string,
    requestId: string,
    optionId: string,
  ): Promise<void> {
    await this.call('answer_permission', {
      runtime_id: runtimeId,
      request_id: requestId,
      option_id: optionId,
    })
  }

  async prompt(runtimeId: string, text: string): Promise<void> {
    await this.call('prompt', { runtime_id: runtimeId, text })
  }

  async cancel(runtimeId?: string): Promise<void> {
    const params = runtimeId ? { runtime_id: runtimeId } : undefined
    await this.call('cancel', params)
  }

  close(): void {
    this.rl.close()
    this.socket.destroy()
  }
}

export function dial(
  socketPath: string,
  opts?: { callTimeoutMs?: number },
): Promise<Client> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ path: socketPath })

    const onConnect = () => {
      socket.removeListener('error', onError)
      resolve(new Client(socket, opts))
    }
    const onError = (err: Error) => {
      socket.removeListener('connect', onConnect)
      reject(new Error(`dial control socket: ${err.message}`))
    }

    socket.on('connect', onConnect)
    socket.on('error', onError)
  })
}
