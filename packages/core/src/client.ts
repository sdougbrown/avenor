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

export interface SubscribeOptions {
  runtime_id?: string
  replay?: boolean
  limit?: number
  after_seq?: number
}

export interface HistoryOptions {
  runtime_id: string
  limit?: number
  after_seq?: number
}

export interface HistoryResult<TEvent = Event> {
  runtime_id?: string
  events: TEvent[]
  latest_seq?: number
}

export interface SpawnParams {
  agent?: string
  prompt_file?: string
  prompt?: string
  dir?: string
  label?: string
  timeout?: number
  model?: string
  agent_profile?: string
  backend?: string
  server_url?: string
  session_id?: string
  sentinel_file?: string
  on_event?: string
  auto_approve?: boolean
  [key: string]: unknown
}

type PendingCall = {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: ReturnType<typeof setTimeout>
}

type EventResolver = {
  resolve: (value: IteratorResult<Event>) => void
  active: boolean
}

function normalizeSubscribeOptions(options?: SubscribeOptions): SubscribeOptions | undefined {
  if (!options) return undefined
  const normalized: SubscribeOptions = {}
  if (options.runtime_id) normalized.runtime_id = options.runtime_id
  if (options.replay !== undefined) normalized.replay = options.replay
  if (options.limit !== undefined) {
    normalized.limit = Math.max(1, Math.min(256, Math.trunc(options.limit)))
  }
  if (options.after_seq !== undefined) {
    normalized.after_seq = Math.max(0, Math.trunc(options.after_seq))
  }
  return Object.keys(normalized).length > 0 ? normalized : undefined
}

function subscribeOptionsKey(options?: SubscribeOptions): string {
  return JSON.stringify(normalizeSubscribeOptions(options) ?? {})
}

function removeResolver(resolvers: EventResolver[], target: EventResolver): void {
  const index = resolvers.indexOf(target)
  if (index >= 0) {
    resolvers.splice(index, 1)
  }
}

export class Client {
  private socket: net.Socket
  private rl: readline.Interface
  private nextID = 0
  private pending = new Map<number, PendingCall>()
  private started = false
  private callTimeout: number

  private eventQueue: Event[] = []
  private eventResolvers: EventResolver[] = []
  private eventDone = false
  private dropped = 0
  private subscribed = false
  private subscriptionKey = subscribeOptionsKey()

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
          this.pushEvent({ event: 'client.lagged', dropped_count: lag })
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
      for (const resolver of this.eventResolvers) {
        resolver.active = false
        resolver.resolve({ value: undefined, done: true })
      }
      this.eventResolvers = []
    })
  }

  private pushEvent(ev: Event): void {
    while (this.eventResolvers.length > 0) {
      const resolver = this.eventResolvers.shift()!
      if (!resolver.active) continue
      resolver.active = false
      resolver.resolve({ value: ev, done: false })
      return
    }

    if (this.eventQueue.length < 256) {
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

  async subscribe(options?: SubscribeOptions): Promise<void> {
    const key = subscribeOptionsKey(options)
    if (this.subscribed) {
      if (key !== subscribeOptionsKey() && key !== this.subscriptionKey) {
        throw new Error('client already subscribed with different options')
      }
      return
    }
    this.subscribed = true
    this.subscriptionKey = key
    await this.call('subscribe', normalizeSubscribeOptions(options))
  }

  events(options?: SubscribeOptions): AsyncIterableIterator<Event> {
    let active = true
    let pendingResolver: EventResolver | null = null
    let subscribedPromise: Promise<void> | null = null

    const ensureSubscribed = async (): Promise<void> => {
      if (!subscribedPromise) {
        subscribedPromise = this.subscribe(options)
      }
      await subscribedPromise
    }

    const next = async (): Promise<IteratorResult<Event>> => {
      if (!active || this.eventDone) {
        return { value: undefined, done: true }
      }

      try {
        await ensureSubscribed()
      } catch (error) {
        active = false
        throw error
      }

      if (!active || this.eventDone) {
        return { value: undefined, done: true }
      }

      if (this.eventQueue.length > 0) {
        return { value: this.eventQueue.shift()!, done: false }
      }

      return new Promise<IteratorResult<Event>>((resolve) => {
        pendingResolver = { resolve, active: true }
        this.eventResolvers.push(pendingResolver)
      })
    }

    const stop = async (): Promise<IteratorResult<Event>> => {
      if (!active) {
        return { value: undefined, done: true }
      }
      active = false
      if (pendingResolver) {
        pendingResolver.active = false
        removeResolver(this.eventResolvers, pendingResolver)
        pendingResolver.resolve({ value: undefined, done: true })
        pendingResolver = null
      }
      return { value: undefined, done: true }
    }

    return {
      next,
      return: stop,
      async throw(error?: unknown): Promise<IteratorResult<Event>> {
        await stop()
        throw error
      },
      [Symbol.asyncIterator]() {
        return this
      },
    }
  }

  async history(options: HistoryOptions): Promise<HistoryResult> {
    const params: HistoryOptions = {
      runtime_id: options.runtime_id,
      limit: options.limit !== undefined ? Math.max(1, Math.min(256, Math.trunc(options.limit))) : undefined,
      after_seq: options.after_seq !== undefined ? Math.max(0, Math.trunc(options.after_seq)) : undefined,
    }
    return this.call('history', params) as Promise<HistoryResult>
  }

  async status(runtimeId?: string): Promise<Record<string, unknown>> {
    const params = runtimeId ? { runtime_id: runtimeId } : undefined
    return this.call('status', params) as Promise<Record<string, unknown>>
  }

  // Result is the lossless terminal-output path. Status and history stay
  // bounded so polling and observers cannot accidentally attach large replies.
  async result(runtimeId?: string): Promise<Record<string, unknown>> {
    const params = runtimeId ? { runtime_id: runtimeId } : undefined
    return this.call('result', params) as Promise<Record<string, unknown>>
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
    message?: string,
  ): Promise<void> {
    const params: Record<string, string> = {
      runtime_id: runtimeId,
      request_id: requestId,
      option_id: optionId,
    }
    if (message) {
      params.message = message
    }
    await this.call('answer_permission', params)
  }

  async prompt(runtimeId: string, text: string): Promise<void> {
    await this.call('prompt', { runtime_id: runtimeId, text })
  }

  async interruptAndPrompt(runtimeId: string, text: string, keepQueue = false): Promise<void> {
    await this.call('interrupt_and_prompt', {
      runtime_id: runtimeId,
      text,
      keep_queue: keepQueue,
    })
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
