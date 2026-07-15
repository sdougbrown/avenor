import { type Event, type SubscribeOptions } from './client.js'
import {
  RunReducer,
  type RunReducerLimits,
  type RunSnapshot,
  type RunSnapshotSeed,
} from './run-reducer.js'

export interface RunObserverClient {
  events(options?: SubscribeOptions): AsyncIterable<Event>
  close?(): void
}

export interface ObserveRunOptions extends SubscribeOptions {
  reducer_limits?: Partial<RunReducerLimits>
  initial_snapshot?: RunSnapshotSeed
  close_client?: boolean
}

type SnapshotListener = (snapshot: RunSnapshot, event: Event) => void

type PendingNext = {
  resolve: (snapshot: RunSnapshot | null) => void
  reject: (error: Error) => void
  predicate?: (snapshot: RunSnapshot) => boolean
}

export interface RunObserver {
  snapshot(): RunSnapshot
  subscribe(listener: SnapshotListener): () => void
  next(predicate?: (snapshot: RunSnapshot) => boolean): Promise<RunSnapshot | null>
  close(): Promise<void>
  readonly closed: boolean
}

export function observeRun(
  client: RunObserverClient,
  options: ObserveRunOptions = {},
): RunObserver {
  const reducer = new RunReducer(options.reducer_limits, options.initial_snapshot)
  const listeners = new Set<SnapshotListener>()
  const pendingNext: PendingNext[] = []
  const streamOptions: SubscribeOptions = {}

  if (options.runtime_id) {
    streamOptions.runtime_id = options.runtime_id
    streamOptions.replay = options.replay ?? true
  } else if (options.replay !== undefined) {
    streamOptions.replay = options.replay
  }

  if (options.limit !== undefined) {
    streamOptions.limit = options.limit
  }
  if (options.after_seq !== undefined) {
    streamOptions.after_seq = options.after_seq
  }

  const iterator = client.events(streamOptions)[Symbol.asyncIterator]()
  let closed = false
  let clientClosed = false
  let failure: Error | null = null

  const closeClient = (): void => {
    if (!options.close_client || clientClosed) return
    clientClosed = true
    client.close?.()
  }

  const flushPending = (snapshot: RunSnapshot | null): void => {
    const unresolved = pendingNext.splice(0, pendingNext.length)
    for (const waiter of unresolved) {
      if (failure) {
        waiter.reject(failure)
        continue
      }
      if (snapshot && waiter.predicate && !waiter.predicate(snapshot)) {
        pendingNext.push(waiter)
        continue
      }
      waiter.resolve(snapshot)
    }
  }

  const loop = (async () => {
    try {
      while (!closed) {
        const result = await iterator.next()
        if (result.done || closed) {
          break
        }
        const snapshot = reducer.apply(result.value)
        for (const listener of listeners) {
          listener(snapshot, result.value)
        }
        flushPending(snapshot)
      }
    } catch (error) {
      failure = error instanceof Error ? error : new Error(String(error))
    } finally {
      closed = true
      flushPending(null)
    }
  })()

  return {
    get closed() {
      return closed
    },
    snapshot() {
      return reducer.snapshot()
    },
    subscribe(listener) {
      listeners.add(listener)
      return () => {
        listeners.delete(listener)
      }
    },
    next(predicate) {
      if (failure) {
        return Promise.reject(failure)
      }
      if (closed) {
        return Promise.resolve(null)
      }
      return new Promise<RunSnapshot | null>((resolve, reject) => {
        pendingNext.push({ resolve, reject, predicate })
      })
    },
    async close() {
      try {
        if (!closed) {
          closed = true
          if (typeof iterator.return === 'function') {
            await iterator.return()
          }
        }
        await loop.catch(() => {})
      } finally {
        closeClient()
      }
    },
  }
}
