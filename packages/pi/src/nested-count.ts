import * as fs from 'node:fs'
import * as path from 'node:path'
import { socketsRoot } from '@dougbots/avenor-core'

const NESTED_DIAL_TIMEOUT_MS = 2_000
const MAX_NESTED_DEPTH = 99
const MAX_CONCURRENT_NESTED_DIALS = 16

export interface NestedSupervisorClient {
  list(): Promise<Array<Record<string, unknown>>>
  close(): void
}

export type NestedSupervisorDial = (
  socketPath: string,
  options?: { callTimeoutMs?: number },
) => Promise<NestedSupervisorClient>

export class AsyncLimiter {
  private active = 0
  private readonly queue: Array<() => void> = []

  constructor(private readonly limit: number) {}

  async run<T>(task: () => Promise<T>): Promise<T> {
    // The capacity check and increment contain no await, so they cannot be
    // interleaved by another task on the single-threaded event loop.
    if (this.active >= this.limit) {
      await new Promise<void>(resolve => this.queue.push(resolve))
    }

    this.active++
    try {
      return await task()
    } finally {
      this.active--
      this.queue.shift()?.()
    }
  }
}

const nestedDialLimiter = new AsyncLimiter(MAX_CONCURRENT_NESTED_DIALS)

export function isActiveRun(run: Record<string, unknown>): boolean {
  const status = String(run.status ?? 'running').toLowerCase()
  return status === 'running' || status === 'idle'
}

export function childPiPid(run: Record<string, unknown>): number | undefined {
  if (String(run.backend ?? '').toLowerCase() !== 'pi') return undefined
  if (typeof run.pid !== 'number' || !Number.isInteger(run.pid) || run.pid <= 0) return undefined
  return run.pid
}

function nestedSocketPath(pid: number): string {
  const requestedRoot = socketsRoot()
  const requestedPath = path.join(requestedRoot, `avenor-mcp-${pid}.sock`)
  // A missing sockets root already means the child supervisor is unavailable;
  // preserve the normal dial failure path instead of making polling depend on
  // the directory existing before the first supervisor starts.
  if (!fs.existsSync(requestedRoot)) return requestedPath

  const root = fs.realpathSync(requestedRoot)
  const parent = fs.realpathSync(path.dirname(requestedPath))
  if (parent !== root) throw new Error('nested supervisor socket escaped sockets root')
  return path.join(parent, path.basename(requestedPath))
}

/**
 * Dial a nested supervisor with a bounded connection/RPC timeout.
 * A client that resolves after the timeout loses the race and is closed.
 */
export async function dialWithTimeout(
  dial: NestedSupervisorDial,
  socketPath: string,
  timeoutOverride?: number,
): Promise<NestedSupervisorClient> {
  const effectiveTimeout = timeoutOverride ?? NESTED_DIAL_TIMEOUT_MS
  let timedOut = false
  let timeout: ReturnType<typeof setTimeout> | undefined
  const timeoutMarker = Symbol('dial timeout')
  const connection = dial(socketPath, { callTimeoutMs: effectiveTimeout }).then(client => {
    // A connection can resolve after the outer timeout wins the race. Close it
    // instead of leaking a socket that the caller can no longer observe.
    if (timedOut) client.close()
    return client
  })

  try {
    const result = await Promise.race([
      connection,
      new Promise<typeof timeoutMarker>(resolve => {
        timeout = setTimeout(() => resolve(timeoutMarker), effectiveTimeout)
      }),
    ])
    if (result === timeoutMarker) {
      timedOut = true
      throw new Error('dial timeout')
    }
    return result
  } finally {
    if (timeout) clearTimeout(timeout)
  }
}

async function countNestedRunsAtDepth(
  pid: number,
  dial: NestedSupervisorDial,
  seen: Set<number>,
  depth: number,
): Promise<number> {
  if (!Number.isInteger(pid) || pid <= 0 || seen.has(pid) || depth >= MAX_NESTED_DEPTH) return 0

  seen.add(pid)

  let socketPath: string
  try {
    socketPath = nestedSocketPath(pid)
  } catch (error) {
    console.warn('avenor nested count: invalid supervisor socket path', error)
    return 0
  }

  let activeRuns: Array<Record<string, unknown>>
  try {
    activeRuns = await nestedDialLimiter.run(async () => {
      const client = await dialWithTimeout(dial, socketPath)
      try {
        return (await client.list()).filter(isActiveRun)
      } finally {
        client.close()
      }
    })
  } catch {
    // A child supervisor can disappear between PID discovery and this dial.
    return 0
  }

  let total = 0
  for (const run of activeRuns) {
    const nestedPid = childPiPid(run)
    // Count the active run at the depth boundary. The strict `<` below keeps
    // the boundary at 99 levels instead of allowing a 100th supervisor walk.
    total += 1
    if (nestedPid && depth + 1 < MAX_NESTED_DEPTH) {
      total += await countNestedRunsAtDepth(nestedPid, dial, seen, depth + 1)
    }
  }
  return total
}

/**
 * Count all active descendants of a spawned pi process. Each pi process owns
 * its own supervisor, so walking the tree requires dialing the supervisor for
 * every active pi run that exposes a child process PID.
 *
 * The depth guard prevents malformed acyclic data from recursing forever, and
 * the dial limiter keeps a large fan-out from opening unbounded sockets at
 * once. A traversal-wide PID set separately prevents cycles and duplicate
 * subtree walks from stale cross-branch references.
 */
export async function countNestedRuns(
  pid: number,
  dial: NestedSupervisorDial,
): Promise<number> {
  return countNestedRunsAtDepth(pid, dial, new Set(), 0)
}
