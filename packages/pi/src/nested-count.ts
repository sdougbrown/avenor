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

class AsyncLimiter {
  private active = 0
  private readonly queue: Array<() => void> = []

  constructor(private readonly limit: number) {}

  async run<T>(task: () => Promise<T>): Promise<T> {
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

function isActiveRun(run: Record<string, unknown>): boolean {
  const status = String(run.status ?? 'running').toLowerCase()
  return status === 'running' || status === 'idle'
}

function childPiPid(run: Record<string, unknown>): number | undefined {
  if (String(run.backend ?? '').toLowerCase() !== 'pi') return undefined
  if (typeof run.pid !== 'number' || !Number.isInteger(run.pid) || run.pid <= 0) return undefined
  return run.pid
}

async function dialWithTimeout(
  dial: NestedSupervisorDial,
  socketPath: string,
): Promise<NestedSupervisorClient> {
  let timedOut = false
  let timeout: ReturnType<typeof setTimeout> | undefined
  const connection = dial(socketPath, { callTimeoutMs: NESTED_DIAL_TIMEOUT_MS }).then(client => {
    // A connection can resolve after the outer timeout wins the race. Close it
    // instead of leaking a socket that the caller can no longer observe.
    if (timedOut) client.close()
    return client
  })

  try {
    return await Promise.race([
      connection,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error('dial timeout')), NESTED_DIAL_TIMEOUT_MS)
      }),
    ])
  } catch (error) {
    timedOut = true
    throw error
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

  const nextSeen = new Set(seen)
  nextSeen.add(pid)
  const socketPath = path.join(socketsRoot(), `avenor-mcp-${pid}.sock`)

  try {
    const activeRuns = await nestedDialLimiter.run(async () => {
      const client = await dialWithTimeout(dial, socketPath)
      try {
        return (await client.list()).filter(isActiveRun)
      } finally {
        client.close()
      }
    })

    const counts = await Promise.all(activeRuns.map(async run => {
      const nestedPid = childPiPid(run)
      // Count the active run at the depth boundary, but do not walk beyond it.
      if (!nestedPid || depth + 1 >= MAX_NESTED_DEPTH) return 1
      return 1 + await countNestedRunsAtDepth(nestedPid, dial, nextSeen, depth + 1)
    }))
    return counts.reduce((total, count) => total + count, 0)
  } catch {
    return 0
  }
}

/**
 * Count all active descendants of a spawned pi process. Each pi process owns
 * its own supervisor, so walking the tree requires dialing the supervisor for
 * every active pi run that exposes a child process PID.
 *
 * The depth guard prevents malformed acyclic data from recursing forever, and
 * the dial limiter keeps a large fan-out from opening unbounded sockets at
 * once. A path-local PID set separately prevents cycles.
 */
export async function countNestedRuns(
  pid: number,
  dial: NestedSupervisorDial,
): Promise<number> {
  return countNestedRunsAtDepth(pid, dial, new Set(), 0)
}
