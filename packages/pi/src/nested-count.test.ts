import * as path from 'node:path'
import { describe, expect, it, jest } from 'bun:test'
import {
  countNestedRuns,
  type NestedSupervisorDial,
  type NestedSupervisorClient,
  AsyncLimiter,
  dialWithTimeout,
  isActiveRun,
  childPiPid,
} from './nested-count.js'

type Run = Record<string, unknown>

function makeDial(supervisors: Record<number, Run[]>) {
  const calls: number[] = []
  let closeCount = 0
  const dial: NestedSupervisorDial = async socketPath => {
    const match = path.basename(socketPath).match(/^avenor-mcp-(\d+)\.sock$/)
    if (!match) throw new Error(`unexpected socket path: ${socketPath}`)

    const pid = Number(match[1])
    calls.push(pid)
    if (!(pid in supervisors)) throw new Error(`missing supervisor for ${pid}`)

    return {
      async list() {
        return [...supervisors[pid]!]
      },
      close() {
        closeCount++
      },
    }
  }

  return { calls, closeCount: () => closeCount, dial }
}

describe('countNestedRuns', () => {
  it('counts active descendants through multiple pi supervisor layers', async () => {
    const harness = makeDial({
      100: [
        { status: 'running', backend: 'pi', pid: 200 },
        { status: 'idle', backend: 'opencode-acp', pid: 999 },
        { status: 'done', backend: 'pi', pid: 202 },
      ],
      200: [
        { status: 'running', backend: 'pi', pid: 300 },
        { status: 'idle', backend: 'pi', pid: 301 },
      ],
      300: [{ status: 'running', backend: 'pi', pid: 400 }],
      301: [],
      400: [],
    })

    await expect(countNestedRuns(100, harness.dial)).resolves.toBe(5)
    expect([...new Set(harness.calls)].sort()).toEqual([100, 200, 300, 301, 400])
    expect(harness.closeCount()).toBe(5)
  })

  it('returns zero when a child supervisor cannot be listed', async () => {
    let closeCount = 0
    const dial: NestedSupervisorDial = async () => ({
      async list() {
        throw new Error('list failed')
      },
      close() {
        closeCount++
      },
    })

    await expect(countNestedRuns(700, dial)).resolves.toBe(0)
    expect(closeCount).toBe(1)
  })

  it.each([
    { pid: 0, label: 'zero' },
    { pid: -1, label: 'negative' },
    { pid: 1.5, label: 'float' },
  ])('rejects invalid root PID $label ($pid) without dialing', async ({ pid }) => {
    let dialCount = 0
    const dial: NestedSupervisorDial = async () => {
      dialCount++
      throw new Error('should not dial')
    }

    await expect(countNestedRuns(pid as number, dial)).resolves.toBe(0)
    expect(dialCount).toBe(0)
  })

  it('stops at the maximum nesting depth', async () => {
    const basePid = 1_000
    const supervisors: Record<number, Run[]> = {}
    for (let i = 0; i < 100; i++) {
      supervisors[basePid + i] = [{ status: 'running', backend: 'pi', pid: basePid + i + 1 }]
    }
    supervisors[basePid + 100] = []
    const harness = makeDial(supervisors)

    await expect(countNestedRuns(basePid, harness.dial)).resolves.toBe(99)
    expect(harness.calls).toHaveLength(99)
    // Every dialled supervisor's client is closed exactly once.
    expect(harness.closeCount()).toBe(99)
  })

  it('stops safely when supervisor data contains a pid cycle', async () => {
    const harness = makeDial({
      500: [{ status: 'running', backend: 'pi', pid: 600 }],
      600: [{ status: 'running', backend: 'pi', pid: 500 }],
    })

    await expect(countNestedRuns(500, harness.dial)).resolves.toBe(2)
    expect(harness.calls).toEqual([500, 600])
  })

})

describe('dialWithTimeout', () => {
  it('returns the connection when it wins the race', async () => {
    let resolved = false
    let closeCalled = false
    let callTimeoutMs: number | undefined
    const dial: NestedSupervisorDial = async (_socketPath, options) => {
      resolved = true
      callTimeoutMs = options?.callTimeoutMs
      return {
        async list() { return [] },
        close() { closeCalled = true },
      }
    }

    const client = await dialWithTimeout(dial, '/tmp/test.sock', 10)
    expect(resolved).toBe(true)
    expect(callTimeoutMs).toBe(10)
    expect(await client.list()).toEqual([])
    expect(closeCalled).toBe(false)
    client.close()
  })

  it('rejects when timeout wins', async () => {
    jest.useFakeTimers()
    try {
      const dial: NestedSupervisorDial = async () =>
        new Promise(() => {
          /* never resolve */
        })

      const pending = dialWithTimeout(dial, '/tmp/test.sock', 5)
      jest.advanceTimersByTime(5)
      await expect(pending).rejects.toThrow('dial timeout')
    } finally {
      jest.useRealTimers()
    }
  })

  it('closes a late-resolving client after timeout wins', async () => {
    jest.useFakeTimers()
    try {
      let closeCalled = false
      let resolveDial!: (client: NestedSupervisorClient) => void
      const dial: NestedSupervisorDial = async () => new Promise(resolve => { resolveDial = resolve })

      const pending = dialWithTimeout(dial, '/tmp/test.sock', 10)
      jest.advanceTimersByTime(10)
      await expect(pending).rejects.toThrow('dial timeout')

      const closed = new Promise<void>(resolve => {
        resolveDial({
          async list() { return [] },
          close() {
            closeCalled = true
            resolve()
          },
        })
      })
      await closed
      expect(closeCalled).toBe(true)
    } finally {
      jest.useRealTimers()
    }
  })

  it('propagates rejection when dial function rejects immediately', async () => {
    const dial: NestedSupervisorDial = async () => {
      throw new Error('connection refused')
    }

    await expect(dialWithTimeout(dial, '/tmp/test.sock', 10)).rejects.toThrow('connection refused')
  })
})

describe('AsyncLimiter', () => {
  it('enforces max concurrency', async () => {
    const limiter = new AsyncLimiter(2)
    let active = 0
    let peak = 0
    let startedCount = 0
    let markFirstWave!: () => void
    let release!: () => void
    const firstWaveStarted = new Promise<void>(resolve => { markFirstWave = resolve })
    const releaseFirstWave = new Promise<void>(resolve => { release = resolve })
    const tasks: Array<Promise<void>> = []

    for (let i = 0; i < 6; i++) {
      tasks.push(
        limiter.run(async () => {
          active++
          peak = Math.max(peak, active)
          if (++startedCount === 2) markFirstWave()
          await releaseFirstWave
          active--
        }),
      )
    }

    await firstWaveStarted
    expect(peak).toBe(2)
    release()
    await Promise.all(tasks)
    expect(active).toBe(0)
    expect(await limiter.run(async () => 42)).toBe(42)
  })

  it('starts queued tasks in FIFO order when concurrency slots free up', async () => {
    const limiter = new AsyncLimiter(1)
    const order: string[] = []
    let releaseFirst!: () => void
    let markFirstStarted!: () => void
    const firstStarted = new Promise<void>(resolve => { markFirstStarted = resolve })
    const firstReleased = new Promise<void>(resolve => { releaseFirst = resolve })

    const p1 = limiter.run(async () => {
      order.push('1:start')
      markFirstStarted()
      await firstReleased
      order.push('1:done')
    })
    await firstStarted

    const p2 = limiter.run(async () => {
      order.push('2:start')
      order.push('2:done')
    })
    const p3 = limiter.run(async () => {
      order.push('3:start')
      order.push('3:done')
    })

    releaseFirst()
    await Promise.all([p1, p2, p3])
    expect(order).toEqual(['1:start', '1:done', '2:start', '2:done', '3:start', '3:done'])
  })

  it('handles more than 16 queued tasks with limit of 16', async () => {
    const limiter = new AsyncLimiter(16)
    let active = 0
    let peak = 0
    let startedCount = 0
    let markFirstWave!: () => void
    let release!: () => void
    const firstWaveStarted = new Promise<void>(resolve => { markFirstWave = resolve })
    const releaseFirstWave = new Promise<void>(resolve => { release = resolve })
    const tasks: Array<Promise<void>> = []

    for (let i = 0; i < 32; i++) {
      tasks.push(
        limiter.run(async () => {
          active++
          peak = Math.max(peak, active)
          if (++startedCount === 16) markFirstWave()
          await releaseFirstWave
          active--
        }),
      )
    }

    await firstWaveStarted
    expect(peak).toBe(16)
    release()
    await Promise.all(tasks)
    expect(active).toBe(0)
  })

  it('propagates errors from queued tasks while later tasks still run', async () => {
    const limiter = new AsyncLimiter(1)
    const results: string[] = []
    let releaseTask1!: () => void
    const task1Released = new Promise<void>(resolve => { releaseTask1 = resolve })

    const p1 = limiter.run(async () => {
      await task1Released
      throw new Error('task 1 failed')
    })
    const p2 = limiter.run(async () => {
      results.push('task 2 succeeded')
    })
    const p3 = limiter.run(async () => {
      results.push('task 3 succeeded')
    })

    releaseTask1()
    await expect(p1).rejects.toThrow('task 1 failed')
    await Promise.all([p2, p3])
    expect(results).toEqual(['task 2 succeeded', 'task 3 succeeded'])
  })
})

describe('isActiveRun', () => {
  it('returns true for running', () => {
    expect(isActiveRun({ status: 'running' })).toBe(true)
    expect(isActiveRun({ status: 'RUNNING' })).toBe(true)
  })

  it('returns true for idle', () => {
    expect(isActiveRun({ status: 'idle' })).toBe(true)
    expect(isActiveRun({ status: 'Idle' })).toBe(true)
  })

  it('returns false for done', () => {
    expect(isActiveRun({ status: 'done' })).toBe(false)
  })

  it('defaults to running when status is null', () => {
    expect(isActiveRun({ status: null })).toBe(true)
  })

  it('defaults to running when status is absent', () => {
    expect(isActiveRun({})).toBe(true)
  })

  it('returns false for empty string status', () => {
    expect(isActiveRun({ status: '' })).toBe(false)
  })
})

describe('childPiPid', () => {
  it('returns valid Pi PID', () => {
    expect(childPiPid({ backend: 'pi', pid: 42 })).toBe(42)
    expect(childPiPid({ backend: 'PI', pid: 42 })).toBe(42)
  })

  it('returns undefined for non-Pi backend', () => {
    expect(childPiPid({ backend: 'opencode-acp', pid: 42 })).toBeUndefined()
  })

  it('returns undefined for missing backend', () => {
    expect(childPiPid({ pid: 42 })).toBeUndefined()
  })

  it('returns undefined for zero pid', () => {
    expect(childPiPid({ backend: 'pi', pid: 0 })).toBeUndefined()
  })

  it('returns undefined for negative pid', () => {
    expect(childPiPid({ backend: 'pi', pid: -1 })).toBeUndefined()
  })

  it('returns undefined for float pid', () => {
    expect(childPiPid({ backend: 'pi', pid: 1.5 })).toBeUndefined()
  })

  it('returns undefined for non-number pid', () => {
    expect(childPiPid({ backend: 'pi', pid: 'abc' as unknown as number })).toBeUndefined()
    expect(childPiPid({ backend: 'pi', pid: Number.NaN })).toBeUndefined()
    expect(childPiPid({ backend: 'pi', pid: Number.POSITIVE_INFINITY })).toBeUndefined()
  })
})
