import * as path from 'node:path'
import { describe, expect, it } from 'bun:test'
import { countNestedRuns, type NestedSupervisorDial } from './nested-count.js'

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

  it('rejects invalid root PIDs without dialing', async () => {
    let dialCount = 0
    const dial: NestedSupervisorDial = async () => {
      dialCount++
      throw new Error('should not dial')
    }

    await expect(countNestedRuns(0, dial)).resolves.toBe(0)
    await expect(countNestedRuns(-1, dial)).resolves.toBe(0)
    await expect(countNestedRuns(1.5, dial)).resolves.toBe(0)
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
