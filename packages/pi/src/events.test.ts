import { describe, expect, it, mock } from 'bun:test'
import type { EventBus } from '@earendil-works/pi-coding-agent'
import {
  CHANNEL_LAST_TURN_ERROR,
  CHANNEL_POLL_COMPLETED,
  CHANNEL_POLL_ERROR,
  CHANNEL_RUN_TERMINAL,
  createDeliveryErrorPayload,
  createLastTurnErrorPayload,
  createPeerErrorPayload,
  createPollCompletedPayload,
  createPollErrorPayload,
  createRunTerminalPayload,
  emitAvenorEvent,
  onAvenorEvent,
} from './events.js'
import type { PollCompletedPayload, PollErrorPayload, RunTerminalPayload } from './events.js'

function createMockBus(): EventBus {
  const handlers = new Map<string, Set<(data: unknown) => void>>()
  return {
    emit(channel: string, data: unknown): void {
      for (const handler of Array.from(handlers.get(channel) ?? [])) {
        try {
          handler(data)
        } catch (error) {
          console.error(`mock event handler failed: ${String(error)}`)
        }
      }
    },
    on(channel: string, handler: (data: unknown) => void): () => void {
      let set = handlers.get(channel)
      if (!set) {
        set = new Set()
        handlers.set(channel, set)
      }
      set.add(handler)
      return () => {
        set?.delete(handler)
        if (set?.size === 0) handlers.delete(channel)
      }
    },
  }
}

describe('Avenor telemetry event bridge', () => {
  it('uses namespaced channels', () => {
    expect(CHANNEL_POLL_COMPLETED).toBe('avenor:poll:completed')
    expect(CHANNEL_POLL_ERROR).toBe('avenor:poll:error')
    expect(CHANNEL_RUN_TERMINAL).toBe('avenor:run:terminal')
  })

  it('delivers typed poll payloads through the injected Pi event bus', () => {
    const bus = createMockBus()
    const received: PollCompletedPayload[] = []
    const unsubscribe = onAvenorEvent(bus, CHANNEL_POLL_COMPLETED, payload => {
      received.push(payload)
    })

    const payload = createPollCompletedPayload([
      {
        runId: 'run-1',
        supervisorId: '/tmp/parent.sock',
        runtimeId: 'runtime-1',
        label: 'explore',
        status: 'running',
        agent: 'horse',
        phaseLabel: 'reading',
        backend: 'pi',
        nestedCount: 3,
      },
    ], 1_700_000_000_000, 7)
    emitAvenorEvent(bus, CHANNEL_POLL_COMPLETED, payload)

    expect(received).toEqual([payload])
    expect(received[0]?.entries[0]?.runId).toBe('run-1')
    expect(received[0]?.entries[0]?.phaseLabel).toBe('reading')
    expect(received[0]?.generation).toBe(7)
    unsubscribe()
  })

  it('delivers bounded polling errors through the injected Pi event bus', () => {
    const bus = createMockBus()
    const received: PollErrorPayload[] = []
    const unsubscribe = onAvenorEvent(bus, CHANNEL_POLL_ERROR, payload => {
      received.push(payload)
    })

    const payload = createPollErrorPayload({
      source: 'run-status',
      runId: 'run-1',
      message: 'statusTool failed',
      error: `\u001b[31m${'x'.repeat(700)}\u001b[0m`,
      count: 3,
      timestamp: 1_700_000_000_000,
    })
    emitAvenorEvent(bus, CHANNEL_POLL_ERROR, payload)

    expect(received).toEqual([payload])
    expect(received[0]).toMatchObject({
      source: 'run-status',
      runId: 'run-1',
      message: 'statusTool failed',
      count: 3,
      timestamp: 1_700_000_000_000,
    })
    expect(received[0]?.error).toHaveLength(600)
    expect(received[0]?.error).toEndWith('…')
    expect(Object.isFrozen(payload)).toBe(true)
    unsubscribe()
  })

  it('delivers terminal payloads and supports unsubscribe', () => {
    const bus = createMockBus()
    const received: RunTerminalPayload[] = []
    const handler = (payload: RunTerminalPayload) => received.push(payload)
    const unsubscribe = onAvenorEvent(bus, CHANNEL_RUN_TERMINAL, handler)

    emitAvenorEvent(bus, CHANNEL_RUN_TERMINAL, createRunTerminalPayload({
      runId: 'run-1',
      supervisorId: '/tmp/parent.sock',
      runtimeId: 'runtime-1',
      label: 'explore',
      agent: 'horse',
      status: 'done',
      nestedCount: 5,
      backend: 'pi',
    }))
    unsubscribe()
    emitAvenorEvent(bus, CHANNEL_RUN_TERMINAL, createRunTerminalPayload({
      runId: 'run-2',
      label: 'review',
      agent: 'mule',
      status: 'failed',
    }))

    expect(received).toHaveLength(1)
    expect(received[0]).toMatchObject({
      runId: 'run-1',
      runtimeId: 'runtime-1',
      label: 'explore',
      agent: 'horse',
      status: 'done',
      nestedCount: 5,
      backend: 'pi',
    })
    expect(received[0]?.supervisorKey).toMatch(/^supervisor:[0-9a-f]{16}$/)
    expect(received[0]).not.toHaveProperty('supervisorId')
  })

  it('copies and freezes the bounded poll payload', () => {
    const source = {
      runId: 'run-1',
      supervisorId: '/tmp/parent.sock',
      runtimeId: 'runtime-1',
      label: 'task-a',
      status: 'running',
      agent: 'horse',
      phase: 'tool',
      phaseLabel: 'reading',
      pendingPermission: true,
      permissionDescription: 'Allow bash',
      pid: 100,
      backend: 'pi',
      nestedCount: 2,
      // Include transcript/final_output at runtime to verify the explicit allowlist.
      transcript: 'not-public',
      final_output: 'not-public',
    } as any

    const payload = createPollCompletedPayload([source], 1, 1)
    expect(payload.entries).toHaveLength(1)
    expect(payload.entries[0]).toMatchObject({
      runId: 'run-1',
      runtimeId: 'runtime-1',
      label: 'task-a',
      status: 'running',
      agent: 'horse',
      phase: 'tool',
      phaseLabel: 'reading',
      pendingPermission: true,
      backend: 'pi',
      nestedCount: 2,
    })
    expect(payload.entries[0]?.supervisorKey).toMatch(/^supervisor:[0-9a-f]{16}$/)
    expect(payload.entries[0]).not.toHaveProperty('supervisorId')
    expect(payload.entries[0]).not.toHaveProperty('permissionDescription')
    expect(payload.entries[0]).not.toHaveProperty('pid')
    expect(payload.entries[0]).not.toHaveProperty('transcript')
    expect(payload.entries[0]).not.toHaveProperty('final_output')
    expect(Object.isFrozen(payload)).toBe(true)
    expect(Object.isFrozen(payload.entries)).toBe(true)
    expect(Object.isFrozen(payload.entries[0])).toBe(true)
  })

  it('handles empty poll payloads', () => {
    const payload = createPollCompletedPayload([], 1, 2)

    expect(payload.entries).toEqual([])
    expect(payload.timestamp).toBe(1)
    expect(payload.generation).toBe(2)
    expect(Object.isFrozen(payload)).toBe(true)
    expect(Object.isFrozen(payload.entries)).toBe(true)
  })

  it('copies and freezes terminal payloads', () => {
    const payload = createRunTerminalPayload({
      runId: 'run-1',
      supervisorId: '/tmp/parent.sock',
      runtimeId: 'runtime-1',
      label: 'task-a',
      agent: 'horse',
      status: 'done',
      nestedCount: 5,
      backend: 'pi',
    })

    expect(payload).toMatchObject({
      runId: 'run-1',
      runtimeId: 'runtime-1',
      label: 'task-a',
      agent: 'horse',
      status: 'done',
      nestedCount: 5,
      backend: 'pi',
    })
    expect(payload.supervisorKey).toMatch(/^supervisor:[0-9a-f]{16}$/)
    expect(payload).not.toHaveProperty('supervisorId')
    expect(Object.isFrozen(payload)).toBe(true)
  })

  it('omits optional terminal fields when they are unavailable', () => {
    const payload = createRunTerminalPayload({
      runId: 'run-2',
      label: 'task-b',
      agent: 'mule',
      status: 'failed',
    })

    expect(payload).toEqual({
      runId: 'run-2',
      supervisorKey: 'singleton',
      label: 'task-b',
      agent: 'mule',
      status: 'failed',
    })
    expect(payload.supervisorKey).toBe('singleton')
    expect(Object.isFrozen(payload)).toBe(true)
  })

  it('uses unknown error for empty text after sanitizing', () => {
    const payload = createPollErrorPayload({
      source: 'run-status',
      message: '\u001b[31m\n\t  \n',
      error: '\u001b[31m\u001b[0m',
      count: 1,
      timestamp: 1_700_000_000_000,
    })

    expect(payload.message).toBe('unknown error')
    expect(payload.error).toBe('unknown error')
    expect(Object.isFrozen(payload)).toBe(true)
  })

  it('sanitizes message while bounding error independently', () => {
    const longMessage = 'a'.repeat(700)
    const longError = 'b'.repeat(800)
    const payload = createPollErrorPayload({
      source: 'singleton-list',
      message: longMessage,
      error: longError,
      count: 2,
      timestamp: 1_700_000_000_000,
    })

    expect(payload.message).toHaveLength(600)
    expect(payload.message).toEndWith('…')
    expect(payload.error).toHaveLength(600)
    expect(payload.error).toEndWith('…')
    expect(payload.source).toBe('singleton-list')
    expect(payload.count).toBe(2)
  })

  it('preserves clean text under the bound limit', () => {
    const payload = createPollErrorPayload({
      source: 'spawn-status',
      message: 'short msg',
      error: 'short err',
      count: 1,
      timestamp: 1_700_000_000_000,
    })

    expect(payload.message).toBe('short msg')
    expect(payload.error).toBe('short err')
  })

  it('copies and freezes a delivery error with its retry count', () => {
    const payload = createDeliveryErrorPayload({
      source: 'idle-send',
      runId: 'run-1',
      label: 'worker',
      message: 'idle completion send was never confirmed',
      error: new Error('no message_end after 3 attempts'),
      attempts: 3,
      timestamp: 1_700_000_000_000,
    })

    expect(payload).toEqual({
      source: 'idle-send',
      runId: 'run-1',
      label: 'worker',
      message: 'idle completion send was never confirmed',
      error: 'Error: no message_end after 3 attempts',
      attempts: 3,
      timestamp: 1_700_000_000_000,
    })
    expect(Object.isFrozen(payload)).toBe(true)
  })

  it('copies and freezes a peer error, carrying the broker code when it sent one', () => {
    const payload = createPeerErrorPayload({
      operation: 'poll',
      code: 'peer-retired',
      message: 'the central peer identity is no longer accepted',
      error: 'broker /poll-control: 410 peer-retired',
      terminal: true,
      timestamp: 1_700_000_000_000,
    })

    expect(payload).toEqual({
      operation: 'poll',
      code: 'peer-retired',
      message: 'the central peer identity is no longer accepted',
      error: 'broker /poll-control: 410 peer-retired',
      terminal: true,
      timestamp: 1_700_000_000_000,
    })
    expect(Object.isFrozen(payload)).toBe(true)
  })

  it('omits the code for a refusal that named none', () => {
    const payload = createPeerErrorPayload({
      operation: 'publish-turn',
      message: 'central last-turn publication failed',
      error: 'transport failure',
      terminal: false,
      timestamp: 1_700_000_000_000,
    })

    // Absent, not empty: a consumer reading `code` must not see a code the broker
    // never sent.
    expect('code' in payload).toBe(false)
  })

  it('sanitizes a peer error and keeps the operation and terminal flag', () => {
    const payload = createPeerErrorPayload({
      operation: 'register',
      code: 'peer-retired',
      message: '\u001b[31m\n',
      error: '\u001b[0m',
      terminal: true,
      timestamp: 1_700_000_000_000,
    })

    expect(payload).toMatchObject({ operation: 'register', message: 'unknown error', error: 'unknown error', terminal: true })
  })

  it('copies and freezes a last-turn error with sanitized text', () => {
    const payload = createLastTurnErrorPayload({
      message: 'session_last_set failed',
      error: 'control socket is closed',
      timestamp: 1_700_000_000_000,
    })

    expect(payload).toEqual({
      message: 'session_last_set failed',
      error: 'control socket is closed',
      timestamp: 1_700_000_000_000,
    })
    expect(Object.isFrozen(payload)).toBe(true)
  })

  it('uses unknown error for empty last-turn text after sanitizing', () => {
    const payload = createLastTurnErrorPayload({
      message: '\u001b[31m\n\t  \n',
      error: '\u001b[31m\u001b[0m',
      timestamp: 1_700_000_000_000,
    })

    expect(payload.message).toBe('unknown error')
    expect(payload.error).toBe('unknown error')
    expect(Object.isFrozen(payload)).toBe(true)
  })

  it('sanitizes last-turn message while bounding error independently', () => {
    const longMessage = 'a'.repeat(700)
    const longError = 'b'.repeat(800)
    const payload = createLastTurnErrorPayload({
      message: longMessage,
      error: longError,
      timestamp: 1_700_000_000_000,
    })

    expect(payload.message).toHaveLength(600)
    expect(payload.message).toEndWith('…')
    expect(payload.error).toHaveLength(600)
    expect(payload.error).toEndWith('…')
  })

  it('uses unknown error for an empty delivery failure and bounds both texts', () => {
    const payload = createDeliveryErrorPayload({
      source: 'idle-send',
      runId: 'run-1',
      label: 'worker',
      message: '\u001b[31m\n\t  \n',
      error: 'b'.repeat(800),
      attempts: 1,
      timestamp: 1_700_000_000_000,
    })

    expect(payload.message).toBe('unknown error')
    expect(payload.error).toHaveLength(600)
    expect(payload.error).toEndWith('…')
    expect(Object.isFrozen(payload)).toBe(true)
  })

  it('forwards handlers to the shared bus without owning global state', () => {
    const emit = mock<(channel: string, data: unknown) => void>()
    const unsubscribe = mock(() => {})
    const on = mock<(channel: string, handler: (data: unknown) => void) => () => void>(() => unsubscribe)
    const bus: EventBus = { emit, on }
    const handler = () => {}

    const returned = onAvenorEvent(bus, CHANNEL_POLL_COMPLETED, handler)
    emitAvenorEvent(bus, CHANNEL_POLL_COMPLETED, {
      entries: [],
      timestamp: 1,
      generation: 1,
    })
    returned()

    expect(on).toHaveBeenCalledWith(CHANNEL_POLL_COMPLETED, handler)
    expect(emit).toHaveBeenCalledWith(CHANNEL_POLL_COMPLETED, {
      entries: [],
      timestamp: 1,
      generation: 1,
    })
    expect(unsubscribe).toHaveBeenCalled()
  })

  it('keeps separate subscriptions isolated by channel', () => {
    const bus = createMockBus()
    const calls: string[] = []
    onAvenorEvent(bus, CHANNEL_POLL_COMPLETED, () => calls.push('poll'))
    onAvenorEvent(bus, CHANNEL_RUN_TERMINAL, () => calls.push('terminal'))

    emitAvenorEvent(bus, CHANNEL_POLL_COMPLETED, { entries: [], timestamp: 1, generation: 1 })
    expect(calls).toEqual(['poll'])
  })
})
