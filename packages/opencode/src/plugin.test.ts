import { describe, it, expect, mock } from 'bun:test'

const spawnToolMock = mock(async () => ({
  run_id: 'run-1',
  runtime_id: 'rt-1',
  label: 'test run',
}))
const statusToolMock = mock(async () => ({
  run_id: 'run-1',
  runtime_id: 'rt-1',
  session_id: 'child-session',
  status: 'waiting',
  pending_permission: {
    request_id: 'perm-1',
    description: 'read /outside/project',
    options: [],
  },
}))

mock.module('@dougbots/avenor-core', () => ({
  spawnTool: spawnToolMock,
  statusTool: statusToolMock,
  eventsTool: mock(async () => ({ events: [] })),
  answerPermissionTool: mock(async () => ({})),
  followUpTool: mock(async () => ({})),
  shutdownTool: mock(async () => ({})),
}))

const { AvenorPlugin } = await import('./plugin.js')

function makeClient() {
  return {
    session: {
      promptAsync: mock(async () => {}),
    },
  }
}

function makeCtx(overrides: Record<string, unknown> = {}) {
  return {
    client: makeClient(),
    project: {},
    directory: '/tmp/test',
    worktree: '/tmp/test',
    serverUrl: new URL('http://localhost:4096'),
    $: {} as any,
    experimental_workspace: { register: () => {} },
    ...overrides,
  }
}

describe('AvenorPlugin', () => {
  it('exports a function', () => {
    expect(typeof AvenorPlugin).toBe('function')
  })

  it('registers an event hook, permission.ask hook, and all expected tools', async () => {
    const hooks = await AvenorPlugin(makeCtx() as any)
    expect(typeof hooks.event).toBe('function')
    expect(typeof hooks['permission.ask']).toBe('function')
    expect(hooks.tool).toBeDefined()
    const toolNames = Object.keys(hooks.tool ?? {})
    expect(toolNames).toContain('avenor_spawn')
    expect(toolNames).toContain('avenor_status')
    expect(toolNames).toContain('avenor_answer_permission')
    expect(toolNames).toContain('avenor_follow_up')
    expect(toolNames).toContain('avenor_events')
    expect(toolNames).toContain('avenor_shutdown')
  })

  it('event hook ignores non-idle events', async () => {
    const hooks = await AvenorPlugin(makeCtx() as any)
    await expect(
      hooks.event?.({ event: { type: 'session.updated', properties: { sessionID: 'abc' } } as any })
    ).resolves.toBeUndefined()
  })

  it('event hook on session.idle does nothing with no tracked runs', async () => {
    const ctx = makeCtx()
    const hooks = await AvenorPlugin(ctx as any)
    await hooks.event?.({
      event: { type: 'session.idle', properties: { sessionID: 'session-1' } } as any,
    })
    expect(ctx.client.session.promptAsync).not.toHaveBeenCalled()
  })

  it('permission.ask hook ignores unknown sessions', async () => {
    const ctx = makeCtx()
    const hooks = await AvenorPlugin(ctx as any)
    const output = { status: 'ask' as const }
    await hooks['permission.ask']?.(
      {
        id: 'perm-1',
        sessionID: 'unknown-session',
        type: 'bash',
        title: 'Run bash command',
        messageID: 'msg-1',
        metadata: {},
        time: { created: Date.now() },
      },
      output,
    )
    // Should not inject anything — unknown session
    expect(ctx.client.session.promptAsync).not.toHaveBeenCalled()
    // Should not change output status
    expect(output.status).toBe('ask')
  })

  it('monitors fire-and-forget runs immediately so permissions cannot time out before idle', async () => {
    spawnToolMock.mockClear()
    statusToolMock.mockClear()
    statusToolMock
      .mockImplementationOnce(async () => ({
        run_id: 'run-1',
        runtime_id: 'rt-1',
        session_id: 'child-session',
        status: 'waiting',
        pending_permission: {
          request_id: 'perm-1',
          description: 'read /outside/project',
          options: [],
        },
      }))
      .mockImplementationOnce(async () => ({
        run_id: 'run-1',
        runtime_id: 'rt-1',
        session_id: 'child-session',
        status: 'done',
      }))
    let hooks!: Awaited<ReturnType<typeof AvenorPlugin>>
    let completionPrompted!: () => void
    const completionPrompt = new Promise<void>((resolve) => {
      completionPrompted = resolve
    })
    let promptCount = 0
    const ctx = makeCtx({
      client: {
        session: {
          promptAsync: mock(async () => {
            promptCount++
            if (promptCount === 1) {
              // Fire idle on the next task, after the waiting monitor has
              // returned and released its monitoring flag.
              setTimeout(() => {
                void hooks.event?.({
                  event: {
                    type: 'session.idle',
                    properties: { sessionID: 'orchestrator-session' },
                  } as any,
                })
              }, 0)
            } else {
              completionPrompted()
            }
          }),
        },
      },
    })
    hooks = await AvenorPlugin(ctx as any)
    const spawn = hooks.tool?.avenor_spawn as any

    await spawn.execute(
      { agent: 'jockey', wait: false },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )

    await completionPrompt

    expect(statusToolMock).toHaveBeenCalledTimes(2)
    expect(ctx.client.session.promptAsync).toHaveBeenCalledTimes(2)
    expect(ctx.client.session.promptAsync.mock.calls[0]?.[0]).toEqual({
      path: { id: 'orchestrator-session' },
      body: {
        parts: [{
          type: 'text',
          text: expect.stringContaining('Permission request: read /outside/project'),
        }],
      },
    })
    expect(ctx.client.session.promptAsync.mock.calls[1]?.[0]).toEqual({
      path: { id: 'orchestrator-session' },
      body: {
        parts: [{
          type: 'text',
          text: expect.stringContaining('finished with status **done**'),
        }],
      },
    })
  })
})
