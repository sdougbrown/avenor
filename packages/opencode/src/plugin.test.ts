import { describe, it, expect, mock } from 'bun:test'
import { AvenorPlugin } from './plugin.js'

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
})
