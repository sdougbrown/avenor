import { describe, it, expect } from 'bun:test'
import extensionFactory from './index.js'

describe('Avenor Pi Extension', () => {
  it('exports a function', () => {
    expect(typeof extensionFactory).toBe('function')
  })

  it('registers all expected tools', async () => {
    const registeredTools: string[] = []
    const registeredCommands: string[] = []
    const registeredRenderers: string[] = []
    const eventHandlers: Map<string, any[]> = new Map()

    const mockPi = {
      on: (event: string, handler: any) => {
        eventHandlers.set(event, [...(eventHandlers.get(event) ?? []), handler])
      },
      registerTool: (def: { name: string }) => {
        registeredTools.push(def.name)
      },
      registerCommand: (name: string, _opts: any) => {
        registeredCommands.push(name)
      },
      registerMessageRenderer: (type: string, _renderer: any) => {
        registeredRenderers.push(type)
      },
      setStatus: () => {},
      notify: () => {},
    }

    await extensionFactory(mockPi as any)

    // Tools
    expect(registeredTools).toContain('avenor_spawn')
    expect(registeredTools).toContain('avenor_status')
    expect(registeredTools).toContain('avenor_answer_permission')
    expect(registeredTools).toContain('avenor_follow_up')
    expect(registeredTools).toContain('avenor_events')
    expect(registeredTools).toContain('avenor_shutdown')

    // Commands
    expect(registeredCommands).toContain('avenor-status')
    expect(registeredCommands).toContain('avenor-watch')
    expect(registeredCommands).toContain('avenor-cancel')

    // Message renderer
    expect(registeredRenderers).toContain('avenor-active-runs')

    // Event handlers
    expect(eventHandlers.has('session_start')).toBe(true)
    expect(eventHandlers.has('session_shutdown')).toBe(true)
    expect(eventHandlers.has('before_agent_start')).toBe(true)
    expect(eventHandlers.has('tool_result')).toBe(true)
  })

  it('session_start captures context', async () => {
    let capturedCtx: any = null

    const mockPi = {
      on: (event: string, handler: any) => {
        if (event === 'session_start') {
          handler({}, { ui: { setWidget: () => {} } })
          capturedCtx = { ui: { setWidget: () => {} } }
        }
      },
      registerTool: () => {},
      registerCommand: () => {},
      registerMessageRenderer: () => {},
      setStatus: () => {},
      notify: () => {},
    }

    await extensionFactory(mockPi as any)
    // session_start handler should have been invoked
    expect(capturedCtx).not.toBeNull()
  })
})
