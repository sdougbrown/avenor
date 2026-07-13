import { describe, it, expect } from 'bun:test'
import { Text } from '@earendil-works/pi-tui'
import extensionFactory, { statusSupervisorId } from './index.js'

describe('Avenor Pi Extension', () => {
  it('exports a function', () => {
    expect(typeof extensionFactory).toBe('function')
  })

  it('only reuses a supervisor socket when the caller supplied one', () => {
    expect(statusSupervisorId(undefined, '/tmp/spawned.sock')).toBeUndefined()
    expect(statusSupervisorId('/tmp/requested.sock', '/tmp/spawned.sock')).toBe('/tmp/spawned.sock')
  })

  describe('Registration', () => {
    it('registers all expected tools, commands, renderers, and event handlers', async () => {
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
    })
  })

  describe('session lifecycle', () => {
    it('session_start stores context for later use', async () => {
      const widgetCalls: any[] = []
      const statusCalls: any[] = []
      const sessionHandlers: Record<string, any> = {}
      const mockCtx = {
        ui: {
          setWidget: (...args: any[]) => {
            widgetCalls.push(args)
          },
          setStatus: (...args: any[]) => {
            statusCalls.push(args)
          },
          notify: () => {},
        },
      }

      const mockPi = {
        on: (event: string, handler: any) => {
          if (event === 'session_start' || event === 'session_shutdown') {
            sessionHandlers[event] = handler
          }
        },
        registerTool: () => {},
        registerCommand: () => {},
        registerMessageRenderer: () => {},
        setStatus: () => {},
        notify: () => {},
      }

      await extensionFactory(mockPi as any)

      await sessionHandlers.session_start({}, mockCtx)
      await sessionHandlers.session_shutdown({}, mockCtx)

      expect(widgetCalls).toEqual([[ 'avenor-status', undefined ]])
      expect(statusCalls).toEqual([[ 'avenor', '' ]])
    })

    it('session_shutdown clears state', async () => {
      let shutdownCalled = false

      const mockPi = {
        on: (event: string, handler: any) => {
          if (event === 'session_shutdown') {
            handler({}, {})
            shutdownCalled = true
          }
        },
        registerTool: () => {},
        registerCommand: () => {},
        registerMessageRenderer: () => {},
        setStatus: () => {},
        notify: () => {},
      }

      await extensionFactory(mockPi as any)
      expect(shutdownCalled).toBe(true)
    })
  })

  describe('before_agent_start', () => {
    it('returns empty (no message) when no tracked runs', async () => {
      let handlerResult: any = undefined

      const mockPi = {
        on: (event: string, handler: any) => {
          if (event === 'before_agent_start') {
            // Fire the handler — trackedRuns is empty, should return undefined
            handlerResult = handler({}, {})
          }
        },
        registerTool: () => {},
        registerCommand: () => {},
        registerMessageRenderer: () => {},
        setStatus: () => {},
        notify: () => {},
      }

      await extensionFactory(mockPi as any)

      expect(await handlerResult).toBeUndefined()
    })
  })

  describe('message renderer', () => {
    it('renders avenor-active-runs without throwing', async () => {
      let capturedRenderer: any = null

      const mockPi = {
        on: () => {},
        registerTool: () => {},
        registerCommand: () => {},
        registerMessageRenderer: (type: string, renderer: any) => {
          if (type === 'avenor-active-runs') {
            capturedRenderer = renderer
          }
        },
        setStatus: () => {},
        notify: () => {},
      }

      await extensionFactory(mockPi as any)

      expect(capturedRenderer).not.toBeNull()

      const mockTheme = {
        fg: (_color: string, text: string) => text,
        bold: (text: string) => text,
      }

      const result = capturedRenderer(
        { content: 'Active sub-agents:\n  • task1 (mule): running' },
        {},
        mockTheme,
      )

      expect(result).toBeInstanceOf(Text)
    })
  })
})
