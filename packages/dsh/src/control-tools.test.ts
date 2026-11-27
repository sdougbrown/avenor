import { describe, it, expect } from 'bun:test'
import {
  KILL_TOOL_NAME,
  RESULT_TOOL_NAME,
  STATUS_TOOL_NAME,
  controlToolDefinitions,
  type ControlTools,
} from './control-tools.js'

function fakeControl() {
  const calls: Array<{ tool: string; args: unknown }> = []
  const control: ControlTools = {
    async status(request) {
      calls.push({ tool: 'status', args: request })
      return { runs: [], count: 0 }
    },
    async result(request) {
      calls.push({ tool: 'result', args: request })
      return { run_id: request.runId, status: 'done', ready: true, output: 'ok' }
    },
    async kill(request) {
      calls.push({ tool: 'kill', args: request })
      return { killed: true, run_id: request.runId }
    },
  }
  return { control, calls }
}

const byName = (name: string) => {
  const tool = controlToolDefinitions(fakeControl().control).find((entry) => entry.name === name)
  if (!tool) throw new Error(`missing tool ${name}`)
  return tool
}

describe('control tools', () => {
  it('publishes status, result, and kill with object output schemas', () => {
    const tools = controlToolDefinitions(fakeControl().control)
    expect(tools.map((tool) => tool.name)).toEqual([STATUS_TOOL_NAME, RESULT_TOOL_NAME, KILL_TOOL_NAME])
    for (const tool of tools) {
      expect((tool.output.schema as { type?: string }).type).toBe('object')
    }
  })

  it('lists every run when status has no run id', async () => {
    const { control, calls } = fakeControl()
    const tool = controlToolDefinitions(control).find((entry) => entry.name === STATUS_TOOL_NAME)!
    const result = await tool.execute({})
    expect(calls).toEqual([{ tool: 'status', args: {} }])
    expect(tool.output.render({}, result)[0]).toMatchObject({ type: 'text' })
  })

  it('rejects an unknown status view', async () => {
    const tool = byName(STATUS_TOOL_NAME)
    await expect(tool.execute({ view: 'verbose' })).rejects.toThrow('view must be lifecycle or full')
  })

  it('reads a finished run without waiting by default', async () => {
    const { control, calls } = fakeControl()
    const tool = controlToolDefinitions(control).find((entry) => entry.name === RESULT_TOOL_NAME)!
    await tool.execute({ runId: 'run-1' })
    expect(calls).toEqual([{ tool: 'result', args: { runId: 'run-1', wait: false } }])
  })

  it('honours an explicit wait and timeout', async () => {
    const { control, calls } = fakeControl()
    const tool = controlToolDefinitions(control).find((entry) => entry.name === RESULT_TOOL_NAME)!
    await tool.execute({ runId: 'run-1', wait: true, timeout: '90s' })
    expect(calls).toEqual([{ tool: 'result', args: { runId: 'run-1', wait: true, timeout: '90s' } }])
  })

  it('requires a run id to kill, and forwards it', async () => {
    const { control, calls } = fakeControl()
    const tool = controlToolDefinitions(control).find((entry) => entry.name === KILL_TOOL_NAME)!
    await expect(tool.execute({})).rejects.toThrow('runId is required')
    await tool.execute({ runId: 'run-1' })
    expect(calls).toEqual([{ tool: 'kill', args: { runId: 'run-1' } }])
  })
})
