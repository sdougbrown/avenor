import { describe, it, expect } from 'bun:test'
import { answerToolDefinition, ANSWER_TOOL_NAME } from './answer-tool.js'

describe('avenor_answer_permission', () => {
  it('declares the run and option ids as required', () => {
    const tool = answerToolDefinition({ answer: async () => ({ ok: true }) })
    expect(tool.name).toBe(ANSWER_TOOL_NAME)
    expect(tool.parameters.type).toBe('object')
    expect(tool.parameters.required).toEqual(['runId', 'optionId'])
    expect(tool.description).toContain('permission')
  })

  it('forwards the answer to Avenor and reports the outcome', async () => {
    const seen: Array<{ runId: string; optionId: string; requestId?: string }> = []
    const tool = answerToolDefinition({
      answer: async (request) => {
        seen.push(request)
        return { ok: true }
      },
    })

    const result = await tool.execute({ runId: 'run-1', optionId: 'allow_once' })
    expect(seen).toEqual([{ runId: 'run-1', optionId: 'allow_once' }])
    expect(result).toEqual({ ok: true })
    expect(tool.output.render({}, { ok: true })[0]).toMatchObject({ type: 'text' })
  })

  it('rejects a call without the ids it needs', async () => {
    const tool = answerToolDefinition({ answer: async () => ({ ok: true }) })
    await expect(tool.execute({ runId: 'run-1' })).rejects.toThrow('optionId')
    await expect(tool.execute({ optionId: 'deny' })).rejects.toThrow('runId')
    await expect(tool.execute(null)).rejects.toThrow('object')
  })
})
