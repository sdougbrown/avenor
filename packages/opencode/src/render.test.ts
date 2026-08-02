import { describe, expect, it } from 'bun:test'
import {
  formatAnswerPermissionOutput,
  formatEventsOutput,
  formatFollowUpOutput,
  formatInspectOutput,
  formatResultOutput,
  formatShutdownOutput,
  formatStatusOutput,
} from './render.js'

describe('OpenCode Avenor result renderers', () => {
  it('formats status and result prose while retaining complete metadata', () => {
    const status = {
      run_id: 'run-1',
      label: 'worker',
      status: 'waiting',
      final_output: 'hello',
      pending_permission: {
        description: 'Read file',
        options: [{ option_id: 'allow_once', label: 'Allow once' }],
      },
      phase: 'tool',
    }
    const statusOutput = formatStatusOutput({ run_id: 'call-run' }, status)
    expect(statusOutput).toEqual({
      title: 'worker — waiting',
      output: [
        'Status: worker — waiting',
        'Run: run-1',
        'Permission: Read file',
        'Options: allow_once (Allow once)',
        'Final output:',
        'hello',
        'Guidance: Call avenor_answer_permission with run_id "run-1" and option_id "allow_once".',
        'Phase: tool',
      ].join('\n'),
      metadata: status,
    })

    const statuses = [status, { run_id: 'run-2', label: 'done worker', status: 'done' }]
    const allRuns = formatStatusOutput({}, statuses)
    expect(allRuns.title).toBe('Avenor status — 2 runs')
    expect(allRuns.output).toContain('Avenor status — 2 runs\nworker — waiting (run_id: run-1)\ndone worker — done (run_id: run-2)')
    expect(allRuns.metadata).toEqual({ results: statuses, count: 2 })

    const result = {
      run_id: 'run-1', label: 'worker', status: 'running', ready: false,
      timed_out: true, output_truncated: true, output_event_path: '/tmp/events', output: 'partial',
      session_id: 'session-1', stop_reason: 'budget',
    }
    const resultOutput = formatResultOutput({ run_id: 'call-run' }, result)
    expect(resultOutput.title).toBe('worker — running (wait timed out)')
    expect(resultOutput.output).toBe([
      'Result: worker — running',
      'Run: run-1',
      'Warning: wait timed out; the run is still active.',
      'Guidance: Call avenor_result with run_id "run-1".',
      'Warning: final output may be truncated; use avenor_events with run_id "run-1" and path "/tmp/events".',
      'Final output:',
      'partial',
      'Session: session-1',
      'Stop reason: budget',
    ].join('\n'))
    expect(resultOutput.metadata).toEqual(result)
  })

  it('formats acknowledgement tools with argument-derived identifiers only in metadata', () => {
    const permission = formatAnswerPermissionOutput(
      { run_id: 'run-1', option_id: 'allow', request_id: 'request-1', message: 'not retained' },
      { ok: true },
    )
    expect(permission.title).toBe('run-1 — permission answered')
    expect(permission.output).toContain('Permission answer: accepted.\nRun: run-1\nOption: allow\nRequest: request-1')
    expect(permission.metadata).toEqual({ ok: true, run_id: 'run-1', option_id: 'allow', request_id: 'request-1' })

    const followUp = formatFollowUpOutput({ run_id: 'run-1', message: 'not retained' }, { run_id: 'run-2', label: 'follow-up' })
    expect(followUp).toEqual(expect.objectContaining({
      title: 'follow-up — dispatched',
      output: expect.stringContaining('Source run: run-1\nRun: run-2'),
      metadata: { run_id: 'run-2', label: 'follow-up', source_run_id: 'run-1' },
    }))

    const shutdown = formatShutdownOutput({ supervisor_id: '/tmp/sock', force: true }, { ok: false, cleaned_up: ['/tmp/one'] })
    expect(shutdown).toEqual(expect.objectContaining({
      title: 'Avenor — shut down',
      output: [
        'Avenor shutdown: failed.',
        'Supervisor: /tmp/sock',
        'Mode: force',
        'Cleanup: 1 paths',
        'Warning: shutdown did not complete.',
        'Guidance: retry avenor_shutdown.',
        'Cleanup path: /tmp/one',
      ].join('\n'),
      metadata: { ok: false, cleaned_up: ['/tmp/one'], supervisor_id: '/tmp/sock', force: true },
    }))
  })

  it('uses bounded event and inspect templates without dumping structured data', () => {
    const events = {
      events: [
        { event: 'agent.message', seq: 2, delta: 'hello' },
        { type: 'session.end', seq: '3' },
      ],
    }
    const eventOutput = formatEventsOutput({ run_id: 'run-1', types: ['agent.message'] }, events)
    expect(eventOutput).toEqual({
      title: 'run-1 — 2 events',
      output: [
        'Events: run-1 — 2 events',
        'Filter: agent.message',
        'Preview: Event 2: agent.message — hello',
        '… 1 items omitted',
        'Event 2: agent.message — hello',
        'Event 3: session.end',
      ].join('\n'),
      metadata: { ...events, run_id: 'run-1', count: 2 },
    })

    const inspect = {
      run_id: 'run-1', label: 'worker', status: { status: 'done' }, final_output: 'answer',
      snapshot: {}, transcript: [{ kind: 'assistant', text: 'answer' }],
      tools: [{ title: 'read', status: 'done', preview: 'file' }],
      live_tools: [], permissions: [],
    }
    const inspectOutput = formatInspectOutput({ run_id: 'call-run' }, inspect as any)
    expect(inspectOutput.title).toBe('worker — done')
    expect(inspectOutput.output).toBe([
      'Inspect: worker — done',
      'Run: run-1',
      'Counts: transcript=1, tools=1, live_tools=0, permissions=0',
      'Final output:',
      'answer',
      'Guidance: Call avenor_events with run_id "run-1" for raw events.',
      'Transcript:',
      'assistant: answer',
      'Tools:',
      'Tool: read (done): file',
      'Live tools:',
      'Permissions:',
    ].join('\n'))
    expect(inspectOutput.metadata).toEqual(inspect)
  })

  it('sanitizes and bounds display text without changing metadata', () => {
    const rawOutput = `\u001b[31mHEAD-MARKER hello\tworld\u0000\n${Array.from({ length: 20 }, (_, index) => `line-${index}`).join('\n')}${'x'.repeat(1_600)}TAIL-MARKER`
    const value = { run_id: 'run-1', label: 'worker', status: 'done', ready: true, output: rawOutput }
    const formatted = formatResultOutput({}, value)
    expect(formatted.output).not.toContain('\u001b')
    expect(formatted.output).not.toContain('\u0000')
    expect(formatted.output).not.toContain('HEAD-MARKER')
    expect(formatted.output).toContain('TAIL-MARKER')
    expect(formatted.output).toContain('[preview clipped:')
    expect(formatted.output).toContain('lines omitted')
    expect(formatted.output.indexOf('[preview clipped:')).toBeLessThan(formatted.output.indexOf('lines omitted'))
    expect(formatted.metadata.output).toBe(rawOutput)
  })
})
