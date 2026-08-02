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
      'Warning: final output may be truncated; retry avenor_result or read the durable event path "/tmp/events".',
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

  it('uses fallback prose for malformed acknowledgement, follow-up, events, inspect, and shutdown results', () => {
    const retry = 'Retry the tool or use avenor_status/avenor_inspect.'
    const malformedEvents = { events: [null, { event: 'agent.message', seq: 1, delta: 'hello' }] }

    expect(formatAnswerPermissionOutput({ run_id: 'run-1', option_id: 'allow' }, { ok: 'yes' } as any)).toEqual({
      title: 'run-1 — permission answered',
      output: ['Avenor answer_permission result unavailable.', retry].join('\n'),
      metadata: { ok: 'yes', run_id: 'run-1', option_id: 'allow', request_id: undefined },
    })
    expect(formatFollowUpOutput({ run_id: 'run-1' }, { run_id: null, label: 'follow-up' } as any)).toEqual({
      title: 'follow-up — dispatched',
      output: ['Avenor follow_up result unavailable.', retry].join('\n'),
      metadata: { run_id: null, label: 'follow-up', source_run_id: 'run-1' },
    })
    expect(formatEventsOutput({ run_id: 'run-1' }, malformedEvents as any)).toEqual({
      title: 'run-1 — 2 events',
      output: ['Avenor events result unavailable.', retry].join('\n'),
      metadata: { ...malformedEvents, run_id: 'run-1', count: 2 },
    })
    expect(formatInspectOutput({ run_id: 'run-1' }, {} as any)).toEqual({
      title: 'run-1 — unknown',
      output: ['Avenor inspect result unavailable.', retry].join('\n'),
      metadata: {},
    })
    expect(formatShutdownOutput({}, { ok: 'yes', cleaned_up: 'not-an-array' } as any)).toEqual({
      title: 'Avenor — shut down',
      output: ['Avenor shutdown result unavailable.', retry].join('\n'),
      metadata: { ok: 'yes', cleaned_up: 'not-an-array', supervisor_id: undefined, force: false },
    })
  })

  it('formats empty OpenCode event lists', () => {
    expect(formatEventsOutput({ run_id: 'run-1' }, { events: [] })).toEqual({
      title: 'run-1 — 0 events',
      output: ['Events: run-1 — 0 events', 'No events.'].join('\n'),
      metadata: { events: [], run_id: 'run-1', count: 0 },
    })
  })

  it('caps hostile collection displays without changing metadata', () => {
    const hostile = (value: string) => `\u001b[31m${value}\u001b[0m\t\u0000`
    const marker = '… 2 items omitted'

    const statuses = Array.from({ length: 14 }, (_, index) => ({
      run_id: hostile(`run-${index}`),
      label: hostile(`worker-${index}`),
      status: 'running',
    }))
    const expectedStatuses = structuredClone(statuses)
    const statusOutput = formatStatusOutput({}, statuses as any)
    const statusLines = statusOutput.output.split('\n')
    expect(statusLines.filter(line => line.includes('(run_id:'))).toHaveLength(12)
    expect(statusLines.filter(line => line === marker)).toEqual([marker])
    expect(statusOutput.output).toContain('worker-0 — running (run_id: run-0)')
    expect(statusOutput.output).not.toContain('worker-12')
    expect(statusOutput.metadata).toEqual({ results: expectedStatuses, count: 14 })

    const options = Array.from({ length: 10 }, (_, index) => ({
      option_id: hostile(`option-${index}`),
      label: hostile(`label-${index}`),
    }))
    const permissionStatus = {
      run_id: hostile('permission-run'),
      label: hostile('permission-worker'),
      status: 'waiting',
      pending_permission: { description: hostile('permission request'), options },
    }
    const expectedPermissionStatus = structuredClone(permissionStatus)
    const permissionOutput = formatStatusOutput({}, permissionStatus as any)
    expect(permissionOutput.output.split('\n').find(line => line.startsWith('Options:'))).toBe(
      `Options: ${Array.from({ length: 8 }, (_, index) => `option-${index} (label-${index})`).join(', ')} ${marker}`,
    )
    expect(permissionOutput.output).not.toContain('option-8')
    expect(permissionOutput.metadata).toEqual(expectedPermissionStatus)

    const types = Array.from({ length: 10 }, (_, index) => hostile(`type-${index}`))
    const events = Array.from({ length: 14 }, (_, index) => ({
      event: hostile(`event-${index}`),
      seq: index,
      delta: hostile(`delta-${index}`),
    }))
    const eventsPayload = { events }
    const expectedEvents = structuredClone(eventsPayload)
    const eventsRun = hostile('events-run')
    const eventsOutput = formatEventsOutput({ run_id: eventsRun, types }, eventsPayload)
    const eventLines = eventsOutput.output.split('\n')
    expect(eventLines.find(line => line.startsWith('Filter:'))).toBe(
      `Filter: ${Array.from({ length: 8 }, (_, index) => `type-${index}`).join(', ')} ${marker}`,
    )
    expect(eventLines.filter(line => line.startsWith('Event '))).toHaveLength(12)
    expect(eventLines.filter(line => line.startsWith('…'))).toEqual([marker])
    expect(eventsOutput.output).toContain('Event 0: event-0 — delta-0')
    expect(eventsOutput.output).not.toContain('event-12')
    expect(eventsOutput.metadata).toEqual({ ...expectedEvents, run_id: eventsRun, count: 14 })

    const transcript = Array.from({ length: 7 }, (_, index) => ({ kind: 'assistant', text: hostile(`transcript-${index}`) }))
    const tools = Array.from({ length: 7 }, (_, index) => ({
      title: hostile(`tool-${index}`), status: hostile('done'), preview: hostile(`preview-${index}`),
    }))
    const liveTools = Array.from({ length: 7 }, (_, index) => ({
      kind: hostile(`live-${index}`), preview: hostile(`live-preview-${index}`),
    }))
    const permissions = Array.from({ length: 7 }, (_, index) => ({
      description: hostile(`permission-${index}`),
      request_id: hostile(`request-${index}`),
      options: [{ option_id: hostile(`allow-${index}`), label: hostile(`Allow ${index}`) }],
    }))
    const inspect = {
      run_id: hostile('inspect-run'),
      label: hostile('inspect-worker'),
      status: { status: 'done' },
      final_output: hostile('inspect-output'),
      snapshot: {},
      transcript,
      tools,
      live_tools: liveTools,
      permissions,
    }
    const expectedInspect = structuredClone(inspect)
    const inspectOutput = formatInspectOutput({}, inspect as any)
    const inspectLines = inspectOutput.output.split('\n')
    expect(inspectLines.filter(line => line.startsWith('assistant: transcript-'))).toHaveLength(5)
    expect(inspectLines.filter(line => line.startsWith('Tool: tool-'))).toHaveLength(5)
    expect(inspectLines.filter(line => line.startsWith('Live tool: live-'))).toHaveLength(5)
    expect(inspectLines.filter(line => line.startsWith('Permission: permission-'))).toHaveLength(5)
    expect(inspectLines.filter(line => line === marker)).toEqual([marker, marker, marker, marker])
    expect(inspectOutput.output).not.toContain('transcript-5')
    expect(inspectOutput.output).not.toContain('tool-5')
    expect(inspectOutput.output).not.toContain('live-5')
    expect(inspectOutput.output).not.toContain('permission-5')
    expect(inspectOutput.metadata).toEqual(expectedInspect)

    const cleanup = { ok: true, cleaned_up: Array.from({ length: 10 }, (_, index) => hostile(`/tmp/cleanup-${index}`)) }
    const expectedCleanup = structuredClone(cleanup)
    const supervisor = hostile('/tmp/sock')
    const cleanupOutput = formatShutdownOutput({ supervisor_id: supervisor, force: true }, cleanup)
    const cleanupLines = cleanupOutput.output.split('\n')
    expect(cleanupLines.filter(line => line.startsWith('Cleanup path:'))).toHaveLength(8)
    expect(cleanupLines.filter(line => line === marker)).toEqual([marker])
    expect(cleanupOutput.output).toContain('Cleanup path: /tmp/cleanup-0')
    expect(cleanupOutput.output).not.toContain('cleanup-8')
    expect(cleanupOutput.metadata).toEqual({ ...expectedCleanup, supervisor_id: supervisor, force: true })

    for (const output of [
      statusOutput.output,
      permissionOutput.output,
      eventsOutput.output,
      inspectOutput.output,
      cleanupOutput.output,
    ]) {
      expect(output).not.toMatch(/[\u001b\u0000\t]/)
    }
  })

  it('keeps the final conclusion in bounded multiline result previews', () => {
    const evidence = Array.from(
      { length: 30 },
      (_, index) => `analysis-${String(index).padStart(2, '0')}: ${'x'.repeat(64)}`,
    )
    const conclusion = 'FINAL CONCLUSION: ship the implementation.'
    const value = {
      run_id: 'run-1',
      label: 'worker',
      status: 'done',
      ready: true,
      output: [...evidence, conclusion].join('\n'),
    }
    const expectedMetadata = structuredClone(value)

    const formatted = formatResultOutput({}, value)

    expect(formatted.output).toBe([
      'Result: worker — done',
      'Run: run-1',
      'Guidance: Call avenor_result or avenor_events with run_id "run-1".',
      'Final output:',
      ...evidence.slice(-11),
      conclusion,
      '[preview clipped: 923 characters omitted]',
      '… 19 lines omitted',
    ].join('\n'))
    expect(formatted.metadata).toEqual(expectedMetadata)
    expect(value).toEqual(expectedMetadata)
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
