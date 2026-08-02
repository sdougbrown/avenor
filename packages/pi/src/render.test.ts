import { describe, expect, it } from 'bun:test'
import type { Theme } from '@earendil-works/pi-coding-agent'
import {
  renderAnswerPermissionCall,
  renderAnswerPermissionResult,
  renderEventsCall,
  renderEventsResult,
  renderFollowUpCall,
  renderFollowUpResult,
  renderInspectCall,
  renderInspectResult,
  renderResultCall,
  renderResultResult,
  renderShutdownCall,
  renderShutdownResult,
  renderStatusCall,
  renderStatusResult,
} from './render.js'

const theme = {
  fg: (_color: string, text: string) => text,
  bold: (text: string) => text,
} as Theme

const collapsed = { expanded: false, isPartial: false }
const expanded = { expanded: true, isPartial: false }

function text(component: { render(width: number): string[] }): string {
  return component.render(2_000).map(line => line.trimEnd()).join('\n')
}

function result(details: unknown, content = 'agent JSON'): any {
  return { content: [{ type: 'text', text: content }], details }
}

describe('Avenor Pi renderers', () => {
  it('uses the exact compact call templates for all non-spawn tools', () => {
    expect(text(renderStatusCall({}, theme))).toBe('avenor_status (all runs)')
    expect(text(renderStatusCall({ run_id: 'run', view: 'full', supervisor_id: '/tmp/sock' }, theme))).toBe('avenor_status run_id "run" view "full" supervisor_id "/tmp/sock"')
    expect(text(renderResultCall({ run_id: 'run', wait: false, timeout: '5m', supervisor_id: '/tmp/sock' }, theme))).toBe('avenor_result run_id "run" wait=false timeout "5m" supervisor_id "/tmp/sock"')
    expect(text(renderInspectCall({ run_id: 'run', limit: 10, after_seq: 3, supervisor_id: '/tmp/sock' }, theme))).toBe('avenor_inspect run_id "run" limit=10 after_seq=3 supervisor_id "/tmp/sock"')
    expect(text(renderAnswerPermissionCall({ run_id: 'run', option_id: 'allow', request_id: 'request', message: 'yes', supervisor_id: '/tmp/sock' }, theme))).toBe('avenor_answer_permission run_id "run" option_id "allow" request_id "request" message "yes" supervisor_id "/tmp/sock"')
    expect(text(renderFollowUpCall({ run_id: 'run', message: 'continue', label: 'again', supervisor_id: '/tmp/sock' }, theme))).toBe('avenor_follow_up run_id "run" message "continue" label "again" supervisor_id "/tmp/sock"')
    expect(text(renderEventsCall({ run_id: 'run', types: ['agent.message', 'session.end'], limit: 5, supervisor_id: '/tmp/sock' }, theme))).toBe('avenor_events run_id "run" types=["agent.message", "session.end"] limit=5 supervisor_id "/tmp/sock"')
    expect(text(renderShutdownCall({ supervisor_id: '/tmp/sock', force: true }, theme))).toBe('avenor_shutdown supervisor_id "/tmp/sock" force=true')
  })

  it('renders exact status and result summaries while keeping expanded details separate', () => {
    const status = {
      run_id: 'run-1',
      label: 'worker',
      status: 'waiting',
      phase: 'tool',
      phase_label: 'Read files',
      session_id: 'session-1',
      backend: 'pi',
      agent: 'horse',
      model: 'model',
      latest_seq: 4,
      pending_permission: {
        description: 'Read /tmp',
        options: [{ option_id: 'allow_once', label: 'Allow once' }],
      },
      final_output: 'first line\nsecond line',
    }
    expect(text(renderStatusResult(result(status), collapsed, theme, { run_id: 'call-run' }))).toBe([
      'Status: worker — waiting',
      'Run: run-1',
      'Permission: Read /tmp',
      'Options: allow_once (Allow once)',
      'Final output:',
      'first line',
      'second line',
      'Guidance: Call avenor_answer_permission with run_id "run-1" and option_id "allow_once".',
    ].join('\n'))
    expect(text(renderStatusResult(result(status), expanded, theme, { run_id: 'call-run' }))).toContain([
      'Phase: tool',
      'Phase label: Read files',
      'Session: session-1',
      'Backend: pi',
      'Agent: horse',
      'Model: model',
      'Latest sequence: 4',
    ].join('\n'))

    const timedOut = {
      run_id: 'run-2',
      label: 'reader',
      status: 'running',
      ready: false,
      timed_out: true,
      output_truncated: true,
      output_event_path: '/tmp/events.log',
      output: 'partial answer',
      session_id: 'session-2',
      stop_reason: 'budget',
    }
    expect(text(renderResultResult(result(timedOut), expanded, theme, { run_id: 'call-run' }))).toBe([
      'Result: reader — running',
      'Run: run-2',
      'Warning: wait timed out; the run is still active.',
      'Guidance: Call avenor_result with run_id "run-2".',
      'Warning: final output may be truncated; retry avenor_result or read the durable event path "/tmp/events.log".',
      'Final output:',
      'partial answer',
      'Session: session-2',
      'Stop reason: budget',
    ].join('\n'))
  })

  it('renders permission, follow-up, events, inspect, and shutdown without serializing details', () => {
    expect(text(renderAnswerPermissionResult(result({ ok: true }), collapsed, theme, {
      run_id: 'run-1', option_id: 'allow_once', request_id: 'request-1',
    }))).toBe([
      'Permission answer: accepted.',
      'Run: run-1',
      'Option: allow_once',
      'Request: request-1',
      'Guidance: Call avenor_status with run_id "run-1" to verify the permission state.',
    ].join('\n'))

    expect(text(renderFollowUpResult(result({ run_id: 'run-2', label: 'follow-up' }), collapsed, theme, {
      run_id: 'run-1', message: 'continue',
    }))).toContain('Source run: run-1\nRun: run-2\nLabel: follow-up')

    const events = { events: [{ event: 'agent.message', seq: '2', delta: 'hello' }, { type: 'session.end', seq: 3 }] }
    expect(text(renderEventsResult(result(events), expanded, theme, { run_id: 'run-1', types: ['agent.message'] }))).toBe([
      'Events: run-1 — 2 events',
      'Filter: agent.message',
      'Preview: Event 2: agent.message — hello',
      '… 1 items omitted',
      'Event 2: agent.message — hello',
      'Event 3: session.end',
    ].join('\n'))

    const inspect = {
      run_id: 'run-1',
      label: 'worker',
      status: { status: 'done' },
      final_output: 'answer',
      transcript: [{ kind: 'assistant', text: 'answer' }],
      tools: [{ title: 'read', status: 'done', preview: 'file' }],
      live_tools: [{ kind: 'bash', preview: 'echo hi' }],
      permissions: [{ description: 'Read file', request_id: 'request-1', options: [{ option_id: 'allow', label: 'Allow' }] }],
    }
    expect(text(renderInspectResult(result(inspect), expanded, theme, { run_id: 'call-run' }))).toContain([
      'Inspect: worker — done',
      'Run: run-1',
      'Counts: transcript=1, tools=1, live_tools=1, permissions=1',
      'Final output:',
      'answer',
      'Guidance: Call avenor_events with run_id "run-1" for raw events.',
      'Transcript:',
      'assistant: answer',
      'Tools:',
      'Tool: read (done): file',
      'Live tools:',
      'Live tool: bash: echo hi',
      'Permissions:',
      'Permission: Read file (request_id: request-1); options: allow (Allow)',
    ].join('\n'))

    expect(text(renderShutdownResult(result({ ok: false, cleaned_up: ['/tmp/one'] }), expanded, theme, {
      supervisor_id: '/tmp/sock', force: true,
    }))).toBe([
      'Avenor shutdown: failed.',
      'Supervisor: /tmp/sock',
      'Mode: force',
      'Cleanup: 1 paths',
      'Warning: shutdown did not complete.',
      'Guidance: retry avenor_shutdown.',
      'Cleanup path: /tmp/one',
    ].join('\n'))
  })

  it('keeps result, event, and shutdown details out of collapsed renderings', () => {
    const collapsedResult = text(renderResultResult(result({
      run_id: 'run-1', label: 'worker', status: 'done', output: 'answer', session_id: 'session-1', stop_reason: 'budget',
    }), collapsed, theme, { run_id: 'call-run' }))
    expect(collapsedResult).toBe([
      'Result: worker — done',
      'Run: run-1',
      'Guidance: Call avenor_result or avenor_events with run_id "run-1".',
      'Final output:',
      'answer',
    ].join('\n'))
    expect(collapsedResult).not.toContain('Session:')
    expect(collapsedResult).not.toContain('Stop reason:')

    const collapsedEvents = text(renderEventsResult(result({
      events: [
        { event: 'agent.message', seq: 1, delta: 'first' },
        { event: 'session.end', seq: 2, delta: 'second' },
      ],
    }), collapsed, theme, { run_id: 'run-1' }))
    expect(collapsedEvents).toBe([
      'Events: run-1 — 2 events',
      'Preview: Event 1: agent.message — first',
      '… 1 items omitted',
    ].join('\n'))
    expect(collapsedEvents).not.toContain('Event 2: session.end')

    const emptyCollapsedEvents = text(renderEventsResult(result({ events: [] }), collapsed, theme, { run_id: 'run-1' }))
    expect(emptyCollapsedEvents).toBe([
      'Events: run-1 — 0 events',
      'No events.',
    ].join('\n'))

    const collapsedShutdown = text(renderShutdownResult(result({ ok: true, cleaned_up: ['/tmp/one'] }), collapsed, theme, {
      supervisor_id: '/tmp/sock', force: false,
    }))
    expect(collapsedShutdown).toBe([
      'Avenor shutdown: succeeded.',
      'Supervisor: /tmp/sock',
      'Mode: graceful',
      'Cleanup: 1 paths',
    ].join('\n'))
    expect(collapsedShutdown).not.toContain('Cleanup path:')
  })

  it('caps hostile collection displays without changing details', () => {
    const hostile = (value: string) => `\u001b[31m${value}\u001b[0m\t\u0000`
    const marker = '… 2 items omitted'

    const statuses = Array.from({ length: 14 }, (_, index) => ({
      run_id: hostile(`run-${index}`),
      label: hostile(`worker-${index}`),
      status: 'running',
    }))
    const expectedStatuses = structuredClone(statuses)
    const statusResult = result(statuses)
    const statusText = text(renderStatusResult(statusResult, collapsed, theme, {}))
    const statusLines = statusText.split('\n')
    expect(statusLines.filter(line => line.includes('(run_id:'))).toHaveLength(12)
    expect(statusLines.filter(line => line === marker)).toEqual([marker])
    expect(statusText).toContain('worker-0 — running (run_id: run-0)')
    expect(statusText).not.toContain('worker-12')
    expect(statusResult.details).toEqual(expectedStatuses)

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
    const permissionResult = result(permissionStatus)
    const permissionText = text(renderStatusResult(permissionResult, collapsed, theme, {}))
    expect(permissionText.split('\n').find(line => line.startsWith('Options:'))).toBe(
      `Options: ${Array.from({ length: 8 }, (_, index) => `option-${index} (label-${index})`).join(', ')} ${marker}`,
    )
    expect(permissionText).not.toContain('option-8')
    expect(permissionResult.details).toEqual(expectedPermissionStatus)

    const types = Array.from({ length: 10 }, (_, index) => hostile(`type-${index}`))
    const events = Array.from({ length: 14 }, (_, index) => ({
      event: hostile(`event-${index}`),
      seq: index,
      delta: hostile(`delta-${index}`),
    }))
    const eventsPayload = { events }
    const expectedEvents = structuredClone(eventsPayload)
    const eventsResult = result(eventsPayload)
    const eventsText = text(renderEventsResult(eventsResult, expanded, theme, {
      run_id: hostile('events-run'), types,
    }))
    const eventLines = eventsText.split('\n')
    expect(text(renderEventsCall({ run_id: hostile('events-run'), types }, theme))).toBe(
      `avenor_events run_id "events-run" types=[${Array.from({ length: 8 }, (_, index) => `"type-${index}"`).join(', ')}] ${marker}`,
    )
    expect(eventLines.find(line => line.startsWith('Filter:'))).toBe(
      `Filter: ${Array.from({ length: 8 }, (_, index) => `type-${index}`).join(', ')} ${marker}`,
    )
    expect(eventLines.filter(line => line.startsWith('Event '))).toHaveLength(12)
    expect(eventLines.filter(line => line === '… 13 items omitted')).toEqual(['… 13 items omitted'])
    expect(eventLines.filter(line => line === marker)).toEqual([marker])
    expect(eventsText).toContain('Event 0: event-0 — delta-0')
    expect(eventsText).not.toContain('event-12')
    expect(eventsResult.details).toEqual(expectedEvents)

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
      transcript,
      tools,
      live_tools: liveTools,
      permissions,
    }
    const expectedInspect = structuredClone(inspect)
    const inspectResult = result(inspect)
    const inspectText = text(renderInspectResult(inspectResult, expanded, theme, {}))
    const inspectLines = inspectText.split('\n')
    expect(inspectLines.filter(line => line.startsWith('assistant: transcript-'))).toHaveLength(5)
    expect(inspectLines.filter(line => line.startsWith('Tool: tool-'))).toHaveLength(5)
    expect(inspectLines.filter(line => line.startsWith('Live tool: live-'))).toHaveLength(5)
    expect(inspectLines.filter(line => line.startsWith('Permission: permission-'))).toHaveLength(5)
    expect(inspectLines.filter(line => line === marker)).toEqual([marker, marker, marker, marker])
    expect(inspectText).not.toContain('transcript-5')
    expect(inspectText).not.toContain('tool-5')
    expect(inspectText).not.toContain('live-5')
    expect(inspectText).not.toContain('permission-5')
    expect(inspectResult.details).toEqual(expectedInspect)

    const cleanup = { ok: true, cleaned_up: Array.from({ length: 10 }, (_, index) => hostile(`/tmp/cleanup-${index}`)) }
    const expectedCleanup = structuredClone(cleanup)
    const cleanupResult = result(cleanup)
    const cleanupText = text(renderShutdownResult(cleanupResult, expanded, theme, {
      supervisor_id: hostile('/tmp/sock'), force: true,
    }))
    const cleanupLines = cleanupText.split('\n')
    expect(cleanupLines.filter(line => line.startsWith('Cleanup path:'))).toHaveLength(8)
    expect(cleanupLines.filter(line => line === marker)).toEqual([marker])
    expect(cleanupText).toContain('Cleanup path: /tmp/cleanup-0')
    expect(cleanupText).not.toContain('cleanup-8')
    expect(cleanupResult.details).toEqual(expectedCleanup)

    for (const output of [statusText, permissionText, eventsText, inspectText, cleanupText]) {
      expect(output).not.toMatch(/[\u001b\u0000\t]/)
    }
  })

  it('defensively renders partial and malformed results, strips controls, and marks bounded previews', () => {
    expect(text(renderStatusResult(result({ label: 'worker', status: 'running' }), { expanded: false, isPartial: true }, theme, {}))).toBe('Working: worker — running')
    expect(text(renderStatusResult(result(undefined, '{"run_id":"raw"}'), { expanded: false, isPartial: true }, theme, {}))).toBe('Working…')
    expect(text(renderStatusResult(result(undefined), collapsed, theme, {}))).toBe([
      'Avenor status result unavailable.',
      'Retry the tool or use avenor_status/avenor_inspect.',
    ].join('\n'))

    const hostileOutput = `\u001b[31mSTART-MARKER hello\tworld\u0000\n${Array.from({ length: 20 }, (_, index) => `line-${index}`).join('\n')}${'x'.repeat(700)}TAIL-MARKER`
    const rendered = text(renderResultResult(result({
      run_id: 'run-1', label: 'worker', status: 'done', ready: true, output: hostileOutput,
    }), collapsed, theme, {}))
    expect(rendered).not.toContain('\u001b')
    expect(rendered).not.toContain('\u0000')
    expect(rendered).toContain('START-MARKER')
    expect(rendered).not.toContain('TAIL-MARKER')
    expect(rendered).toContain('[preview clipped:')
    expect(rendered).toContain('lines omitted')
    expect(rendered.indexOf('[preview clipped:')).toBeLessThan(rendered.indexOf('lines omitted'))
  })

  it('falls back when a result getter throws during rendering', () => {
    let getterRead = false
    const throwingDetails = Object.defineProperty({}, 'events', {
      get() {
        getterRead = true
        throw new Error('intentional renderer test failure')
      },
    })

    expect(text(renderEventsResult(result(throwingDetails), collapsed, theme, { run_id: 'run-1' }))).toBe([
      'Avenor events result unavailable.',
      'Retry the tool or use avenor_status/avenor_inspect.',
    ].join('\n'))
    expect(getterRead).toBe(true)
  })
})
