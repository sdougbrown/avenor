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
      'Warning: final output may be truncated; use avenor_events with run_id "run-2" and path "/tmp/events.log".',
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
})
