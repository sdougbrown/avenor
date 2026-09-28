#!/usr/bin/env node
import { parseArgs } from 'node:util'

import { parseDuration } from './duration.js'
import { runBridge } from './run.js'

const USAGE = `Usage: avenor-escalation-bridge --socket <path> --workflow-id <id> \\
  --webhook-url <url> --decision-dir <dir> --template <template.json> \\
  [--wait 30s] [--gate-timeout 15m] [--poll 2s]

Watches a durable Avenor workflow for parked awaiting_gate activations, POSTs
each pending human gate to the transport webhook, polls --decision-dir for
decision-<activation>-<gate>.json files, and records the decision through the
kernel's gate command path. See docs/escalation.md for the full protocol.`

const { values } = parseArgs({
  options: {
    socket: { type: 'string' },
    'workflow-id': { type: 'string' },
    'webhook-url': { type: 'string' },
    'decision-dir': { type: 'string' },
    template: { type: 'string' },
    wait: { type: 'string', default: '30s' },
    'gate-timeout': { type: 'string', default: '15m' },
    poll: { type: 'string', default: '2s' },
  },
})

const required = ['socket', 'workflow-id', 'webhook-url', 'decision-dir', 'template'] as const
const missing = required.filter((key) => !values[key])
if (missing.length > 0) {
  process.stderr.write(`${USAGE}\nerror: missing required arguments: ${missing.join(', ')}\n`)
  process.exit(2)
}

let waitMs: number
let gateTimeoutMs: number
let pollMs: number
try {
  waitMs = parseDuration(values.wait!) * 1000
  gateTimeoutMs = parseDuration(values['gate-timeout']!) * 1000
  pollMs = parseDuration(values.poll!) * 1000
} catch (exc) {
  process.stderr.write(`${USAGE}\nerror: ${(exc as Error).message}\n`)
  process.exit(2)
}

process.on('SIGINT', () => {
  console.log('bridge: interrupted')
  process.exit(130)
})

process.exit(
  await runBridge({
    socketPath: values.socket!,
    workflowId: values['workflow-id']!,
    webhookUrl: values['webhook-url']!,
    decisionDir: values['decision-dir']!,
    templatePath: values.template!,
    waitMs,
    gateTimeoutMs,
    pollMs,
  }),
)
