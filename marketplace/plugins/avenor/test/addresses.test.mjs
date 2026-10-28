// Unit tests for the peer layer's pure modules, run with node's own runner:
//
//   node --test marketplace/plugins/avenor/test/addresses.test.mjs \
//     marketplace/plugins/avenor/test/wire.test.mjs
//
// Name the files explicitly: node 24 treats a bare directory argument as a
// module path, not a test root.
//
// These cover the seams a PR inherits: the avenor:* address ladder and the
// broker wire helpers. The live-broker drive (the hook contract end to end)
// is test/preflight.mjs, which needs a real stable supervisor.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { parseAvenorAddress, parseMessageTarget, matchSession } from '../hooks/addresses.js'

test('parseAvenorAddress: only the avenor: prefix is ours', () => {
  assert.equal(parseAvenorAddress('avenor:rt_1'), 'rt_1')
  assert.equal(parseAvenorAddress('avenor:claude-run-1'), 'claude-run-1')
  assert.equal(parseAvenorAddress('avenor:'), '')
  assert.equal(parseAvenorAddress('uds:/tmp/peer.sock'), null)
  assert.equal(parseAvenorAddress('some teammate'), null)
  assert.equal(parseAvenorAddress(''), null)
  assert.equal(parseAvenorAddress(undefined), null)
})

const LISTING = [
  { run_id: 'supervisor', label: 'supervisor', kind: 'supervisor' },
  { run_id: 'rt_1', label: 'claude-run-1' },
  { run_id: 'rt_2', label: 'claude-run-1' }, // same label on two runs
  { run_id: 'rt_3', label: 'probe' },
]

test('matchSession: an exact broker run id wins over any label', () => {
  const hit = matchSession(LISTING, 'rt_3', '')
  assert.equal(hit.match, 'unique')
  assert.equal(hit.runId, 'rt_3')
})

test('matchSession: one exact label resolves, two are ambiguous', () => {
  const one = matchSession(LISTING, 'probe', '')
  assert.equal(one.match, 'unique')
  assert.equal(one.runId, 'rt_3')

  const two = matchSession(LISTING, 'claude-run-1', '')
  assert.equal(two.match, 'ambiguous')
  assert.deepEqual(two.candidates.sort(), ['rt_1', 'rt_2'])
})

test('matchSession: a miss is none, and garbage rows are skipped', () => {
  assert.deepEqual(matchSession(LISTING, 'nobody', ''), { match: 'none', runId: '', candidates: [] })
  assert.deepEqual(matchSession([null, 7, {}], 'nobody', ''), { match: 'none', runId: '', candidates: [] })
})

test('matchSession: the caller never resolves its own run', () => {
  const self = matchSession(LISTING, 'supervisor', 'supervisor')
  assert.equal(self.match, 'none')
  const selfByLabel = matchSession([{ run_id: 'me', label: 'mine' }], 'mine', 'me')
  assert.equal(selfByLabel.match, 'none')
})

test('matchSession: a ref that is both a run id and a label resolves to the id', () => {
  const listing = [{ run_id: 'rt_9', label: 'supervisor' }]
  const hit = matchSession(listing, 'rt_9', '')
  assert.equal(hit.match, 'unique')
  assert.equal(hit.runId, 'rt_9')
})

test('matchSession: a run id in one row beats a label of the same name in another', () => {
  // The run-id rung is read over the whole listing before the label rung, so a
  // peer that happens to be named like another run's id still resolves to the
  // run the ref actually names.
  const crossRow = [
    { run_id: 'rt_7', label: 'rt_9' },
    { run_id: 'rt_9', label: 'unrelated' },
  ]
  const hit = matchSession(crossRow, 'rt_9', '')
  assert.equal(hit.match, 'unique')
  assert.equal(hit.runId, 'rt_9')
  // Two runs naming that label would be ambiguous only when no run id matches.
  const twoLabels = [
    { run_id: 'rt_7', label: 'rt_9' },
    { run_id: 'rt_8', label: 'rt_9' },
  ]
  assert.equal(matchSession(twoLabels, 'rt_9', '').match, 'ambiguous')
})

test('matchSession: an empty ref resolves to nothing without reading rows', () => {
  // `avenor:` with no ref parses to '', and a row may legally carry an empty
  // label: rather than let an unnamed row win, the empty ref short-circuits the
  // way the sibling resolvers refuse it. Pinned so the behavior stays chosen.
  const unnamed = [{ run_id: 'rt_4', label: '' }]
  assert.deepEqual(matchSession(unnamed, '', ''), { match: 'none', runId: '', candidates: [] })
  assert.deepEqual(matchSession(LISTING, '', ''), { match: 'none', runId: '', candidates: [] })
})

test('message targets accept plain refs, self, and avenor addresses without whitespace', () => {
  for (const [input, ref] of [['scout', 'scout'], ['rt_1', 'rt_1'], ['self', 'self'], ['avenor:scout', 'scout'], ['avenor:self', 'self']]) {
    assert.equal(parseMessageTarget(input), ref)
  }
  for (const input of ['', 'avenor:', ' scout', 'scout ', 'some teammate', 'avenor: scout', undefined, 7]) {
    assert.equal(parseMessageTarget(input), null)
  }
  assert.equal(parseAvenorAddress('scout'), null)
})
