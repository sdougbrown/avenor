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

import { parseAvenorAddress, matchSession } from '../hooks/addresses.js'

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