// The Claude plugin's copy of the default-label derivation, driven over the
// shared fixture the Go and TypeScript derivations answer to.

import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import test from "node:test"

import { brokerIsLocal, defaultHostLabel, labelUsable, sanitizeSessionLabel, shortHost } from "../hooks/label.js"

const rule = JSON.parse(readFileSync(fileURLToPath(new URL("../../../../schemas/session-name-rule.json", import.meta.url)), "utf8"))

for (const c of rule.sanitizeCases) {
  test("sanitizes: " + c.name, () => {
    assert.equal(sanitizeSessionLabel(c.input), c.label)
  })
}

for (const c of rule.hostLabelCases) {
  test("host label: " + c.name, () => {
    const taken = c.taken === null ? null : new Set(c.taken)
    assert.equal(defaultHostLabel({ ...c, taken }), c.label)
  })
}

test("the name rule agrees with the fixture's accepts and refusals", () => {
  for (const c of rule.cases) {
    assert.equal(labelUsable(c.input), c.valid, c.name)
  }
})

test("a broker is local when it is loopback or the machine itself", () => {
  assert.equal(brokerIsLocal("http://127.0.0.1:8787", "fenwick"), true)
  assert.equal(brokerIsLocal("http://127.1.2.3", "fenwick"), true)
  assert.equal(brokerIsLocal("http://user:pw@localhost:1/x", "fenwick"), true)
  assert.equal(brokerIsLocal("https://FENWICK.lan:8443/", "fenwick.home"), true)
  assert.equal(brokerIsLocal("https://broker.example.com", "fenwick"), false)
  assert.equal(brokerIsLocal("https://broker.example.com", ""), false)
  assert.equal(brokerIsLocal("not a url", "fenwick"), true)
  assert.equal(shortHost("grace.home.example.com\n"), "grace")
})
