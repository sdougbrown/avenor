// The Claude plugin's copy of the default-label derivation, driven over the
// shared fixture the Go and OpenCode derivations answer to.

import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import test from "node:test"

import { hostLabel, labelUsable, pickLabel, sanitizeSessionLabel, workLabel } from "../hooks/label.js"

const rule = JSON.parse(readFileSync(fileURLToPath(new URL("../../../../schemas/session-name-rule.json", import.meta.url)), "utf8"))

for (const c of rule.sanitizeCases) {
  test("sanitizes: " + c.name, () => {
    assert.equal(sanitizeSessionLabel(c.input), c.label)
  })
}

for (const c of rule.deriveCases) {
  test("derives: " + c.name, () => {
    assert.equal(workLabel(c.toplevel, c.branch), c.label)
  })
}

for (const c of rule.hostCases) {
  test("host label: " + c.name, () => {
    assert.equal(hostLabel(c.host, c.work), c.label)
  })
}

test("the name rule agrees with the fixture's accepts and refusals", () => {
  for (const c of rule.cases) {
    assert.equal(labelUsable(c.input), c.valid, c.name)
  }
})

for (const c of rule.pickCases) {
  test("picks: " + c.name, () => {
    assert.equal(pickLabel(c.base, new Set(c.taken)), c.label)
  })
}

test("a taken label takes the first free suffix, within the limit", () => {
  assert.equal(pickLabel("claude-avenor", new Set()), "claude-avenor")
  assert.equal(pickLabel("claude-avenor", new Set(["claude-avenor"])), "claude-avenor-2")
  assert.equal(pickLabel("claude-avenor", new Set(["claude-avenor", "claude-avenor-2"])), "claude-avenor-3")
  const long = "x".repeat(64)
  assert.equal(Array.from(pickLabel(long, new Set([long]))).length, 64)
  assert.ok(pickLabel(long, new Set([long])).endsWith("-2"))
})

test("an unreadable listing keeps the base, and nine taken suffixes keep it too", () => {
  assert.equal(pickLabel("claude-avenor", null), "claude-avenor")
  const all = new Set(["claude-avenor"])
  for (let n = 2; n <= 9; n += 1) {
    all.add("claude-avenor-" + n)
  }
  assert.equal(pickLabel("claude-avenor", all), "claude-avenor")
})
