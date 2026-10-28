// The avenor:* address adapter — the one seam that answers how an
// "avenor:*" SendMessage address maps onto the broker's run listing. The
// hooks in register.js read this module only; pointing them at another
// adapter changes the address space without touching the hook contract.
//
// Pure functions: nothing here touches the engine call object, so it can be
// swapped freely. The resolution ladder has one rung per the broker's own:
// an exact broker run id first, then one exact label, else the ambiguity
// (or nothing).

const AVENOR_PREFIX = "avenor:"

/**
 * The ref beneath the "avenor:" prefix, or null when the address is not
 * ours, so a send to anything else passes through to the normal pipeline.
 */
export function parseAvenorAddress(to) {
  return typeof to === "string" && to.indexOf(AVENOR_PREFIX) === 0
    ? to.slice(AVENOR_PREFIX.length)
    : null
}

/**
 * Resolution over the /sessions listing rows. `excludeRunId` drops the
 * caller's own record, so a session can never route a message back to
 * itself through its own layer.
 *
 * An empty ref resolves to nothing before the rows are read: the sibling
 * resolvers refuse it the same way (packages/core/src/tools/message-provider.ts
 * short-circuits `ref === ''`), and no row can legitimately be named "".
 */
export function matchSession(sessions, ref, excludeRunId) {
  if (ref === "") return { match: "none", runId: "", candidates: [] }
  const rows = Array.isArray(sessions) ? sessions : []
  let runIDMatch = null
  const labelMatches = []
  for (const row of rows) {
    const entry = row === null || typeof row !== "object" ? {} : row
    const runID = typeof entry.run_id === "string" ? entry.run_id : ""
    const label = typeof entry.label === "string" ? entry.label : ""
    if (runID === "" || runID === excludeRunId) continue
    if (runID === ref && runIDMatch === null) runIDMatch = runID
    if (label === ref && labelMatches.indexOf(runID) === -1) labelMatches.push(runID)
  }
  if (runIDMatch !== null) return { match: "unique", runId: runIDMatch, candidates: [] }
  if (labelMatches.length === 1) return { match: "unique", runId: labelMatches[0], candidates: [] }
  if (labelMatches.length > 1) return { match: "ambiguous", runId: "", candidates: labelMatches }
  return { match: "none", runId: "", candidates: [] }
}