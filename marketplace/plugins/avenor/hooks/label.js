// The default display label for a session that no person has named:
// "[<hostname>-]<agent>-<repo>", with a short run-id tail where the name alone
// cannot be trusted to be unique. The sanitizing mirrors
// internal/stable/sessiondefault.go and packages/opencode/src/label.ts;
// schemas/session-name-rule.json pins them to the same answers, and its
// hostLabelCases pin this derivation.

const MAX_NAME_RUNES = 64
const TAIL_RUNES = 6
const RESERVED = ["self", "supervisor"]
const DISALLOWED = /[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/gu

function boundedRunes(text, limit) {
  return Array.from(text).slice(0, limit).join("")
}

/** A derived label cut down to what the session name rule accepts. */
export function sanitizeSessionLabel(label) {
  const separated = label.replace(DISALLOWED, "").replace(/[/\\\s]+/gu, "-").replace(/-{2,}/g, "-")
  return boundedRunes(separated, MAX_NAME_RUNES).replace(/^-+|-+$/g, "")
}

/** Whether a sanitized label may stand as a display name. */
export function labelUsable(label) {
  const runes = Array.from(label).length
  return runes > 0 && runes <= MAX_NAME_RUNES && !RESERVED.includes(label) && label.replace(DISALLOWED, "") === label
}

/** The base name of a path, ignoring trailing separators. */
export function baseName(path) {
  const parts = path.split(/[/\\]+/).filter((part) => part !== "")
  return parts.length === 0 ? "" : parts[parts.length - 1]
}

/** A machine name without its domain: "grace.home.example.com" is "grace". */
export function shortHost(name) {
  return name.trim().split(".")[0]
}

/**
 * Whether the broker runs on this machine: a loopback address, or the
 * machine's own name. An unparseable URL counts as local, so no prefix is
 * added on a guess.
 */
export function brokerIsLocal(brokerUrl, hostname) {
  const match = /^[a-z][a-z0-9+.-]*:\/\/(?:[^@/]*@)?(\[[^\]]+\]|[^:/?#]+)/i.exec(brokerUrl)
  if (match === null) {
    return true
  }
  const host = match[1].toLowerCase()
  if (host === "localhost" || host === "[::1]" || /^127(\.\d{1,3}){3}$/.test(host)) {
    return true
  }
  return hostname !== "" && shortHost(host) === shortHost(hostname).toLowerCase()
}

/** The last characters of a run id that tell it from another run's. */
export function runTail(runId) {
  return runId.replace(/[^A-Za-z0-9]/g, "").slice(-TAIL_RUNES)
}

function withTail(base, tail) {
  if (tail === "") {
    return base
  }
  const stem = boundedRunes(base, MAX_NAME_RUNES - 1 - tail.length).replace(/-+$/, "")
  return stem === "" ? "" : stem + "-" + tail
}

/**
 * The default label for a session of agent `host` in `dir`:
 * "[<hostname>-]<host>-<repo>". The repo is the git top-level's base name,
 * which for a linked worktree is the worktree's own directory. The hostname
 * leads only when the broker is on another machine. The run-id tail is added
 * when there is no repo to name (the directory stands in), when another peer
 * holds the label, and when the listing could not be read (`taken` null).
 * Empty means nothing usable derives and the run keeps its minted id.
 */
export function defaultHostLabel({ host, hostname, brokerUrl, dir, toplevel, runId, taken }) {
  const repo = toplevel === "" ? "" : sanitizeSessionLabel(baseName(toplevel))
  const work = repo !== "" ? repo : sanitizeSessionLabel(baseName(dir))
  if (work === "") {
    return ""
  }
  const prefix = brokerIsLocal(brokerUrl, hostname) ? "" : sanitizeSessionLabel(shortHost(hostname))
  const base = sanitizeSessionLabel([prefix, host, work].filter((part) => part !== "").join("-"))
  const crowded = repo === "" || taken === null || taken.has(base)
  const label = crowded ? withTail(base, runTail(runId)) : base
  return labelUsable(label) ? label : ""
}
