// The default display label for a session that no person has named: the host,
// the git top-level's base name, and "@<branch>" off the default branches.
// The sanitizing and the work label mirror internal/stable/sessiondefault.go
// and packages/opencode/src/label.ts; schemas/session-name-rule.json pins all
// three to the same answers.

const MAX_NAME_RUNES = 64
const MAX_SUFFIX = 9
const DEFAULT_BRANCHES = ["main", "master"]
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

/** "<repo>" or "<repo>@<branch>" for a git top-level and its checked-out branch. */
export function workLabel(toplevel, branch) {
  const repo = baseName(toplevel)
  const carried = branch !== "" && !DEFAULT_BRANCHES.includes(branch)
  return sanitizeSessionLabel(carried ? repo + "@" + branch : repo)
}

/** The label a host's session registers under, or "" when nothing usable derives. */
export function hostLabel(host, work) {
  const label = sanitizeSessionLabel(host + "-" + work)
  return work !== "" && labelUsable(label) ? label : ""
}

/**
 * The host's default label for a session in dir: the git top-level and branch
 * when git answered, and the directory's own name when it did not or when the
 * top-level yields no usable label.
 */
export function defaultHostLabel(host, dir, toplevel, branch) {
  const fromGit = toplevel === "" ? "" : hostLabel(host, workLabel(toplevel, branch))
  return fromGit !== "" ? fromGit : hostLabel(host, workLabel(dir, ""))
}

/**
 * The first of base, base-2 … base-9 that no listed peer holds. A listing the
 * caller could not read (null) keeps the base: guessing at unlisted labels
 * would invent names.
 */
export function pickLabel(base, taken) {
  if (taken === null || !taken.has(base)) {
    return base
  }
  const stem = boundedRunes(base, MAX_NAME_RUNES - String(MAX_SUFFIX).length - 1).replace(/-+$/, "")
  for (let n = 2; n <= MAX_SUFFIX && stem !== ""; n += 1) {
    const candidate = stem + "-" + n
    if (!taken.has(candidate)) {
      return candidate
    }
  }
  return base
}
