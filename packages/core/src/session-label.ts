import { execFile } from 'node:child_process'
import * as nodePath from 'node:path'
import { MAX_SESSION_NAME_RUNES, RESERVED_SESSION_REFERENCES, stripDisallowedSessionNameRunes } from './message.js'
import { boundedRunes } from './messaging-text.js'

/**
 * The work-derived default display label for a session: the base name of the
 * git top-level of the session's directory, plus "@<branch>" when the
 * checked-out branch is not one of the conventional default names. It mirrors
 * the supervisor's own session-label derivation so a session that never
 * carries a native title still appears under the work it serves.
 */

// The repository's real default branch would mean resolving origin/HEAD — a
// config or network read on every derivation — so the derivation treats the
// two conventional names as the default ones instead and carries any other
// branch in the label.
const DEFAULT_BRANCH_NAMES = new Set(['main', 'master'])

const GIT_TIMEOUT_MS = 5_000

/** Read one git answer for dir, or an empty string when git fails or hangs. */
export type GitReader = (dir: string, args: string[]) => Promise<string>

function defaultGitReader(dir: string, args: string[]): Promise<string> {
  return new Promise(resolve => {
    execFile('git', ['-C', dir, ...args], { timeout: GIT_TIMEOUT_MS }, (error, stdout) => {
      resolve(error ? '' : stdout.trim())
    })
  })
}

/**
 * Turn a derived label into one the session name rule accepts: runes no
 * identifier may hold are dropped, path separators and whitespace runs become
 * a single separator, and the result is truncated to the name limit without
 * splitting a rune, then trimmed of separators.
 */
export function sanitizeSessionLabel(label: string): string {
  const stripped = stripDisallowedSessionNameRunes(label)
  const separated = stripped.replace(/[/\\\s]+/gu, '-')
  const collapsed = separated.replace(/-{2,}/g, '-')
  const truncated = boundedRunes(collapsed, MAX_SESSION_NAME_RUNES)
  return truncated.replace(/^-+|-+$/g, '')
}

export function labelUsable(label: string): boolean {
  // The name rule counts runes (code points), so the limit check must too: a
  // code-unit length would refuse a limit-length label holding an astral rune.
  const runes = Array.from(label).length
  // A name is unusable when stripping it changes it: it held a rune no
  // identifier may hold, the same class ValidSessionName refuses.
  return runes > 0 && runes <= MAX_SESSION_NAME_RUNES && !RESERVED_SESSION_REFERENCES.has(label) && stripDisallowedSessionNameRunes(label) === label
}

/**
 * The default label for a session working in dir, or an empty string when
 * neither git nor the directory yield anything usable. Both answers come from
 * git itself, so GIT_DIR, linked worktrees and symlinked repositories resolve
 * exactly as git sees them; a missing or unreadable repository falls back to
 * the directory's own base name.
 */
export async function sessionLabel(dir: string | undefined, git: GitReader = defaultGitReader): Promise<string> {
  if (!dir) return ''
  const toplevel = await git(dir, ['rev-parse', '--show-toplevel'])
  if (toplevel) {
    let label = nodePath.basename(toplevel)
    const branch = await git(dir, ['branch', '--show-current'])
    if (branch && !DEFAULT_BRANCH_NAMES.has(branch)) label += '@' + branch
    const candidate = sanitizeSessionLabel(label)
    if (labelUsable(candidate)) return candidate
  }
  const fallback = sanitizeSessionLabel(nodePath.basename(dir))
  return labelUsable(fallback) ? fallback : ''
}

/** The label a host's session registers under, or "" when nothing usable derives. */
export function hostLabel(host: string, work: string): string {
  const label = sanitizeSessionLabel(`${host}-${work}`)
  return work !== '' && labelUsable(label) ? label : ''
}

// The highest -N collision suffix a default label carries, and that suffix
// spelled out: the room a truncation must leave for one.
const MAX_LABEL_SUFFIX = 9
const MAX_LABEL_SUFFIX_TEXT = '-9'

/**
 * The label a session publishes for its derived base: the base itself when no
 * peer holds it, then the base-2 … base-9 suffixed variants, the base cut to
 * leave room for the longest suffix once suffixing starts. When every variant
 * is taken, or the peers read returned no information (undefined), the
 * unsuffixed base is kept rather than a name invented.
 */
export function pickSessionLabel(base: string, taken: Set<string> | undefined): string {
  if (!taken || !taken.has(base)) return base
  let stem = boundedRunes(base, MAX_SESSION_NAME_RUNES - MAX_LABEL_SUFFIX_TEXT.length)
  // A dash at the cut boundary would double with the appended separator.
  stem = stem.replace(/-+$/, '')
  if (stem !== '') {
    for (let n = 2; n <= MAX_LABEL_SUFFIX; n++) {
      const candidate = `${stem}-${n}`
      if (!taken.has(candidate)) return candidate
    }
  }
  return base
}
