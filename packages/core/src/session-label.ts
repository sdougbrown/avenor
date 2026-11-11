import { execFile } from 'node:child_process'
import { MAX_SESSION_NAME_RUNES, RESERVED_SESSION_REFERENCES, stripDisallowedSessionNameRunes } from './message.js'
import type { PeersProviderResult } from './message.js'
import { boundedRunes } from './messaging-text.js'

/**
 * The work-derived default display label for a session: `defaultHostLabel`
 * serves Pi and OpenCode, and mirrors the Claude plugin's hooks/label.js —
 * "[<hostname>-]<host>-<repo>" with no branch. The supervisor's own
 * "<repo>[@branch]" label is derived on the Go side (internal/stable).
 */

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
 * The git top-level of dir, or an empty string when dir is not in a repository.
 * The answer comes from git itself, so GIT_DIR, linked worktrees and symlinked
 * repositories resolve exactly as git sees them.
 */
export function gitToplevel(dir: string, git: GitReader = defaultGitReader): Promise<string> {
  return git(dir, ['rev-parse', '--show-toplevel'])
}

const TAIL_RUNES = 6

/** The last path segment, splitting on either separator and ignoring trailing ones. */
function baseName(path: string): string {
  const parts = path.split(/[/\\]+/).filter(part => part !== '')
  return parts.length === 0 ? '' : parts[parts.length - 1]
}

/** A machine name without its domain: "grace.home.example.com" is "grace". */
export function shortHost(name: string): string {
  return name.trim().split('.')[0]
}

/**
 * Whether the broker runs on this machine: a loopback address, or the
 * machine's own name. An unparseable URL counts as local, so no prefix is
 * added on a guess.
 */
export function brokerIsLocal(brokerUrl: string, hostname: string): boolean {
  const match = /^[a-z][a-z0-9+.-]*:\/\/(?:[^@/]*@)?(\[[^\]]+\]|[^:/?#]+)/i.exec(brokerUrl)
  if (match === null) return true
  const host = match[1].toLowerCase()
  if (host === 'localhost' || host === '[::1]' || /^127(\.\d{1,3}){3}$/.test(host)) return true
  return hostname !== '' && shortHost(host) === shortHost(hostname).toLowerCase()
}

/** The last characters of a run id that tell it from another run's. */
export function runTail(runId: string): string {
  return runId.replace(/[^A-Za-z0-9]/g, '').slice(-TAIL_RUNES)
}

function withTail(base: string, tail: string): string {
  if (tail === '') return base
  const stem = boundedRunes(base, MAX_SESSION_NAME_RUNES - 1 - tail.length).replace(/-+$/, '')
  return stem === '' ? '' : `${stem}-${tail}`
}

export type DefaultHostLabelInput = {
  host: string
  hostname: string
  brokerUrl: string
  dir: string
  toplevel: string
  runId: string
  /** The labels other peers hold, or null when the listing is unreadable or partial. */
  taken: Set<string> | null
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
export function defaultHostLabel(input: DefaultHostLabelInput): string {
  const { host, hostname, brokerUrl, dir, toplevel, runId, taken } = input
  const repo = toplevel === '' ? '' : sanitizeSessionLabel(baseName(toplevel))
  const work = repo !== '' ? repo : sanitizeSessionLabel(baseName(dir))
  if (work === '') return ''
  const prefix = brokerIsLocal(brokerUrl, hostname) ? '' : sanitizeSessionLabel(shortHost(hostname))
  const base = sanitizeSessionLabel([prefix, host, work].filter(part => part !== '').join('-'))
  const crowded = repo === '' || taken === null || taken.has(base)
  const label = crowded ? withTail(base, runTail(runId)) : base
  return labelUsable(label) ? label : ''
}

/**
 * The labels the peers in a listing hold, leaving out the peer whose run id is
 * excludeRunId. A listing marked incomplete (detail set) is no information and
 * yields undefined, which the caller passes on as null so the label takes its
 * run-id tail rather than guess at labels the listing did not carry.
 */
export function takenLabelsFromListing(listing: PeersProviderResult, excludeRunId?: string): Set<string> | undefined {
  if (listing.detail !== undefined) return undefined
  const taken = new Set<string>()
  for (const peer of listing.peers) {
    if (peer.label && peer.run_id !== excludeRunId) taken.add(peer.label)
  }
  return taken
}
