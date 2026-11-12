/**
 * Whether a terminal sentinel leaves a session worth resuming. Mirrors the only
 * hit Go's `runFactsFromStatus` reaches for a registry entry: a non-empty
 * session id. The sentinel word never decides this.
 */
export function resumableFromSession(sessionId: unknown): boolean {
  return typeof sessionId === 'string' && sessionId !== ''
}
