const PROFILE_ENTRY_TYPE = 'pi-agents:profile'

type SessionEntry = {
  type?: unknown
  customType?: unknown
  data?: unknown
}

/**
 * Read the optional pi-agents session convention without depending on its
 * package. A namespaced environment variable is an optional launch override.
 */
export function resolveAgentProfile(
  entries: readonly unknown[] | undefined,
  environment: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const fromEnvironment = environment.PI_AGENT_PROFILE
  if (fromEnvironment) return fromEnvironment

  let selected: string | undefined
  for (const entry of entries ?? []) {
    if (!entry || typeof entry !== 'object') continue
    const custom = entry as SessionEntry
    if (custom.type !== 'custom' || custom.customType !== PROFILE_ENTRY_TYPE) continue
    if (!custom.data || typeof custom.data !== 'object') continue

    const name = (custom.data as { name?: unknown }).name
    if (typeof name === 'string') selected = name
    if (name === null) selected = undefined
  }
  return selected
}
