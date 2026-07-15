export function asRecord(value: unknown): Record<string, unknown> | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return null
  }
  return value as Record<string, unknown>
}

export function stringField(
  record: Record<string, unknown> | null | undefined,
  ...keys: string[]
): string | undefined {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'string' && value.length > 0) {
      return value
    }
  }
  return undefined
}

export function numberField(
  record: Record<string, unknown> | null | undefined,
  ...keys: string[]
): number | undefined {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'number' && Number.isFinite(value)) {
      return value
    }
    if (typeof value === 'string' && value.trim().length > 0) {
      const parsed = Number(value)
      if (Number.isFinite(parsed)) {
        return parsed
      }
    }
  }
  return undefined
}

export function stringArrayField(
  record: Record<string, unknown> | null | undefined,
  ...keys: string[]
): string[] | undefined {
  if (!record) return undefined
  for (const key of keys) {
    const value = record[key]
    if (!Array.isArray(value)) continue
    const items = value.filter((item): item is string => typeof item === 'string' && item.length > 0)
    if (items.length > 0) {
      return items
    }
  }
  return undefined
}
