const UNITS: Record<string, number> = { s: 1, m: 60, h: 3600 }

/** Parse '30s' / '5m' / '1h' into seconds. */
export function parseDuration(text: string): number {
  if (!text) throw new Error('empty duration')
  const unit = text.slice(-1).toLowerCase()
  const amount = text.slice(0, -1)
  if (!(unit in UNITS) || !/^\d+$/.test(amount)) {
    throw new Error(`invalid duration ${JSON.stringify(text)} (expected e.g. 30s, 5m, 1h)`)
  }
  return Number(amount) * UNITS[unit]
}
