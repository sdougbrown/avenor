export function validateRunId(id: string): void {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`invalid run_id: ${id}`)
  }
}

export function validateTimeout(value: string): number {
  const trimmed = value.trim()
  const match = /^(\d+)([smh]?)$/.exec(trimmed)
  if (!match) {
    throw new Error(`invalid timeout: ${value}`)
  }
  const amount = Number(match[1])
  const unit = match[2]
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error(`invalid timeout: ${value}`)
  }
  if (unit === 'm') return amount * 60
  if (unit === 'h') return amount * 3600
  return amount
}

const DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0
}

export function validateObservedAt(observedAt: string): void {
  // RFC3339: a calendar date and time with a timezone. The regex pins the shape
  // and the component checks reject an impossible date, matching what the Go
  // router's time.Parse(time.RFC3339) rejects.
  // Go's RFC3339 parser accepts either "." or "," before the fractional second.
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:[.,]\d+)?(?:Z|[+-](\d{2}):(\d{2}))$/.exec(observedAt)
  if (!match) {
    throw new Error('observed_at must be an RFC3339 timestamp')
  }
  const [year, month, day, hour, minute, second] = match.slice(1, 7).map(Number)
  if (month < 1 || month > 12) {
    throw new Error('observed_at must be an RFC3339 timestamp')
  }
  const maxDay = month === 2 && isLeapYear(year) ? 29 : DAYS_IN_MONTH[month - 1]
  if (day < 1 || day > maxDay || hour > 23 || minute > 59 || second > 59) {
    throw new Error('observed_at must be an RFC3339 timestamp')
  }
  // Go's parser deliberately accepts an offset of 24 hours or 60 minutes
  // ("some people do write" them) and rejects anything beyond.
  if (match[7] !== undefined) {
    const offsetHour = Number(match[7])
    const offsetMinute = Number(match[8])
    if (offsetHour > 24 || offsetMinute > 60) {
      throw new Error('observed_at must be an RFC3339 timestamp')
    }
  }
}
