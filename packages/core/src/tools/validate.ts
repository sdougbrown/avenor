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

export function validateObservedAt(observedAt: string): void {
  // RFC3339 pattern that matches ISO 8601 dates with timezone
  const rfc3339Regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;
  if (!rfc3339Regex.test(observedAt)) {
    throw new Error('observed_at must be an RFC3339 timestamp')
  }
}
