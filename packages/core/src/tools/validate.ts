export function validateRunId(id: string): void {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`invalid run_id: ${id}`)
  }
}
