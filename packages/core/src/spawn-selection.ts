import { umpire } from '@umpire/core'
import { fromJson, type UmpireJsonSchema } from '@umpire/json'
import spawnSelectionSchema from '../../../schemas/spawn_selection.umpire.json'

export interface SpawnSelectionInput {
  agent?: string
  model?: string
  backend?: string
  roster_file?: string
  roster_entry?: string
}

const selectorFields = ['agent', 'model', 'backend', 'roster_file', 'roster_entry'] as const
const selectorFieldSet = new Set<string>(selectorFields)

const parsedSchema = fromJson(spawnSelectionSchema as UmpireJsonSchema)
const spawnSelectionUmpire = umpire({
  fields: parsedSchema.fields,
  rules: parsedSchema.rules,
})

type Availability = {
  enabled: boolean
  fair: boolean
  reason: string | null
}

function assertInput(input: unknown): asserts input is SpawnSelectionInput {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw new Error('invalid spawn selector: expected an object')
  }

  for (const key of Object.keys(input)) {
    if (!selectorFieldSet.has(key)) {
      throw new Error(`invalid spawn selector: unknown field "${key}"`)
    }
    const value = (input as Record<string, unknown>)[key]
    if (value !== undefined && typeof value !== 'string') {
      throw new Error(`invalid spawn selector: ${key} must be a string`)
    }
  }
}

/** Validate the direct-versus-roster selector contract. */
export function validateSpawnSelection(
  input: unknown,
  rosterConfigured = false,
): void {
  assertInput(input)
  if (typeof rosterConfigured !== 'boolean') {
    throw new Error('invalid spawn selector: rosterConfigured must be a boolean')
  }

  const values: Record<string, string> = {}
  for (const field of selectorFields) {
    const value = input[field]
    if (value !== undefined && value !== '') {
      values[field] = value
    }
  }

  const availability = spawnSelectionUmpire.check(values, { rosterConfigured }) as Record<string, Availability>
  for (const field of selectorFields) {
    const value = values[field]
    const status = availability[field]
    if (value === undefined || (status?.enabled && status.fair)) continue
    throw new Error(
      `invalid spawn selector: ${status?.reason ?? `${field} is disabled`}`,
    )
  }
}
