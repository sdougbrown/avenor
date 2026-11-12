import spawnInputSchema from '../../../schemas/spawn.input.json'

/**
 * The spawn tool's input contract, read straight from
 * `schemas/spawn.input.json`. That file is the one place a spawn field's name,
 * type, and description is written: the Go MCP server publishes it from a copy
 * compiled into its binary, and every JavaScript surface builds its field text
 * from here, so a reworded field cannot land on one surface only.
 */

export type SpawnSurface = 'go' | 'mcp' | 'pi' | 'opencode'

type SpawnSchemaProperty = {
  type: string
  description?: string
  enum?: string[]
  items?: { type: string }
  'x-avenor-surfaces'?: string[]
}

const properties = spawnInputSchema.properties as Record<string, SpawnSchemaProperty>

/** Every property the schema declares, sorted, so callers can iterate deterministically. */
export const spawnFieldNames: readonly string[] = Object.keys(properties).sort()

/** Each property's canonical schema shape, keyed by property name. */
export const spawnFieldProperties: Readonly<Record<string, SpawnSchemaProperty>> =
  Object.freeze({ ...properties })

/**
 * One field's declared values, for a surface that publishes the restriction.
 * Throws for a field the schema leaves open, so a surface cannot narrow a field
 * the contract does not.
 */
export function spawnFieldEnum(name: string): readonly string[] {
  const values = properties[name]?.enum
  if (!values || values.length === 0) {
    throw new Error(`spawn field "${name}" declares no enum in schemas/spawn.input.json`)
  }
  return values
}

/** The fields one surface publishes, in schema order. */
export function spawnSurfaceFields(surface: SpawnSurface): string[] {
  return spawnFieldNames.filter((name) =>
    (properties[name]?.['x-avenor-surfaces'] ?? []).includes(surface),
  )
}

/**
 * One field's canonical description, for a surface that publishes it. This is
 * the only description accessor: a missing field throws instead of returning an
 * empty string, so a surface cannot publish a field the schema does not declare.
 */
export function spawnFieldDescription(name: string): string {
  const description = properties[name]?.description
  if (!description) {
    throw new Error(`spawn field "${name}" has no description in schemas/spawn.input.json`)
  }
  return description
}
