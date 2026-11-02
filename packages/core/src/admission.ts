// The one field-validation engine for every TypeScript tool router, the twin of
// internal/admission (Go). It runs the same fixed order the Go engine runs:
// unknown property, missing required property, wire type, array item type,
// enum, then string and integer constraints. The applicability entries and the
// repair sentences come from schemas/, so both languages reject and render
// identical inputs identically.
import repairsBase from '../../../schemas/repairs.json'
import { quote, quoteList } from './text.js'

/** Render one repair sentence for an error code with its pre-quoted facts. */
export type RepairRender = (code: string, facts?: Record<string, string>) => string

export interface ApplicabilityEntry {
  permitted: string[]
  required?: string[]
  types?: Record<string, string>
}

export interface StringConstraints {
  minLength?: number
  maxLength?: number
  pattern?: RegExp
}

export interface IntegerConstraints {
  minimum?: number
  maximum?: number
}

export interface ArrayConstraints {
  itemType?: string
}

export interface Constraints {
  strings: Record<string, StringConstraints>
  integers: Record<string, IntegerConstraints>
  arrays: Record<string, ArrayConstraints>
  enums: Record<string, string[]>
}

/**
 * Merge the shared repair sentences (schemas/repairs.json) with a tool's own
 * codes and overrides — the fixture wins, exactly like the Go generators merge
 * them. The one merge for every TypeScript router: a shared sentence is edited
 * once, and a code missing from both tables renders as '' loudly in the
 * fixture comparisons.
 */
export function mergeRepairs(fixtureRepairs: Record<string, string>): Record<string, string> {
  return {
    ...(repairsBase as { repairs: Record<string, string> }).repairs,
    ...fixtureRepairs,
  }
}

/**
 * Compile the per-property limits a published JSON schema declares, so the
 * router and the schema cannot disagree about length, characters, bounds,
 * enums or array item types.
 */
export function compileConstraints(schema: unknown): Constraints {
  const strings: Record<string, StringConstraints> = {}
  const integers: Record<string, IntegerConstraints> = {}
  const arrays: Record<string, ArrayConstraints> = {}
  const enums: Record<string, string[]> = {}
  const variants = (schema as { oneOf?: Array<{ properties?: Record<string, Record<string, unknown>> }> }).oneOf ?? []
  for (const variant of variants) {
    for (const [name, property] of Object.entries(variant.properties ?? {})) {
      if (Array.isArray(property.enum)) {
        enums[name] = property.enum.filter((value): value is string => typeof value === 'string')
      }
      if (property.type === 'array') {
        const items = property.items as { type?: unknown } | undefined
        arrays[name] = { itemType: typeof items?.type === 'string' ? items.type : undefined }
      }
      if (property.type === 'string' && (typeof property.minLength === 'number' || typeof property.maxLength === 'number' || typeof property.pattern === 'string')) {
        strings[name] = {
          minLength: typeof property.minLength === 'number' ? property.minLength : undefined,
          maxLength: typeof property.maxLength === 'number' ? property.maxLength : undefined,
          pattern: typeof property.pattern === 'string' ? new RegExp(property.pattern, 'u') : undefined,
        }
      }
      if (property.type === 'integer' && (typeof property.minimum === 'number' || typeof property.maximum === 'number')) {
        integers[name] = {
          minimum: typeof property.minimum === 'number' ? property.minimum : undefined,
          maximum: typeof property.maximum === 'number' ? property.maximum : undefined,
        }
      }
    }
  }
  return { strings, integers, arrays, enums }
}

/**
 * The wire types a property can declare. An integer means a mathematical
 * integer, matching the JSON Schema: 1.0, 1e3 and values beyond int64 are
 * integers; 1.5 and "1" are not. A value outside int64 is refused by the
 * constraint checks, which name the bound. A `json` property accepts any JSON
 * value.
 */
export function hasWireType(value: unknown, kind: string): boolean {
  switch (kind) {
    case 'json':
      return true
    case 'string':
      return typeof value === 'string'
    case 'integer':
      // Go's wire type is any mathematical integer. A JSON numeral too large
      // for a JS number parses to ±Infinity, which is still a mathematical
      // integer; the constraint check below refuses it by naming the bound.
      return (
        typeof value === 'number' &&
        (Number.isInteger(value) || value === Infinity || value === -Infinity)
      )
    case 'object':
      return value !== null && typeof value === 'object' && !Array.isArray(value)
    case 'array':
      return Array.isArray(value)
    default:
      return false
  }
}

function wrongTypeRepair(repair: RepairRender, kind: string, name: string): string {
  switch (kind) {
    case 'integer':
      return repair('wrong_type_integer', { name: quote(name) })
    case 'object':
      return repair('wrong_type_object', { name: quote(name) })
    case 'array':
      return repair('wrong_type_array', { name: quote(name) })
    default:
      return repair('wrong_type_string', { name: quote(name) })
  }
}

/**
 * Validate one call's fields against its action's applicability entry, with the
 * limits the published schema declares. Returns the invalid_request detail
 * sentence, or null when the input is admitted.
 */
export function validateFields(
  fields: Record<string, unknown>,
  entry: ApplicabilityEntry,
  constraints: Constraints,
  repair: RepairRender,
): string | null {
  const permitted = new Set(entry.permitted)
  for (const name of Object.keys(fields).sort()) {
    if (!permitted.has(name)) return repair('unknown_property', { name: quote(name) })
  }
  for (const name of entry.required ?? []) {
    if (!(name in fields)) return repair('missing_required_property', { name: quote(name) })
  }
  for (const name of entry.permitted) {
    if (!(name in fields)) continue
    const value = fields[name]
    const kind = entry.types?.[name] ?? 'string'
    if (!hasWireType(value, kind)) return wrongTypeRepair(repair, kind, name)
    if (kind === 'array') {
      const constraint = constraints.arrays[name]
      if (constraint?.itemType) {
        for (const item of value as unknown[]) {
          if (!hasWireType(item, constraint.itemType)) {
            return repair('array_item_type', { name: quote(name) })
          }
        }
      }
    }
    const enumValues = constraints.enums[name]
    if (enumValues && !enumValues.includes(value as string)) {
      return repair('invalid_enum', { name: quote(name), values: quoteList(enumValues) })
    }
    if (kind === 'string') {
      const constraint = constraints.strings[name]
      if (!constraint) continue
      const length = Array.from(value as string).length
      if (constraint.minLength !== undefined && length < constraint.minLength) {
        return repair('string_too_short', { name: quote(name), min: String(constraint.minLength) })
      }
      if (constraint.maxLength !== undefined && length > constraint.maxLength) {
        return repair('string_too_long', { name: quote(name), max: String(constraint.maxLength) })
      }
      if (constraint.pattern && !constraint.pattern.test(value as string)) {
        return repair('string_pattern', { name: quote(name) })
      }
    }
    if (kind === 'integer') {
      const constraint = constraints.integers[name]
      // A value at or beyond the int64 wire range names the declared bound, or
      // the int64 limit where the schema declares none — the sentence the Go
      // engine renders for the same input. JSON.parse rounds max int64 up to
      // 2**63, so the engine cannot distinguish the largest valid int64 from
      // one past it; matching Go's IsInt64 refusal means refusing both. The
      // negative side stays strict: -2**63 is exactly min int64, which Go
      // admits. A bound at the int64 limit does not survive JSON.parse exactly,
      // so the limit is named literally there.
      const number = value as number
      if (number >= 2 ** 63 || number < -(2 ** 63)) {
        const bound = number >= 2 ** 63 ? 'integer_too_large' : 'integer_too_small'
        const declaredMax = constraint?.maximum
        const declaredMin = constraint?.minimum
        return repair(bound, {
          name: quote(name),
          max: declaredMax !== undefined && Number.isSafeInteger(declaredMax) ? String(declaredMax) : '9223372036854775807',
          min: declaredMin !== undefined && Number.isSafeInteger(declaredMin) ? String(declaredMin) : '-9223372036854775808',
        })
      }
      if (!constraint) continue
      if (constraint.minimum !== undefined && (value as number) < constraint.minimum) {
        return repair('integer_too_small', { name: quote(name), min: String(constraint.minimum) })
      }
      if (constraint.maximum !== undefined && (value as number) > constraint.maximum) {
        return repair('integer_too_large', { name: quote(name), max: String(constraint.maximum) })
      }
    }
  }
  return null
}
