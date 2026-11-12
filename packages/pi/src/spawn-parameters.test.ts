import { describe, expect, it } from 'bun:test'
import {
  THINKING_LEVELS,
  spawnFieldDescription,
  spawnFieldEnum,
  spawnFieldProperties,
  spawnSurfaceFields,
} from '@dougbots/avenor-core'

import { spawnToolParameters } from './index'

describe('pi spawn parameters', () => {
  it('publishes exactly the fields the schema assigns to the pi surface', () => {
    expect(Object.keys(spawnToolParameters.properties).sort()).toEqual(spawnSurfaceFields('pi'))
  })

  it('publishes the type the schema declares for every field', () => {
    const properties = spawnToolParameters.properties as unknown as Record<
      string,
      { type?: string; anyOf?: { const: string }[] }
    >
    for (const [name, published] of Object.entries(properties)) {
      const declared = spawnFieldProperties[name]?.type
      if (name === 'backend') {
        expect(published.anyOf?.map((literal) => literal.const)).toEqual([...spawnFieldEnum('backend')])
        continue
      }
      if (published.anyOf) {
        // A union of literals carries no top-level type; the thinking levels are
        // the only such field, and the union is the enum the schema describes.
        expect(name).toBe('thinking')
        expect(published.anyOf.map((literal) => literal.const)).toEqual([...THINKING_LEVELS])
        continue
      }
      expect(published.type ?? '').toBe(declared ?? '')
    }
  })

  it('takes every description from the shared spawn schema', () => {
    // TypeBox keeps annotations on the schema object but does not type
    // `description`, so read it through its published shape.
    const properties = spawnToolParameters.properties as unknown as Record<string, { description?: string }>
    for (const [name, property] of Object.entries(properties)) {
      expect(property.description ?? '').toBe(spawnFieldDescription(name))
    }
  })
})
