import { describe, expect, it } from 'bun:test'
import { spawnFieldDescription, spawnSurfaceFields } from '@dougbots/avenor-core'

import { spawnToolParameters } from './index'

describe('pi spawn parameters', () => {
  it('publishes exactly the fields the schema assigns to the pi surface', () => {
    expect(Object.keys(spawnToolParameters.properties).sort()).toEqual(spawnSurfaceFields('pi'))
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
