import { describe, it, expect } from 'bun:test'
import { z } from 'zod'
import { routerAdmittedSchema } from './router-schema'
import messageFlatContract from '../../../schemas/message.input.flat.json'
import peersFlatContract from '../../../schemas/peers.input.flat.json'

/*
 * A drift canary for the two zod internals `routerAdmittedSchema` patches. It
 * goes through the public entry point the MCP SDK itself publishes with (its
 * `zod-json-schema-compat` calls `z4mini.toJSONSchema(schema, …)`) instead of
 * the patched internals, so it fails the day zod stops consulting either
 * override rather than shipping a schema the SDK ignores.
 */
const contracts = [
  ['message', messageFlatContract],
  ['peers', peersFlatContract],
] as const

describe('routerAdmittedSchema', () => {
  for (const [name, published] of contracts) {
    it(`publishes the ${name} flat contract through zod's public toJSONSchema`, () => {
      const schema = routerAdmittedSchema(published)
      expect(z.toJSONSchema(schema, { target: 'draft-7', io: 'input' })).toEqual(published)
    })

    it(`admits omitted and null arguments and leaves ${name} admission to the router`, () => {
      const schema = routerAdmittedSchema(published)
      expect(schema.safeParse(undefined).success).toBe(true)
      expect(schema.safeParse(null).success).toBe(true)
      expect(schema.safeParse({ action: 5 }).success).toBe(true)
    })
  }
})
