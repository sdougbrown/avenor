import { z } from 'zod'

/**
 * The tool input schema for a tool whose admission belongs to the shared
 * router.
 *
 * The MCP SDK parses every `tools/call` argument object against the schema the
 * tool publishes before the handler runs, and answers a failure in its own
 * error shape. The published shape is not what the router admits either: the
 * router refuses a wrong type, a missing field and an unknown action with the
 * contract's own sentence. This schema therefore publishes the supplied JSON
 * Schema (through zod's `toJSONSchema` override) while admitting every argument
 * object unchanged, including the `undefined` a call that omits `arguments`
 * produces, so the router answers first on this surface too.
 */
export function routerAdmittedSchema(published: unknown): z.ZodType {
  const schema = z.looseObject({}) as z.ZodType & {
    _zod: {
      run: (payload: { value: unknown }, ctx: unknown) => unknown
      toJSONSchema?: () => unknown
    }
  }
  const run = schema._zod.run.bind(schema._zod)
  schema._zod.run = (payload, ctx) =>
    run(payload.value === undefined ? { ...payload, value: {} } : payload, ctx)
  schema._zod.toJSONSchema = () => structuredClone(published)
  return schema
}
