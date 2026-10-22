import { z } from 'zod'

/** The zod internals this schema patches, named so a move is one edit. */
type ZodInternals = {
  run: (payload: { value: unknown }, ctx: unknown) => unknown
  toJSONSchema?: () => unknown
}

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
 *
 * Both behaviours ride on zod internals, so construction checks they are there:
 * a zod release that drops them would otherwise leave this surface publishing
 * the loose object and refusing the router's own sentences per call.
 */
export function routerAdmittedSchema(published: unknown): z.ZodType {
  const schema = z.looseObject({})
  const internals = (schema as unknown as { _zod?: Partial<ZodInternals> })._zod
  if (!internals || typeof internals.run !== 'function') {
    throw new Error(
      'routerAdmittedSchema needs the zod internals `_zod.run` and `_zod.toJSONSchema`; this zod version does not expose them, so the router admission pass-through and the published input schema would stop working',
    )
  }
  const run = internals.run.bind(internals)
  internals.run = (payload, ctx) =>
    run(payload.value === undefined ? { ...payload, value: {} } : payload, ctx)
  internals.toJSONSchema = () => structuredClone(published)
  return schema
}
