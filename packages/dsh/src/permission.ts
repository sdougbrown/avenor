/** A permission request an Avenor run is blocked on, normalized for DSH. */
export interface PendingPermission {
  readonly requestId: string
  readonly description: string
  readonly options: readonly PermissionOption[]
}

export interface PermissionOption {
  readonly optionId: string
  readonly label: string
  readonly kind: string
}

/** Raw shape core's `resultTool` reports under `pending_permission`. */
export interface RawPendingPermission {
  readonly request_id?: string
  readonly description?: string
  readonly options?: readonly {
    readonly option_id?: string
    readonly label?: string
    readonly kind?: string
  }[]
}

export function toPendingPermission(raw: RawPendingPermission): PendingPermission {
  return {
    requestId: raw.request_id ?? '',
    description: raw.description ?? '',
    options: (raw.options ?? []).map((option) => ({
      optionId: option.option_id ?? '',
      label: option.label ?? '',
      kind: option.kind ?? '',
    })),
  }
}

/**
 * The message a blocked run sends to the agent that spawned it. The orchestrator
 * answers it; the person looking at the harness is not asked.
 */
export function permissionNotice(runId: string, permission: PendingPermission): string {
  const options = permission.options.length
    ? permission.options.map((option) => `  - ${option.optionId} (${option.label || option.kind})`).join('\n')
    : '  (the run did not report its options)'
  const description = permission.description || '(no description)'
  return [
    `An Avenor run you delegated is blocked on a permission decision.`,
    '',
    `run_id: ${runId}`,
    `request: ${permission.requestId || '(unspecified)'}`,
    `question: ${description}`,
    '',
    `options:`,
    options,
    '',
    `Answer with the avenor_answer_permission tool, passing run_id and one option_id.`,
  ].join('\n')
}
