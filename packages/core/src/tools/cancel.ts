// avenor_cancel requires broker authentication. Same constraint as ask.ts.
export interface CancelToolArgs {
  messageId: string
  supervisorId?: string
}

export interface CancelResult {
  cancelled: boolean
}

export function createCancelTool(): (args: CancelToolArgs) => Promise<CancelResult> {
  return async (_args) => {
    throw new Error(
      'avenor_cancel requires broker authentication and is not yet available ' +
      'from standalone Pi sessions.'
    )
  }
}

export const cancelTool = createCancelTool()
