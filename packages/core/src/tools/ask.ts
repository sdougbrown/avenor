// avenor_ask requires broker authentication. This tool works correctly
// when running inside a broker-authenticated context (claude-channel sidecar
// or channeltools MCP server). For Pi/standalone use, broker auth propagation
// is tracked as a follow-up — the control protocol needs new RPC methods.
export interface AskToolArgs {
  toRunId: string
  message: string
  supervisorId?: string
}

export interface AskResult {
  reply: string
  from_run_id: string
}

export function createAskTool(): (args: AskToolArgs) => Promise<AskResult> {
  return async (_args) => {
    throw new Error(
      'avenor_ask requires broker authentication and is not yet available ' +
      'from standalone Pi sessions. Use the claude-channel sidecar or ' +
      'channeltools MCP server which have built-in broker auth.'
    )
  }
}

export const askTool = createAskTool()
