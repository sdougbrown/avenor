import { spawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'

/** Start one long-lived child — the per-conversation `avenor feed broker`. */
export function spawnAvenorProcess(argv: string[], env: NodeJS.ProcessEnv): ChildProcess {
  return spawn(argv[0]!, argv.slice(1), { env, stdio: ['ignore', 'pipe', 'pipe'] })
}

/** Run a CLI call, passing optional text through stdin. A killed or failed child has a null code. */
export function runAvenorCommand(
  argv: string[],
  env: NodeJS.ProcessEnv,
  signal?: AbortSignal,
  stdin?: string,
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve({ code: null, stdout: '', stderr: '' })
      return
    }
    const child = spawn(argv[0]!, argv.slice(1), {
      env,
      stdio: [stdin === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    child.stdout?.on('data', chunk => { stdout += chunk })
    child.stderr?.on('data', chunk => { stderr += chunk })
    const timer = setTimeout(() => child.kill(), 30_000)
    const onAbort = () => child.kill()
    signal?.addEventListener('abort', onAbort, { once: true })
    child.on('error', () => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', onAbort)
      resolve({ code: null, stdout, stderr })
    })
    child.on('close', code => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', onAbort)
      resolve({ code, stdout, stderr })
    })
    if (stdin !== undefined) {
      // An early child exit can close the pipe before the write completes.
      child.stdin?.on('error', () => {})
      child.stdin?.end(stdin)
    }
  })
}
