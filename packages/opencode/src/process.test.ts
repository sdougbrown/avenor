import { afterEach, describe, expect, it } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

/**
 * `runAvenorCommand` is the only place the plugin hands text to a child
 * process, so its stdin contract is proved here against real children: a stub
 * can show which argument was built, but only a real child shows which bytes
 * reached the pipe.
 *
 * The specifier carries a query no stub can answer. bun keys module mocks by
 * resolved path and keeps one registry for a whole run, and another test file
 * stubs `./process.js` for the plugin, so the query string is what reaches the
 * real implementation from the same run.
 */
const realSpecifier = './process.js?real'
const { runAvenorCommand } = await import(realSpecifier) as typeof import('./process.js')

const temporaries: string[] = []

function temporaryDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-opencode-process-'))
  temporaries.push(dir)
  return dir
}

afterEach(() => {
  for (const dir of temporaries.splice(0)) fs.rmSync(dir, { recursive: true, force: true })
})

describe('runAvenorCommand stdin', () => {
  it('delivers the given text byte for byte, adding no newline', async () => {
    const text = 'naïve ☃ — line one\nline two\n\n'
    const result = await runAvenorCommand(['/bin/cat'], {}, undefined, text)

    expect(result).toEqual({ code: 0, stdout: text, stderr: '' })
    // The trailing newlines are the caller's, not an artifact of the write.
    expect(result.stdout).not.toBe(`${text}\n`)
    expect(Buffer.byteLength(result.stdout, 'utf8')).toBe(Buffer.byteLength(text, 'utf8'))
  })

  it('closes stdin for empty text instead of skipping the write', async () => {
    // An empty message is still a message: the child sees EOF and no bytes.
    expect(await runAvenorCommand(['/bin/cat'], {}, undefined, '')).toEqual({ code: 0, stdout: '', stderr: '' })
  })

  it('leaves stdin untouched when no text is given', async () => {
    // The long-lived feed children take no stdin; nothing is written into
    // theirs.
    expect(await runAvenorCommand(['/bin/cat'], {})).toEqual({ code: 0, stdout: '', stderr: '' })
  })

  it('reports the exit instead of crashing when the child dies unread', async () => {
    // A child that exits before draining its pipe breaks the write with EPIPE;
    // the exit it already reported is the call's one answer, never an unhandled
    // stream error and never a second result.
    const result = await runAvenorCommand(['/bin/sh', '-c', 'exit 3'], {}, undefined, 'x'.repeat(1 << 20))

    expect(result).toEqual({ code: 3, stdout: '', stderr: '' })
  })

  it('reports an unstartable child as uncertainty rather than absence', async () => {
    // A null code is the caller's uncertainty signal: the text may have been
    // sent, so it is never reported as not-sent.
    expect(await runAvenorCommand(['/nonexistent/avenor'], {}, undefined, 'text')).toEqual({
      code: null,
      stdout: '',
      stderr: '',
    })
  })

  it('starts no child when the signal is already aborted', async () => {
    const marker = path.join(temporaryDir(), 'ran')
    const controller = new AbortController()
    controller.abort()

    const result = await runAvenorCommand(['/bin/sh', '-c', `echo ran > ${marker}`], {}, controller.signal, 'text')
    await new Promise(resolve => setTimeout(resolve, 50))

    expect(result).toEqual({ code: null, stdout: '', stderr: '' })
    expect(fs.existsSync(marker)).toBe(false)
  })

  it('kills a child that outlives its abort and reports the uncertainty', async () => {
    const controller = new AbortController()
    const pending = runAvenorCommand(['/bin/sh', '-c', 'sleep 5'], {}, controller.signal, 'text')
    setTimeout(() => controller.abort(), 50)

    expect((await pending).code).toBeNull()
  })
})
