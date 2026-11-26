import { describe, it, expect } from 'bun:test'
import * as fs from 'node:fs'
import * as path from 'node:path'

/**
 * Install-time lifecycle hooks run during `npm install` / `pnpm add`, before
 * the user has executed anything. A hook here can only do side-effectful
 * setup, and this package must not have any: publishing a package whose
 * `postinstall` downloads a binary makes installation non-hermetic, needs the
 * network, and is refused outright by pnpm's build-script allowlist.
 *
 * The avenor binary is an external prerequisite, resolved at runtime by
 * `findAvenorBinary()` (AVENOR_BIN, then PATH, then an existing install).
 */
const INSTALL_HOOKS = ['preinstall', 'install', 'postinstall'] as const

const manifestPath = path.resolve(
  path.dirname(new URL(import.meta.url).pathname),
  '..',
  'package.json',
)
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8')) as {
  scripts?: Record<string, string>
}

describe('published manifest', () => {
  it('declares no install-time lifecycle scripts', () => {
    const declared = INSTALL_HOOKS.filter((hook) => manifest.scripts?.[hook] !== undefined)
    expect(declared).toEqual([])
  })
})
