import { describe, it, expect, mock, beforeEach, afterEach } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { platformMapping, getVersion, getInstallDir, postinstall } from './postinstall.js'

describe('platformMapping', () => {
  it('maps darwin/arm64 to avenor_darwin_arm64', () => {
    expect(platformMapping('darwin', 'arm64')).toBe('avenor_darwin_arm64')
  })

  it('maps darwin/x64 to avenor_darwin_amd64', () => {
    expect(platformMapping('darwin', 'x64')).toBe('avenor_darwin_amd64')
  })

  it('maps linux/x64 to avenor_linux_amd64', () => {
    expect(platformMapping('linux', 'x64')).toBe('avenor_linux_amd64')
  })

  it('maps linux/arm64 to avenor_linux_arm64', () => {
    expect(platformMapping('linux', 'arm64')).toBe('avenor_linux_arm64')
  })

  it('returns null for unsupported platform/arch (win32/x64)', () => {
    expect(platformMapping('win32', 'x64')).toBeNull()
  })

  it('returns null for unsupported platform/arch (darwin/ia32)', () => {
    expect(platformMapping('darwin', 'ia32')).toBeNull()
  })

  it('returns null for unsupported platform/arch (linux/ia32)', () => {
    expect(platformMapping('linux', 'ia32')).toBeNull()
  })
})

describe('getVersion', () => {
  const prevVersion = process.env.AVENOR_VERSION
  const prevPkgVer = process.env.npm_package_version

  afterEach(() => {
    if (prevVersion !== undefined) {
      process.env.AVENOR_VERSION = prevVersion
    } else {
      delete process.env.AVENOR_VERSION
    }
    if (prevPkgVer !== undefined) {
      process.env.npm_package_version = prevPkgVer
    } else {
      delete process.env.npm_package_version
    }
  })

  it('uses AVENOR_VERSION env var when set', () => {
    process.env.AVENOR_VERSION = '1.2.3-test'
    delete process.env.npm_package_version
    expect(getVersion()).toBe('1.2.3-test')
  })

  it('falls back to npm_package_version when AVENOR_VERSION is unset', () => {
    delete process.env.AVENOR_VERSION
    process.env.npm_package_version = '4.5.6-npm'
    expect(getVersion()).toBe('4.5.6-npm')
  })

  it('reads from package.json when both env vars are unset', () => {
    delete process.env.AVENOR_VERSION
    delete process.env.npm_package_version
    const pkgPath = path.resolve(
      path.dirname(new URL(import.meta.url).pathname),
      '..',
      'package.json',
    )
    expect(getVersion()).not.toBe('')
    expect(getVersion()).toBeString()
  })

  it('accepts explicit packageJsonPath', () => {
    delete process.env.AVENOR_VERSION
    delete process.env.npm_package_version
    const pkgPath = path.resolve(
      path.dirname(new URL(import.meta.url).pathname),
      '..',
      'package.json',
    )
    expect(getVersion(pkgPath)).toBeString()
  })

  it('AVENOR_VERSION takes priority over npm_package_version', () => {
    process.env.AVENOR_VERSION = '9.9.9-priority'
    process.env.npm_package_version = '8.8.8-lower'
    expect(getVersion()).toBe('9.9.9-priority')
  })
})

describe('getInstallDir', () => {
  const prevInstallDir = process.env.AVENOR_INSTALL_DIR

  afterEach(() => {
    if (prevInstallDir !== undefined) {
      process.env.AVENOR_INSTALL_DIR = prevInstallDir
    } else {
      delete process.env.AVENOR_INSTALL_DIR
    }
  })

  it('returns AVENOR_INSTALL_DIR when set', () => {
    process.env.AVENOR_INSTALL_DIR = '/opt/avenor/custom'
    expect(getInstallDir('1.0.0')).toBe('/opt/avenor/custom')
  })

  it('returns versioned default when AVENOR_INSTALL_DIR is unset', () => {
    delete process.env.AVENOR_INSTALL_DIR
    const expected = path.join(os.homedir(), '.cache', 'avenor', 'bin', 'avenor', '2.0.0')
    expect(getInstallDir('2.0.0')).toBe(expected)
  })
})

describe('postinstall', () => {
  const prevBin = process.env.AVENOR_BIN
  const prevSkip = process.env.AVENOR_SKIP_DOWNLOAD
  const prevVersion = process.env.AVENOR_VERSION
  const prevPkgVer = process.env.npm_package_version
  const prevInstallDir = process.env.AVENOR_INSTALL_DIR
  const prevPath = process.env.PATH

  afterEach(() => {
    if (prevBin !== undefined) process.env.AVENOR_BIN = prevBin
    else delete process.env.AVENOR_BIN
    if (prevSkip !== undefined) process.env.AVENOR_SKIP_DOWNLOAD = prevSkip
    else delete process.env.AVENOR_SKIP_DOWNLOAD
    if (prevVersion !== undefined) process.env.AVENOR_VERSION = prevVersion
    else delete process.env.AVENOR_VERSION
    if (prevPkgVer !== undefined) process.env.npm_package_version = prevPkgVer
    else delete process.env.npm_package_version
    if (prevInstallDir !== undefined) process.env.AVENOR_INSTALL_DIR = prevInstallDir
    else delete process.env.AVENOR_INSTALL_DIR
    if (prevPath !== undefined) process.env.PATH = prevPath
    else delete process.env.PATH
  })

  function withCleanPath(pathSuffix: string): { tmpDir: string; cleanup: () => void } {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), pathSuffix))
    process.env.PATH = `${tmpDir}:${prevPath || '/usr/bin:/bin'}`
    return {
      tmpDir,
      cleanup: () => {
        try { fs.rmSync(tmpDir, { recursive: true, force: true }) } catch {}
      },
    }
  }

  it('returns early when AVENOR_BIN is set and executable', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-test-bin-'))
    try {
      const fakeBin = path.join(tmpDir, 'avenor')
      fs.writeFileSync(fakeBin, '#!/bin/sh\nexit 0', { mode: 0o755 })
      process.env.AVENOR_BIN = fakeBin

      const mockFetch = mock(async () => new Response('data'))
      await postinstall(mockFetch)
      expect(mockFetch).not.toHaveBeenCalled()
    } finally {
      try { fs.rmSync(tmpDir, { recursive: true, force: true }) } catch {}
    }
  })

  it('returns early when avenor is on PATH', async () => {
    delete process.env.AVENOR_BIN
    const { tmpDir, cleanup } = withCleanPath('avenor-test-path-')
    try {
      const fakeBin = path.join(tmpDir, 'avenor')
      fs.writeFileSync(fakeBin, '#!/bin/sh\nexit 0', { mode: 0o755 })
      const mockFetch = mock(async () => new Response('data'))
      await postinstall(mockFetch)
      expect(mockFetch).not.toHaveBeenCalled()
    } finally {
      cleanup()
    }
  })

  it('skips download when AVENOR_SKIP_DOWNLOAD=1', async () => {
    delete process.env.AVENOR_BIN
    process.env.AVENOR_SKIP_DOWNLOAD = '1'
    const { cleanup } = withCleanPath('avenor-test-skip-path-')
    try {
      const mockFetch = mock(async () => new Response('data'))
      await postinstall(mockFetch)
      expect(mockFetch).not.toHaveBeenCalled()
    } finally {
      cleanup()
    }
  })

  it('soft-fails on download error', async () => {
    delete process.env.AVENOR_BIN
    delete process.env.AVENOR_SKIP_DOWNLOAD
    process.env.AVENOR_VERSION = '0.0.0-test-dl-error'
    const { cleanup } = withCleanPath('avenor-test-dl-path-')
    try {
      const mockFetch = mock(async () => {
        throw new Error('network failure')
      })
      await postinstall(mockFetch)
      expect(mockFetch).toHaveBeenCalled()
    } finally {
      cleanup()
    }
  })

  it('soft-fails on HTTP error response', async () => {
    delete process.env.AVENOR_BIN
    delete process.env.AVENOR_SKIP_DOWNLOAD
    process.env.AVENOR_VERSION = '0.0.0-test-http'
    const { cleanup } = withCleanPath('avenor-test-http-path-')
    try {
      const mockFetch = mock(async () => {
        return new Response('Not Found', { status: 404 })
      })
      await postinstall(mockFetch)
      expect(mockFetch).toHaveBeenCalled()
    } finally {
      cleanup()
    }
  })

  it('soft-fails on unsupported platform', async () => {
    delete process.env.AVENOR_BIN
    delete process.env.AVENOR_SKIP_DOWNLOAD
    const { cleanup } = withCleanPath('avenor-test-unsup-path-')
    try {
      const mockFetch = mock(async () => new Response('data'))
      await postinstall(mockFetch, 'win32', 'x64')
      expect(mockFetch).not.toHaveBeenCalled()
    } finally {
      cleanup()
    }
  })
})
