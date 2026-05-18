import { describe, it, expect, mock, beforeEach, afterEach } from 'bun:test'
import * as crypto from 'node:crypto'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  platformMapping,
  getVersion,
  getInstallDir,
  postinstall,
  verifyChecksum,
  parseChecksums,
} from './postinstall.js'

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
    const ver = getVersion(pkgPath)
    expect(ver).toBeString()
    expect(ver).toMatch(/^\d+\.\d+\.\d+/)
  })

  it('AVENOR_VERSION takes priority over npm_package_version', () => {
    process.env.AVENOR_VERSION = '9.9.9-priority'
    process.env.npm_package_version = '8.8.8-lower'
    expect(getVersion()).toBe('9.9.9-priority')
  })

  it('rejects invalid AVENOR_VERSION format', () => {
    process.env.AVENOR_VERSION = '../../../etc/passwd'
    expect(() => getVersion()).toThrow('Invalid AVENOR_VERSION')
  })

  it('rejects invalid npm_package_version format', () => {
    delete process.env.AVENOR_VERSION
    process.env.npm_package_version = 'not-a-version'
    expect(() => getVersion()).toThrow('Invalid npm_package_version')
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

describe('verifyChecksum', () => {
  it('passes when hash matches', () => {
    const content = 'hello world'
    const hash = crypto.createHash('sha256').update(Buffer.from(content)).digest('hex')
    expect(verifyChecksum(Buffer.from(content), hash)).toBe(true)
  })

  it('fails when hash does not match', () => {
    const content = 'hello world'
    const wrongHash = '0000000000000000000000000000000000000000000000000000000000000000'
    expect(verifyChecksum(Buffer.from(content), wrongHash)).toBe(false)
  })
})

describe('parseChecksums', () => {
  const checksums = [
    'abc123def456  avenor_darwin_arm64',
    '7890ghi123  avenor_darwin_amd64',
    '',
  ].join('\n')

  it('finds asset hash', () => {
    expect(parseChecksums(checksums, 'avenor_darwin_arm64')).toBe('abc123def456')
  })

  it('finds second asset hash', () => {
    expect(parseChecksums(checksums, 'avenor_darwin_amd64')).toBe('7890ghi123')
  })

  it('returns null for missing asset', () => {
    expect(parseChecksums(checksums, 'avenor_linux_amd64')).toBeNull()
  })

  it('returns null for empty content', () => {
    expect(parseChecksums('', 'avenor_darwin_arm64')).toBeNull()
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
    fs.writeFileSync(path.join(tmpDir, 'which'), '#!/bin/sh\nexit 1\n', { mode: 0o755 })
    process.env.PATH = tmpDir
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
      fs.writeFileSync(path.join(tmpDir, 'which'), `#!/bin/sh\n[ "$1" = "avenor" ] && echo "${fakeBin}" && exit 0\nexit 1\n`, { mode: 0o755 })
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

  it('downloads and installs binary on success path', async () => {
    delete process.env.AVENOR_BIN
    delete process.env.AVENOR_SKIP_DOWNLOAD
    process.env.AVENOR_VERSION = '0.0.0-test-success'
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-test-success-'))
    process.env.AVENOR_INSTALL_DIR = tmpDir
    const { cleanup } = withCleanPath('avenor-test-success-path-')
    try {
      const mockFetch = mock(async () => {
        return new Response('fake-binary-content', { status: 200 })
      })
      await postinstall(mockFetch, 'darwin', 'arm64')
      const binaryPath = path.join(tmpDir, 'avenor')
      expect(fs.existsSync(binaryPath)).toBe(true)
      const content = fs.readFileSync(binaryPath, 'utf-8')
      expect(content).toBe('fake-binary-content')
      const stat = fs.statSync(binaryPath)
      expect(stat.mode & 0o111).toBeTruthy() // executable bit set
    } finally {
      cleanup()
      try { fs.rmSync(tmpDir, { recursive: true, force: true }) } catch {}
      delete process.env.AVENOR_INSTALL_DIR
    }
  })

  it('passes checksum verification and installs binary', async () => {
    delete process.env.AVENOR_BIN
    delete process.env.AVENOR_SKIP_DOWNLOAD
    process.env.AVENOR_VERSION = '0.0.0-test-checksum-ok'
    const binaryContent = 'fake-binary-for-checksum-test'
    const binaryHash = crypto.createHash('sha256').update(Buffer.from(binaryContent)).digest('hex')
    const checksumsContent = `${binaryHash}  avenor_darwin_arm64\n${'0'.repeat(64)}  avenor_darwin_amd64\n`
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-test-checksum-ok-'))
    process.env.AVENOR_INSTALL_DIR = tmpDir
    const { cleanup } = withCleanPath('avenor-test-checksum-ok-path-')
    try {
      const mockFetch = mock(async (url: string) => {
        if (url.includes('checksums.txt')) {
          return new Response(checksumsContent, { status: 200 })
        }
        return new Response(binaryContent, { status: 200 })
      })
      await postinstall(mockFetch, 'darwin', 'arm64')
      const binaryPath = path.join(tmpDir, 'avenor')
      expect(fs.existsSync(binaryPath)).toBe(true)
      const content = fs.readFileSync(binaryPath, 'utf-8')
      expect(content).toBe(binaryContent)
    } finally {
      cleanup()
      try { fs.rmSync(tmpDir, { recursive: true, force: true }) } catch {}
      delete process.env.AVENOR_INSTALL_DIR
    }
  })

  it('soft-fails on checksum mismatch', async () => {
    delete process.env.AVENOR_BIN
    delete process.env.AVENOR_SKIP_DOWNLOAD
    process.env.AVENOR_VERSION = '0.0.0-test-checksum-mismatch'
    const checksumsContent = `${'0'.repeat(64)}  avenor_darwin_arm64\n`
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-test-checksum-mismatch-'))
    process.env.AVENOR_INSTALL_DIR = tmpDir
    const { cleanup } = withCleanPath('avenor-test-checksum-mismatch-path-')
    try {
      const mockFetch = mock(async (url: string) => {
        if (url.includes('checksums.txt')) {
          return new Response(checksumsContent, { status: 200 })
        }
        return new Response('real-binary-content', { status: 200 })
      })
      await postinstall(mockFetch, 'darwin', 'arm64')
      const binaryPath = path.join(tmpDir, 'avenor')
      expect(fs.existsSync(binaryPath)).toBe(false)
    } finally {
      cleanup()
      try { fs.rmSync(tmpDir, { recursive: true, force: true }) } catch {}
      delete process.env.AVENOR_INSTALL_DIR
    }
  })

  it('continues install when checksums.txt returns 404', async () => {
    delete process.env.AVENOR_BIN
    delete process.env.AVENOR_SKIP_DOWNLOAD
    process.env.AVENOR_VERSION = '0.0.0-test-checksum-missing'
    const binaryContent = 'binary-content-missing-checksums'
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-test-checksum-missing-'))
    process.env.AVENOR_INSTALL_DIR = tmpDir
    const { cleanup } = withCleanPath('avenor-test-checksum-missing-path-')
    try {
      const mockFetch = mock(async (url: string) => {
        if (url.includes('checksums.txt')) {
          return new Response('Not Found', { status: 404 })
        }
        return new Response(binaryContent, { status: 200 })
      })
      await postinstall(mockFetch, 'darwin', 'arm64')
      const binaryPath = path.join(tmpDir, 'avenor')
      expect(fs.existsSync(binaryPath)).toBe(true)
      const content = fs.readFileSync(binaryPath, 'utf-8')
      expect(content).toBe(binaryContent)
    } finally {
      cleanup()
      try { fs.rmSync(tmpDir, { recursive: true, force: true }) } catch {}
      delete process.env.AVENOR_INSTALL_DIR
    }
  })
})
