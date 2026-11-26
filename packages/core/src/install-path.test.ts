import { describe, it, expect, afterEach } from 'bun:test'
import * as os from 'node:os'
import * as path from 'node:path'
import { getVersion, getInstallDir, installerBinaryPath } from './install-path.js'

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

describe('installerBinaryPath', () => {
  const prevInstallDir = process.env.AVENOR_INSTALL_DIR

  afterEach(() => {
    if (prevInstallDir !== undefined) {
      process.env.AVENOR_INSTALL_DIR = prevInstallDir
    } else {
      delete process.env.AVENOR_INSTALL_DIR
    }
  })

  it('joins the resolved install directory with the binary name', () => {
    process.env.AVENOR_INSTALL_DIR = '/opt/avenor/custom'
    expect(installerBinaryPath('3.0.0')).toBe(path.join('/opt/avenor/custom', 'avenor'))
  })
})
