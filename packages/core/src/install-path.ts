import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

const SEMVER_RE = /^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$/

export function getVersion(packageJsonPath?: string): string {
  const envVer = process.env.AVENOR_VERSION
  if (envVer) {
    if (!SEMVER_RE.test(envVer)) {
      throw new Error(`Invalid AVENOR_VERSION: ${envVer}. Must be a semver string like 1.2.3`)
    }
    return envVer
  }
  const pkgVer = process.env.npm_package_version
  if (pkgVer) {
    if (!SEMVER_RE.test(pkgVer)) {
      throw new Error(`Invalid npm_package_version: ${pkgVer}. Must be a semver string like 1.2.3`)
    }
    return pkgVer
  }
  const pkgPath = packageJsonPath ?? new URL('../package.json', import.meta.url)
  const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf-8'))
  const v = pkg.version as string
  if (!SEMVER_RE.test(v)) {
    throw new Error(`Invalid package version: ${v}. Must be a semver string like 1.2.3`)
  }
  return v
}

export function getInstallDir(version: string): string {
  if (process.env.AVENOR_INSTALL_DIR) {
    return process.env.AVENOR_INSTALL_DIR
  }
  return path.join(os.homedir(), '.cache', 'avenor', 'bin', 'avenor', version)
}

export function installerBinaryPath(version = getVersion()): string {
  return path.join(getInstallDir(version), 'avenor')
}
