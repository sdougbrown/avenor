import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { execSync } from 'node:child_process'

export function platformMapping(platform: string, arch: string): string | null {
  const key = `${platform}/${arch}`
  const map: Record<string, string> = {
    'darwin/arm64': 'avenor_darwin_arm64',
    'darwin/x64': 'avenor_darwin_amd64',
    'linux/x64': 'avenor_linux_amd64',
    'linux/arm64': 'avenor_linux_arm64',
  }
  return map[key] ?? null
}

export function getVersion(packageJsonPath?: string): string {
  const envVer = process.env.AVENOR_VERSION
  if (envVer) return envVer
  const pkgVer = process.env.npm_package_version
  if (pkgVer) return pkgVer
  const pkgPath = packageJsonPath ?? new URL('../package.json', import.meta.url)
  const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf-8'))
  return pkg.version as string
}

export function getInstallDir(version: string): string {
  if (process.env.AVENOR_INSTALL_DIR) {
    return process.env.AVENOR_INSTALL_DIR
  }
  return path.join(os.homedir(), '.cache', 'avenor', 'bin', 'avenor', version)
}

export async function postinstall(
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
  platform?: string,
  arch?: string,
): Promise<void> {
  const envBin = process.env.AVENOR_BIN
  if (envBin) {
    try {
      fs.accessSync(envBin, fs.constants.X_OK)
      return
    } catch {
      // not executable, continue
    }
  }

  try {
    const whichBin = execSync('which avenor', { encoding: 'utf-8', env: process.env }).trim()
    if (whichBin) {
      try {
        fs.accessSync(whichBin, fs.constants.X_OK)
        return
      } catch {
        // not executable, continue
      }
    }
  } catch {
    // which failed or returned empty
  }

  if (process.env.AVENOR_SKIP_DOWNLOAD === '1') {
    console.warn(
      '[avenor-postinstall] AVENOR_SKIP_DOWNLOAD=1, skipping download. Binary may not be available.',
    )
    return
  }

  const plat = platform ?? process.platform
  const a = arch ?? process.arch
  const asset = platformMapping(plat, a)
  if (!asset) {
    console.warn(
      `[avenor-postinstall] Unsupported platform: ${plat}/${a}`,
    )
    return
  }

  const version = getVersion()
  const installDir = getInstallDir(version)
  const binaryPath = path.join(installDir, 'avenor')
  const url = `https://github.com/sdougbrown/avenor/releases/download/v${version}/${asset}`

  try {
    fs.mkdirSync(installDir, { recursive: true, mode: 0o700 })
    const response = await fetchFn(url)
    if (!response.ok) {
      console.warn(
        `[avenor-postinstall] Download failed: HTTP ${response.status} from ${url}`,
      )
      return
    }
    const buffer = Buffer.from(await response.arrayBuffer())
    fs.writeFileSync(binaryPath, buffer)
    fs.chmodSync(binaryPath, 0o755)
  } catch (err) {
    console.warn(
      `[avenor-postinstall] Failed to install avenor binary: ${err instanceof Error ? err.message : String(err)}`,
    )
  }
}

const arg1 = process.argv[1]
const isMain =
  !!arg1 &&
  (arg1.endsWith('postinstall.js') || arg1.endsWith('postinstall.ts'))
if (isMain) {
  postinstall().catch((err) => {
    console.warn(
      `[avenor-postinstall] Unhandled error: ${err instanceof Error ? err.message : String(err)}`,
    )
    process.exit(0)
  })
}
