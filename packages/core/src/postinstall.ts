import * as crypto from 'node:crypto'
import * as fs from 'node:fs'
import { execSync } from 'node:child_process'
import { getInstallDir, getVersion, installerBinaryPath } from './install-path.js'

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

export function verifyChecksum(buffer: Buffer, expectedHash: string): boolean {
  const actual = crypto.createHash('sha256').update(buffer).digest('hex')
  return actual === expectedHash
}

export function parseChecksums(content: string, assetName: string): string | null {
  for (const line of content.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) continue
    const parts = trimmed.split(/\s+/)
    if (parts.length >= 2 && parts[1] === assetName) {
      return parts[0]
    }
  }
  return null
}

export { getInstallDir, getVersion, installerBinaryPath }

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
  const binaryPath = installerBinaryPath(version)
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

    const checksumsUrl = `https://github.com/sdougbrown/avenor/releases/download/v${version}/checksums.txt`
    try {
      const checksumsResponse = await fetchFn(checksumsUrl)
      if (checksumsResponse.ok) {
        const checksumsContent = await checksumsResponse.text()
        const expectedHash = parseChecksums(checksumsContent, asset)
        if (expectedHash) {
          if (!verifyChecksum(buffer, expectedHash)) {
            console.warn(
              `[avenor-postinstall] Checksum mismatch for ${asset}, skipping installation`,
            )
            return
          }
        } else {
          console.warn(
            `[avenor-postinstall] Checksums not available for ${asset}, skipping verification`,
          )
        }
      } else {
        console.warn(
          `[avenor-postinstall] Checksums not available (HTTP ${checksumsResponse.status}), skipping verification`,
        )
      }
    } catch {
      console.warn(
        `[avenor-postinstall] Checksums not available, skipping verification`,
      )
    }

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
