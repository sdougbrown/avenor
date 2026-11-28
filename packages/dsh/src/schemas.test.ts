import { describe, it, expect } from 'bun:test'
import { readFileSync } from 'node:fs'
import messageCopy from './schemas/message.input.flat.json'
import peersCopy from './schemas/peers.input.flat.json'

/** The repo's schemas/*.flat.json stay the single source; the bundle carries
 * copies so a published package is self-contained. This catches drift. */
function repoSchema(name: string): unknown {
  const url = new URL(`../../../schemas/${name}`, import.meta.url)
  return JSON.parse(readFileSync(url, 'utf8'))
}

describe('bundled schemas', () => {
  it('match the repo source for avenor_message', () => {
    expect(messageCopy).toEqual(repoSchema('message.input.flat.json') as typeof messageCopy)
  })

  it('match the repo source for avenor_peers', () => {
    expect(peersCopy).toEqual(repoSchema('peers.input.flat.json') as typeof peersCopy)
  })
})
