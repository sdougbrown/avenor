import { describe, expect, test } from 'bun:test'
import { escapedLinkUrl, splitEscapes } from './escaped-links'

describe('splitEscapes', () => {
  test('counts leading ../ segments', () => {
    expect(splitEscapes('../templates/x')).toEqual({ depth: 1, path: 'templates/x' })
    expect(splitEscapes('../../a/b')).toEqual({ depth: 2, path: 'a/b' })
  })

  test('counts a bare trailing .. as an escape level', () => {
    expect(splitEscapes('../..')).toEqual({ depth: 2, path: '' })
  })

  test('strips trailing slashes from the remainder', () => {
    expect(splitEscapes('../templates/foo/')).toEqual({ depth: 1, path: 'templates/foo' })
  })
})

describe('escapedLinkUrl', () => {
  test('returns null for links inside docs/', () => {
    expect(escapedLinkUrl('page.md', './other.md')).toBeNull()
    expect(escapedLinkUrl('sub/page.md', '../other.md')).toBeNull()
  })

  test('rewrites escaped directories to /tree/', () => {
    expect(escapedLinkUrl('page.md', '../templates/foo')).toBe(
      'https://github.com/sdougbrown/avenor/tree/main/templates/foo',
    )
  })

  test('rewrites escaped files to /blob/', () => {
    expect(escapedLinkUrl('page.md', '../internal/x/gate_handlers.go')).toBe(
      'https://github.com/sdougbrown/avenor/blob/main/internal/x/gate_handlers.go',
    )
  })

  test('classifies extension-less files via the filesystem, not extname', () => {
    expect(escapedLinkUrl('page.md', '../LICENSE')).toBe(
      'https://github.com/sdougbrown/avenor/blob/main/LICENSE',
    )
  })

  test('falls back to the extname heuristic for paths missing from the repo', () => {
    expect(escapedLinkUrl('page.md', '../templates/not-a-real-dir')).toBe(
      'https://github.com/sdougbrown/avenor/tree/main/templates/not-a-real-dir',
    )
    expect(escapedLinkUrl('page.md', '../templates/not-a-real-file.txt')).toBe(
      'https://github.com/sdougbrown/avenor/blob/main/templates/not-a-real-file.txt',
    )
  })

  test('strips anchors before classifying and re-appends them', () => {
    expect(escapedLinkUrl('page.md', '../templates/escalation-bridge#readme')).toBe(
      'https://github.com/sdougbrown/avenor/tree/main/templates/escalation-bridge#readme',
    )
    expect(escapedLinkUrl('page.md', '../#readme')).toBe(
      'https://github.com/sdougbrown/avenor/tree/main#readme',
    )
  })

  test('a fragment does not hide escape levels from the depth guard', () => {
    expect(() => escapedLinkUrl('page.md', '../..#readme')).toThrow(/escapes the repository/)
  })

  test('resolves against the page directory', () => {
    expect(escapedLinkUrl('sub/page.md', '../../templates/foo')).toBe(
      'https://github.com/sdougbrown/avenor/tree/main/templates/foo',
    )
  })

  test('rewrites links resolving to the repo root', () => {
    expect(escapedLinkUrl('page.md', '../')).toBe(
      'https://github.com/sdougbrown/avenor/tree/main',
    )
  })

  test('throws when the link escapes the repository', () => {
    expect(() => escapedLinkUrl('page.md', '../..')).toThrow(/escapes the repository/)
    expect(() => escapedLinkUrl('sub/page.md', '../../../x')).toThrow(/escapes the repository/)
  })
})