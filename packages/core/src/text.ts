// The one JSON-quoting rule and the one template substituter for every
// TypeScript tool router, the twin of internal/admission's quote.go. A leaf
// module on purpose: the admission engine and the per-tool text renderers both
// import it, so neither imports the other.

/** Render a value for a template fact as one JSON string literal. Control,
 * format, line- and paragraph-separator characters are escaped, so a label a
 * producer controls cannot carry a bidi override or a zero-width character into
 * tool text. The rule is byte-identical to internal/admission.Quote.
 */
export function quote(value: string): string {
  return `"${escapeNonPrintable(value)}"`
}

/** Escape the same characters as quote without adding the surrounding quotes. */
export function sanitize(value: string): string {
  return escapeNonPrintable(value)
}

/** Render every value with quote and join them with ", ". */
export function quoteList(values: string[]): string {
  return values.map(quote).join(', ')
}

/** Substitute `{fact}` placeholders, leaving an unfilled placeholder intact. */
export function substitute(template: string | undefined, facts: Record<string, string> = {}): string {
  if (!template) return ''
  let out = ''
  for (let i = 0; i < template.length; ) {
    if (template[i] === '{') {
      const end = template.indexOf('}', i)
      if (end > i && Object.prototype.hasOwnProperty.call(facts, template.slice(i + 1, end))) {
        out += facts[template.slice(i + 1, end)]
        i = end + 1
        continue
      }
    }
    out += template[i]
    i++
  }
  return out
}

const NON_PRINTABLE = /[\p{Cf}\p{Zl}\p{Zp}]/u

function escapeNonPrintable(value: string): string {
  let out = ''
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0
    switch (character) {
      case '"':
        out += '\\"'
        continue
      case '\\':
        out += '\\\\'
        continue
      case '\b':
        out += '\\b'
        continue
      case '\f':
        out += '\\f'
        continue
      case '\n':
        out += '\\n'
        continue
      case '\r':
        out += '\\r'
        continue
      case '\t':
        out += '\\t'
        continue
    }
    if (code < 0x20 || code === 0x7f || (code >= 0x80 && code <= 0x9f) || NON_PRINTABLE.test(character)) {
      if (code > 0xffff) {
        const high = 0xd800 + ((code - 0x10000) >> 10)
        const low = 0xdc00 + ((code - 0x10000) & 0x3ff)
        out += `\\u${high.toString(16).padStart(4, '0')}\\u${low.toString(16).padStart(4, '0')}`
        continue
      }
      out += `\\u${code.toString(16).padStart(4, '0')}`
      continue
    }
    out += character
  }
  return out
}
