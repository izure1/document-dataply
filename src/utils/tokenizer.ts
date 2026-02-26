import { FTSConfig } from '../types'

export function whitespaceTokenize(text: string): string[] {
  if (typeof text !== 'string') return []
  return Array.from(new Set(text.split(/\s+/).filter(Boolean)))
}

export function ngramTokenize(text: string, gramSize: number): string[] {
  if (typeof text !== 'string') return []
  const tokens = new Set<string>()
  const words = text.split(/\s+/).filter(Boolean)

  for (const word of words) {
    if (word.length < gramSize) {
      if (word.length > 0) tokens.add(word)
      continue
    }
    for (let i = 0; i <= word.length - gramSize; i++) {
      tokens.add(word.slice(i, i + gramSize))
    }
  }
  return Array.from(tokens)
}

export function tokenize(text: string, options: FTSConfig): string[] {
  if (options.tokenizer === 'whitespace') {
    return whitespaceTokenize(text)
  }
  if (options.tokenizer === 'ngram') {
    return ngramTokenize(text, options.gramSize)
  }
  return []
}
