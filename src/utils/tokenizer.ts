import { FTSConfig } from '../types'

export function whitespaceTokenize(text: string): string[] {
  if (typeof text !== 'string') return []
  return Array.from(new Set(text.split(/\s+/).filter(Boolean)))
}

export function ngramTokenize(text: string, gramSize: number): string[] {
  if (typeof text !== 'string') return []
  const tokens = new Set<string>()
  const words = text.split(/\s+/).filter(Boolean)

  for (let i = 0, len = words.length; i < len; i++) {
    const word = words[i]
    if (word.length < gramSize) {
      if (word.length > 0) tokens.add(word)
      continue
    }
    for (let j = 0, wLen = word.length; j <= wLen - gramSize; j++) {
      tokens.add(word.slice(j, j + gramSize))
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
