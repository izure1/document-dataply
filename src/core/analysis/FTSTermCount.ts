import type { DocumentJSON } from '../../types'
import { Transaction } from 'dataply'
import { IntervalAnalysisProvider } from '../IntervalAnalysisProvider'
import { tokenize } from '../../utils/tokenizer'

// 구조: { [fieldName]: { [tokenizerStrategy]: { [token]: count } } }
type TermCountData = Record<string, Record<string, Record<string, number>>>

export class FTSTermCount<T extends DocumentJSON = DocumentJSON> extends IntervalAnalysisProvider<T> {
  readonly name = 'fts_term_count'

  private termCount: TermCountData = {}

  async serialize(tx: Transaction): Promise<string> {
    const docs = await this.sample({ count: 1000 }, tx)

    this.termCount = {}

    if (docs.length === 0) return JSON.stringify({})

    const ftsIndices = new Map<string, any>()
    for (const [indexName, config] of this.api.indexManager.registeredIndices) {
      if (config.type === 'fts') {
        ftsIndices.set(indexName, config)
      }
    }

    if (ftsIndices.size === 0) return JSON.stringify({})

    for (let i = 0, len = docs.length; i < len; i++) {
      const doc = docs[i]
      const flatDoc = this.api.flattenDocument(doc)

      for (const [indexName, config] of ftsIndices) {
        const primaryField = this.api.indexManager.getPrimaryField(config)
        const v = flatDoc[primaryField]
        if (typeof v === 'string' && v.length > 0) {
          const ftsConfig = this.api.indexManager.getFtsConfig(config)
          const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
          const tokenizerStrategy = ftsConfig
            ? (ftsConfig.tokenizer === 'ngram' ? `${ftsConfig.gramSize}gram` : ftsConfig.tokenizer)
            : 'whitespace'

          if (!this.termCount[primaryField]) {
            this.termCount[primaryField] = {}
          }
          if (!this.termCount[primaryField][tokenizerStrategy]) {
            this.termCount[primaryField][tokenizerStrategy] = {}
          }

          const targetMap = this.termCount[primaryField][tokenizerStrategy]
          for (let j = 0, len = tokens.length; j < len; j++) {
            const token = tokens[j]
            targetMap[token] = (targetMap[token] || 0) + 1
          }
        }
      }
    }

    // Top-K Pruning per tokenizer strategy
    const optimizedTermCount: TermCountData = {}
    for (const field in this.termCount) {
      optimizedTermCount[field] = {}
      for (const strategy in this.termCount[field]) {
        const tokenMap = this.termCount[field][strategy]
        const sorted = Object.entries(tokenMap)
          .sort((a, b) => b[1] - a[1])
          .slice(0, 1000)

        optimizedTermCount[field][strategy] = {}
        for (let i = 0, len = sorted.length; i < len; i++) {
          optimizedTermCount[field][strategy][sorted[i][0]] = sorted[i][1]
        }
      }
    }

    this.termCount = optimizedTermCount

    return JSON.stringify(this.termCount)
  }

  async load(data: string | null, tx: Transaction): Promise<void> {
    this.termCount = {}
    if (!data) {
      return
    }
    try {
      const parsed = JSON.parse(data)
      if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) {
        this.termCount = parsed
      }
    } catch (e) {
      // Ignore parse error
    }
  }
}
