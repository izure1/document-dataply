import type { DocumentJSON } from '../../types'
import { Transaction } from 'dataply'
import { IntervalAnalysisProvider } from '../IntervalAnalysisProvider'
import { tokenize } from '../../utils/tokenizer'

// 구조: { [fieldName]: { [tokenizerStrategy]: { [token]: count } } }
type TermCountData = Record<string, Record<string, Record<string, number>>>

export class FTSTermCount<T extends DocumentJSON = DocumentJSON> extends IntervalAnalysisProvider<T> {
  readonly name = 'fts_term_count'

  private termCount: TermCountData = {}
  private sampleSize: number = 0

  async serialize(tx: Transaction): Promise<string> {
    const docs = await this.sample({ count: this.api.analysisManager.sampleSize }, tx)

    this.termCount = {}
    this.sampleSize = docs.length

    if (docs.length === 0) return JSON.stringify({ _sampleSize: 0 })

    const ftsIndices = new Map<string, any>()
    for (const [indexName, config] of this.api.indexManager.registeredIndices) {
      if (config.type === 'fts') {
        ftsIndices.set(indexName, config)
      }
    }

    if (ftsIndices.size === 0) return JSON.stringify({ _sampleSize: this.sampleSize })

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

    return JSON.stringify({ _sampleSize: this.sampleSize, ...this.termCount })
  }

  async load(data: string | null, tx: Transaction): Promise<void> {
    this.termCount = {}
    this.sampleSize = 0
    if (!data) {
      return
    }
    try {
      const parsed = JSON.parse(data)
      if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) {
        const { _sampleSize, ...rest } = parsed
        this.sampleSize = typeof _sampleSize === 'number' ? _sampleSize : 0
        this.termCount = rest
      }
    } catch (e) {
      // Ignore parse error
    }
  }

  /**
   * 특정 field/strategy/token의 문서 빈도를 반환합니다.
   * 통계에 없으면 0을 반환합니다.
   */
  getTermCount(field: string, strategy: string, token: string): number {
    return this.termCount[field]?.[strategy]?.[token] ?? 0
  }

  /**
   * 쿼리 토큰 배열에서 최소 빈도(AND 시맨틱스 상한선)를 반환합니다.
   * 통계가 없거나 sampleSize가 0이면 -1을 반환합니다.
   */
  getMinTokenCount(field: string, strategy: string, tokens: string[]): number {
    if (this.sampleSize === 0 || tokens.length === 0) return -1

    let minCount = Infinity
    for (let i = 0, len = tokens.length; i < len; i++) {
      const count = this.getTermCount(field, strategy, tokens[i])
      if (count < minCount) minCount = count
    }
    return minCount === Infinity ? -1 : minCount
  }

  /**
   * 통계가 유효한지 여부를 반환합니다.
   */
  get hasSampleData(): boolean {
    return this.sampleSize > 0
  }

  /**
   * 통계 수집 시 사용된 샘플 크기를 반환합니다.
   */
  getSampleSize(): number {
    return this.sampleSize
  }
}
