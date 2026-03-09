import type {
  DocumentJSON,
  DataplyDocument
} from '../types'
import { AnalysisProvider } from './AnalysisProvider'
import type { Transaction } from 'dataply'

/**
 * Sampling options for interval analysis providers.
 * Specify either a ratio (0~1) or an exact count.
 */
export type SampleOptions = {
  /** Ratio of documents to sample (0 exclusive ~ 1 inclusive) */
  rate: number
  count?: never
} | {
  rate?: never
  /** Exact number of documents to sample */
  count: number
}

/**
 * Abstract base class for interval analysis providers.
 * Data is accumulated in memory and persisted only when flush() is called.
 * No mutation hooks — state is computed independently (e.g. on a schedule or at init).
 */
export abstract class IntervalAnalysisProvider<T extends DocumentJSON = DocumentJSON> extends AnalysisProvider<T> {
  /**
   * Sample random documents from the entire dataset.
   * Fetches only PK index, then reads only the selected documents from disk.
   * @param sampleOptions Sampling strategy — either `{ rate }` or `{ count }`
   * @param tx Optional transaction
   * @returns Randomly selected documents
   */
  async sample(
    sampleOptions: SampleOptions,
    tx?: Transaction
  ): Promise<DataplyDocument<T>[]> {
    // 인덱스에서 PK만 가져옴 (문서 데이터 I/O 없음)
    const pks = await this.api.queryManager.getKeys({})
    const total = pks.length
    if (total === 0) return []

    const k = 'rate' in sampleOptions && sampleOptions.rate != null
      ? Math.ceil(total * Math.min(Math.max(sampleOptions.rate, 0), 1))
      : sampleOptions.count!

    const sampleCount = Math.min(Math.max(k, 0), total)
    if (sampleCount === 0) return []

    // Fisher-Yates partial shuffle on PK array
    for (let i = 0; i < sampleCount; i++) {
      const j = i + Math.floor(Math.random() * (total - i))
      const tmp = pks[i]
      pks[i] = pks[j]
      pks[j] = tmp
    }

    // 선택된 PK만으로 문서 조회 (k개만 I/O)
    const selectedPks = pks.slice(0, sampleCount)
    const rawResults = await this.api.selectMany(selectedPks, false, tx)
    const docs: DataplyDocument<T>[] = []
    for (let i = 0, len = rawResults.length; i < len; i++) {
      const raw = rawResults[i]
      if (raw) docs.push(JSON.parse(raw))
    }

    return docs
  }
}

