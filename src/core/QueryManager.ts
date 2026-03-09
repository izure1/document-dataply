import * as os from 'node:os'
import type {
  DataplyTreeValue,
  DocumentDataplyQuery,
  DocumentDataplyCondition,
  DocumentDataplyQueryOptions,
  DataplyDocument,
  Primitive,
  FinalFlatten,
  DocumentJSON
} from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import type { Optimizer } from './Optimizer'
import { BPTreeAsync, type BPTreeCondition } from 'dataply'
import { tokenize } from '../utils/tokenizer'
import { BinaryHeap } from '../utils/heap'

export class QueryManager<T extends DocumentJSON> {
  private readonly operatorConverters: Partial<Record<
    keyof DocumentDataplyCondition<FinalFlatten<T>>,
    keyof BPTreeCondition<FinalFlatten<T>>
  >> = {
      equal: 'primaryEqual',
      notEqual: 'primaryNotEqual',
      lt: 'primaryLt',
      lte: 'primaryLte',
      gt: 'primaryGt',
      gte: 'primaryGte',
      or: 'primaryOr',
      like: 'like',
    }

  constructor(
    private api: DocumentDataplyAPI<T>,
    private optimizer: Optimizer<T>
  ) { }

  /**
   * Transforms a query object into a verbose query object
   */
  verboseQuery<
    U extends Partial<DocumentDataplyQuery<T>>,
    V extends DataplyTreeValue<U>
  >(query: Partial<DocumentDataplyQuery<U>>): Partial<DocumentDataplyQuery<V>> {
    const result = {}
    for (const field in query) {
      const conditions = query[field] as Partial<DocumentDataplyCondition<U>>
      let newConditions: BPTreeCondition<V>
      if (typeof conditions !== 'object' || conditions === null) {
        newConditions = { primaryEqual: { v: conditions } as unknown as V }
      }
      else {
        newConditions = {}
        for (const operator in conditions) {
          const before = operator as keyof typeof conditions
          const after = this.operatorConverters[before as keyof DocumentDataplyCondition<FinalFlatten<T>>]
          const v = conditions[before]
          if (!after) {
            if (before === 'match') {
              (newConditions as any)[before] = v
            }
            continue
          }
          if (before === 'or' && Array.isArray(v)) {
            newConditions[after] = v.map(val => ({ v: val })) as any
          }
          else if (before === 'like') {
            newConditions[after] = v as any
          }
          else {
            newConditions[after] = { v } as any
          }
        }
      }
      (result as any)[field] = newConditions
    }
    return result
  }

  getFreeMemoryChunkSize(): {
    verySmallChunkSize: number,
    smallChunkSize: number
  } {
    const freeMem = os.freemem()
    const safeLimit = freeMem * 0.2
    const verySmallChunkSize = safeLimit * 0.05
    const smallChunkSize = safeLimit * 0.3
    return { verySmallChunkSize, smallChunkSize }
  }

  private async *applyCandidateByFTSStream<V>(
    candidate: {
      tree: BPTreeAsync<string, DataplyTreeValue<V>>,
      condition: Partial<DocumentDataplyCondition<Partial<DocumentDataplyQuery<T>>>>,
    },
    matchedTokens: string[],
    filterValues?: Set<number>,
    order?: 'asc' | 'desc'
  ): AsyncIterableIterator<number> {
    const keys = new Set<number>()
    for (let i = 0, len = matchedTokens.length; i < len; i++) {
      const token = matchedTokens[i]
      for await (const pair of candidate.tree.whereStream(
        { primaryEqual: { v: token } } as any,
        { order }
      )) {
        const pk = (pair[1] as any).k as number
        if (filterValues && !filterValues.has(pk)) continue
        if (!keys.has(pk)) {
          keys.add(pk)
          yield pk
        }
      }
    }
  }

  private applyCandidateStream<V>(
    candidate: {
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<Partial<DocumentDataplyQuery<T>>>>,
    },
    filterValues?: Set<number>,
    order?: 'asc' | 'desc'
  ): AsyncIterableIterator<number> {
    return candidate.tree.keysStream(
      candidate.condition as any,
      { filterValues, order }
    ) as AsyncIterableIterator<number>
  }

  async getKeys(
    query: Partial<DocumentDataplyQuery<T>>,
    orderBy?: string,
    sortOrder: 'asc' | 'desc' = 'asc'
  ): Promise<Float64Array> {
    const isQueryEmpty = Object.keys(query).length === 0
    const normalizedQuery = isQueryEmpty ? { _id: { gte: 0 } } : query
    const selectivity = await this.optimizer.getSelectivityCandidate(
      this.verboseQuery(normalizedQuery as any),
      orderBy as string
    )

    if (!selectivity) return new Float64Array(0)

    const { driver, others, rollback } = selectivity
    const useIndexOrder = orderBy === undefined || driver.isIndexOrderSupported
    const candidates = [driver, ...others]

    let keys: Set<number> | undefined = undefined
    for (let i = 0, len = candidates.length; i < len; i++) {
      const candidate = candidates[i]
      const currentOrder = useIndexOrder ? sortOrder : undefined
      if (
        candidate.isFtsMatch &&
        candidate.matchTokens &&
        candidate.matchTokens.length > 0
      ) {
        const stream = this.applyCandidateByFTSStream(
          candidate as any,
          candidate.matchTokens,
          keys,
          currentOrder
        )
        keys = new Set()
        for await (const pk of stream) keys.add(pk)
      }
      else {
        const stream = this.applyCandidateStream(candidate as any, keys, currentOrder)
        keys = new Set()
        for await (const pk of stream) keys.add(pk)
      }
    }

    rollback()
    return new Float64Array(Array.from(keys || []))
  }

  async getDriverKeys(
    query: Partial<DocumentDataplyQuery<T>>,
    orderBy?: string,
    sortOrder: 'asc' | 'desc' = 'asc'
  ): Promise<{
    keysStream: AsyncIterableIterator<number>,
    others: {
      tree: BPTreeAsync<string | number, DataplyTreeValue<Primitive>>,
      condition: any,
      field: string,
      indexName: string,
      isFtsMatch: boolean,
      matchTokens?: string[],
      coveredFields?: string[]
    }[],
    compositeVerifyConditions: {
      field: string,
      condition: any
    }[],
    isDriverOrderByField: boolean,
    rollback: () => void,
  } | null> {
    const isQueryEmpty = Object.keys(query).length === 0
    const normalizedQuery = isQueryEmpty ? { _id: { gte: 0 } } : query
    const selectivity = await this.optimizer.getSelectivityCandidate(
      this.verboseQuery(normalizedQuery as any),
      orderBy as string
    )

    if (!selectivity) return null

    const { driver, others, compositeVerifyConditions, rollback } = selectivity
    const useIndexOrder = orderBy === undefined || driver.isIndexOrderSupported
    const currentOrder = useIndexOrder ? sortOrder : undefined

    let keysStream: AsyncIterableIterator<number>
    if (
      driver.isFtsMatch &&
      driver.matchTokens &&
      driver.matchTokens.length > 0
    ) {
      keysStream = this.applyCandidateByFTSStream(
        driver as any,
        driver.matchTokens,
        undefined,
        currentOrder
      )
    }
    else {
      keysStream = this.applyCandidateStream(driver as any, undefined, currentOrder)
    }

    return {
      keysStream,
      others: others as any,
      compositeVerifyConditions,
      isDriverOrderByField: useIndexOrder,
      rollback,
    }
  }

  verifyFts(
    doc: DataplyDocument<T>,
    ftsConditions: { field: string, matchTokens: string[] }[]
  ): boolean {
    const flatDoc = this.api.flattenDocument(doc)
    for (let i = 0, len = ftsConditions.length; i < len; i++) {
      const { field, matchTokens } = ftsConditions[i]
      const docValue = flatDoc[field]
      if (typeof docValue !== 'string') return false
      for (let j = 0, jLen = matchTokens.length; j < jLen; j++) {
        const token = matchTokens[j]
        if (!docValue.includes(token)) return false
      }
    }
    return true
  }

  verifyCompositeConditions(
    doc: DataplyDocument<T>,
    conditions: { field: string, condition: any }[]
  ): boolean {
    if (conditions.length === 0) return true
    const flatDoc = this.api.flattenDocument(doc)
    for (let i = 0, len = conditions.length; i < len; i++) {
      const { field, condition } = conditions[i]
      const docValue = flatDoc[field]
      if (docValue === undefined) return false
      if (!this.verifyValue(docValue, condition)) return false
    }
    return true
  }

  verifyValue(value: Primitive, condition: any): boolean {
    if (typeof condition !== 'object' || condition === null) {
      return value === condition
    }
    if ('primaryEqual' in condition) {
      return value === condition.primaryEqual?.v
    }
    if ('primaryNotEqual' in condition) {
      return value !== condition.primaryNotEqual?.v
    }
    if ('primaryLt' in condition) {
      return value !== null && condition.primaryLt?.v !== undefined && value < condition.primaryLt.v
    }
    if ('primaryLte' in condition) {
      return value !== null && condition.primaryLte?.v !== undefined && value <= condition.primaryLte.v
    }
    if ('primaryGt' in condition) {
      return value !== null && condition.primaryGt?.v !== undefined && value > condition.primaryGt.v
    }
    if ('primaryGte' in condition) {
      return value !== null && condition.primaryGte?.v !== undefined && value >= condition.primaryGte.v
    }
    if ('primaryOr' in condition && Array.isArray(condition.primaryOr)) {
      return condition.primaryOr.some((c: any) => value === c?.v)
    }
    return true
  }

  adjustChunkSize(currentChunkSize: number, chunkTotalSize: number): number {
    if (chunkTotalSize <= 0) return currentChunkSize
    const { verySmallChunkSize, smallChunkSize } = this.getFreeMemoryChunkSize()
    if (chunkTotalSize < verySmallChunkSize) return currentChunkSize * 2
    if (chunkTotalSize > smallChunkSize) return Math.max(Math.floor(currentChunkSize / 2), 20)
    return currentChunkSize
  }

  async *processChunkedKeysWithVerify(
    keysStream: AsyncIterableIterator<number>,
    startIdx: number,
    initialChunkSize: number,
    limit: number,
    ftsConditions: { field: string, matchTokens: string[] }[],
    compositeVerifyConditions: { field: string, condition: any }[],
    others: {
      tree: BPTreeAsync<string | number, DataplyTreeValue<Primitive>>,
      condition: any,
      field: string,
      indexName: string,
      isFtsMatch: boolean,
      matchTokens?: string[],
      coveredFields?: string[]
    }[],
    tx: any
  ): AsyncGenerator<DataplyDocument<T>> {
    const verifyOthers = others.filter(o => !o.isFtsMatch)
    const isFts = ftsConditions.length > 0
    const isCompositeVerify = compositeVerifyConditions.length > 0
    const isVerifyOthers = verifyOthers.length > 0
    const isInfinityLimit = !isFinite(limit)
    const isReadQuotaLimited = !isInfinityLimit || !isCompositeVerify || !isVerifyOthers || !isFts
    let currentChunkSize = isReadQuotaLimited ? limit : initialChunkSize
    let chunk: number[] = []
    let chunkSize = 0
    let dropped = 0

    const processChunk = async (pks: number[]) => {
      const docs: DataplyDocument<T>[] = []
      const rawResults = await this.api.selectMany(new Float64Array(pks), false, tx)
      let chunkTotalSize = 0

      for (let j = 0, len = rawResults.length; j < len; j++) {
        const s = rawResults[j]
        if (!s) continue
        const doc = JSON.parse(s)
        chunkTotalSize += s.length * 2

        if (isFts && !this.verifyFts(doc, ftsConditions)) continue
        if (
          isCompositeVerify &&
          this.verifyCompositeConditions(doc, compositeVerifyConditions) === false
        ) continue

        if (isVerifyOthers) {
          const flatDoc = this.api.flattenDocument(doc)
          let passed = true
          for (let k = 0, kLen = verifyOthers.length; k < kLen; k++) {
            const other = verifyOthers[k]
            const coveredFields = other.coveredFields
            let fieldValue: Primitive | Primitive[]
            if (coveredFields && coveredFields.length > 1) {
              // 복합 인덱스: 모든 covered field 값으로 복합 키 구성
              const values: Primitive[] = []
              let hasMissing = false
              for (let f = 0, fLen = coveredFields.length; f < fLen; f++) {
                const v = flatDoc[coveredFields[f]]
                if (v === undefined) { hasMissing = true; break }
                values.push(v)
              }
              if (hasMissing) { passed = false; break }
              fieldValue = values
            } else {
              fieldValue = flatDoc[other.field]
              if (fieldValue === undefined) { passed = false; break }
            }
            const treeValue: DataplyTreeValue<Primitive> = { k: doc._id, v: fieldValue as any }
            if (!other.tree.verify(treeValue, other.condition)) {
              passed = false
              break
            }
          }
          if (!passed) continue
        }

        docs.push(doc)
      }

      if (!isReadQuotaLimited) {
        currentChunkSize = this.adjustChunkSize(currentChunkSize, chunkTotalSize)
      }
      return docs
    }

    for await (const pk of keysStream) {
      if (dropped < startIdx) {
        dropped++
        continue
      }
      chunk.push(pk)
      chunkSize++
      if (chunkSize >= currentChunkSize) {
        const docs = await processChunk(chunk)
        for (let j = 0, dLen = docs.length; j < dLen; j++) yield docs[j]
        chunk = []
        chunkSize = 0
      }
    }

    if (chunkSize > 0) {
      const docs = await processChunk(chunk)
      for (let j = 0, dLen = docs.length; j < dLen; j++) yield docs[j]
    }
  }

  /**
   * Count documents from the database that match the query
   * @param query The query to use
   * @param tx The transaction to use
   * @returns The number of documents that match the query
   */
  async countDocuments(
    query: Partial<DocumentDataplyQuery<T>>,
    tx?: any
  ): Promise<number> {
    return this.api.runWithDefault(async (tx) => {
      const pks = await this.getKeys(query)
      return pks.length
    }, tx)
  }

  selectDocuments(
    query: Partial<DocumentDataplyQuery<T>>,
    options: DocumentDataplyQueryOptions = {},
    tx?: any
  ): {
    stream: () => AsyncIterableIterator<DataplyDocument<T>>
    drain: () => Promise<DataplyDocument<T>[]>
  } {
    for (const field of Object.keys(query)) {
      if (!this.api.indexedFields.has(field)) {
        throw new Error(`Query field "${field}" is not indexed. Available indexed fields: ${Array.from(this.api.indexedFields).join(', ')}`)
      }
    }

    const orderBy = options.orderBy
    if (orderBy !== undefined && !this.api.indexedFields.has(orderBy as string)) {
      throw new Error(`orderBy field "${orderBy}" is not indexed. Available indexed fields: ${Array.from(this.api.indexedFields).join(', ')}`)
    }

    const {
      limit = Infinity,
      offset = 0,
      sortOrder = 'asc',
      orderBy: orderByField
    } = options

    const self = this
    const stream = () => this.api.streamWithDefault(async function* (tx) {
      const ftsConditions: { field: string, matchTokens: string[] }[] = []
      for (const field in query) {
        const q = query[field] as any
        if (
          q &&
          typeof q === 'object' &&
          'match' in q &&
          typeof q.match === 'string'
        ) {
          const indexNames = self.api.indexManager.fieldToIndices.get(field) || []
          for (const indexName of indexNames) {
            const config = self.api.indexManager.registeredIndices.get(indexName)
            if (config && config.type === 'fts') {
              const ftsConfig = self.api.indexManager.getFtsConfig(config)
              if (ftsConfig) {
                ftsConditions.push({ field, matchTokens: tokenize(q.match, ftsConfig) })
              }
              break
            }
          }
        }
      }

      const driverResult = await self.getDriverKeys(query, orderByField, sortOrder)
      if (!driverResult) return
      const { keysStream, others, compositeVerifyConditions, isDriverOrderByField, rollback } = driverResult
      const initialChunkSize = self.api.options.pageSize

      try {
        if (!isDriverOrderByField && orderByField) {
          const topK = limit === Infinity ? Infinity : offset + limit
          let heap: BinaryHeap<DataplyDocument<T>> | null = null

          if (topK !== Infinity) {
            heap = new BinaryHeap((a: DataplyDocument<T>, b: DataplyDocument<T>) => {
              const aVal = (a as any)[orderByField] ?? (a as any)._id
              const bVal = (b as any)[orderByField] ?? (b as any)._id
              const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0
              return sortOrder === 'asc' ? -cmp : cmp
            })
          }

          const results: DataplyDocument<T>[] = []
          for await (const doc of self.processChunkedKeysWithVerify(
            keysStream,
            0,
            initialChunkSize,
            Infinity,
            ftsConditions,
            compositeVerifyConditions,
            others,
            tx
          )) {
            if (heap) {
              if (heap.size < topK) heap.push(doc)
              else {
                const top = heap.peek()
                if (top) {
                  const aVal = (doc as any)[orderByField] ?? (doc as any)._id
                  const bVal = (top as any)[orderByField] ?? (top as any)._id
                  const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0
                  if (sortOrder === 'asc' ? cmp < 0 : cmp > 0) heap.replace(doc)
                }
              }
            }
            else {
              results.push(doc)
            }
          }

          const finalDocs = heap ? heap.toArray() : results
          finalDocs.sort((a, b) => {
            const aVal = (a as any)[orderByField] ?? (a as any)._id
            const bVal = (b as any)[orderByField] ?? (b as any)._id
            const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0
            return sortOrder === 'asc' ? cmp : -cmp
          })

          const end = limit === Infinity ? undefined : offset + limit
          const limitedResults = finalDocs.slice(offset, end)
          for (let j = 0, len = limitedResults.length; j < len; j++) {
            yield limitedResults[j]
          }
        }
        else {
          const hasFilters = ftsConditions.length > 0 || compositeVerifyConditions.length > 0 || others.length > 0
          const startIdx = hasFilters ? 0 : offset

          let yieldedCount = 0
          let skippedCount = hasFilters ? 0 : offset

          for await (const doc of self.processChunkedKeysWithVerify(
            keysStream,
            startIdx,
            initialChunkSize,
            limit,
            ftsConditions,
            compositeVerifyConditions,
            others,
            tx
          )) {
            if (skippedCount < offset) {
              skippedCount++
              continue
            }
            if (yieldedCount >= limit) break
            yield doc
            yieldedCount++
          }
        }
      }
      finally {
        rollback()
      }
    }, tx)

    const drain = async () => {
      const result: DataplyDocument<T>[] = []
      for await (const document of stream()) {
        result.push(document)
      }
      return result
    }
    return { stream, drain }
  }
}
