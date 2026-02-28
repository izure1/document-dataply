import * as os from 'node:os'
import type {
  DataplyTreeValue,
  DocumentDataplyInnerMetadata,
  DocumentDataplyOptions,
  DocumentJSON,
  FlattenedDocumentJSON,
  Primitive,
  DataplyDocument,
  DocumentDataplyMetadata,
  IndexConfig,
  DocumentDataplyQuery,
  DocumentDataplyIndexedQuery,
  FinalFlatten,
  DocumentDataplyCondition,
  DocumentDataplyQueryOptions,
  FTSConfig
} from '../types'
import {
  DataplyAPI,
  Transaction,
  BPTreeAsync,
  BPTreeAsyncTransaction,
  Ryoiki,
  type BPTreeCondition
} from 'dataply'
import { DocumentSerializeStrategyAsync } from './bptree/documentStrategy'
import { DocumentValueComparator } from './bptree/documentComparator'
import { catchPromise } from '../utils/catchPromise'
import { BinaryHeap } from '../utils/heap'
import { tokenize } from '../utils/tokenizer'

export class DocumentDataplyAPI<T extends DocumentJSON, IC extends IndexConfig<T>> extends DataplyAPI {
  declare runWithDefault
  declare streamWithDefault

  indices: DocumentDataplyInnerMetadata['indices'] = {}
  readonly trees: Map<string, BPTreeAsync<string | number, DataplyTreeValue<Primitive>>> = new Map()
  readonly comparator = new DocumentValueComparator()
  private pendingBackfillFields: string[] = []
  private readonly lock: Ryoiki

  readonly indexedFields: Set<string>
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

  constructor(file: string, options: DocumentDataplyOptions<T, IC>) {
    super(file, options)
    this.trees = new Map()
    this.lock = new Ryoiki()

    // indices에 지정된 필드들을 저장 (_id는 항상 포함)
    this.indexedFields = new Set(['_id'])
    if (options?.indices) {
      for (const field of Object.keys(options.indices)) {
        this.indexedFields.add(field)
      }
    }

    this.hook.onceAfter('init', async (tx, isNewlyCreated) => {
      if (isNewlyCreated) {
        await this.initializeDocumentFile(tx)
      }
      if (!(await this.verifyDocumentFile(tx))) {
        throw new Error('Document metadata verification failed')
      }
      const metadata = await this.getDocumentInnerMetadata(tx)
      const optionsIndices = (options as DocumentDataplyOptions<T, IC>).indices ?? {}
      const targetIndices: { [key: string]: boolean } = {
        ...optionsIndices,
        _id: true
      }

      const backfillTargets: string[] = []
      let isMetadataChanged = false

      for (const field in targetIndices) {
        const isBackfillEnabled = targetIndices[field]
        const existingIndex = metadata.indices[field]

        // 새롭게 추가된 인덱스
        if (!existingIndex) {
          // 사용자 요청: readHead에서 행 생성.
          // PK를 -1로 설정하여 플레이스홀더로 사용.
          metadata.indices[field] = [-1, isBackfillEnabled as boolean | FTSConfig]
          isMetadataChanged = true

          if (isBackfillEnabled && !isNewlyCreated) {
            // DB가 새로 생성된 경우, 백필할 데이터가 없음.
            backfillTargets.push(field)
          }
        }
        // 기존 인덱스
        else {
          const [_pk, isMetaBackfillEnabled] = existingIndex
          // 비활성 -> 활성
          if (isBackfillEnabled && !isMetaBackfillEnabled) {
            metadata.indices[field][1] = isBackfillEnabled as boolean | FTSConfig
            isMetadataChanged = true
            backfillTargets.push(field)
          }
          // 활성 -> 비활성
          else if (!isBackfillEnabled && isMetaBackfillEnabled) {
            metadata.indices[field][1] = false
            isMetadataChanged = true
          }
        }
      }

      if (isMetadataChanged) {
        await this.updateDocumentInnerMetadata(metadata, tx)
      }

      this.indices = metadata.indices

      // 트리 초기화
      for (const field in this.indices) {
        if (field in targetIndices) {
          const tree = new BPTreeAsync<number, DataplyTreeValue<Primitive>>(
            new DocumentSerializeStrategyAsync<Primitive>(
              (this.rowTableEngine as any).order,
              this,
              this.txContext,
              field
            ),
            this.comparator as any
          )
          await tree.init()
          this.trees.set(field, tree as any)
        }
      }

      // 초기화 중 실행하는 대신 백필 대기 필드 저장
      // 초기화 후 backfillIndices()를 호출하여 백필 수행
      this.pendingBackfillFields = backfillTargets

      return tx
    })
  }

  async getDocument(pk: number, tx?: Transaction): Promise<DataplyDocument<T>> {
    return this.runWithDefault(async (tx) => {
      const row = await this.select(pk, false, tx)
      if (!row) {
        throw new Error(`Document not found with PK: ${pk}`)
      }
      return JSON.parse(row) as DataplyDocument<T>
    }, tx)
  }

  async readLock<T>(fn: () => T): Promise<T> {
    let lockId: string
    return this.lock.readLock(async (_lockId) => {
      lockId = _lockId
      return await fn()
    }).finally(() => {
      this.lock.readUnlock(lockId!)
    })
  }

  async writeLock<T>(fn: () => T): Promise<T> {
    let lockId: string
    return this.lock.writeLock(async (_lockId) => {
      lockId = _lockId
      return await fn()
    }).finally(() => {
      this.lock.writeUnlock(lockId!)
    })
  }

  /**
   * Backfill indices for fields that were added with `true` option after data was inserted.
   * This method should be called after `init()` if you want to index existing documents
   * for newly added index fields.
   * 
   * @returns Number of documents that were backfilled
   */
  async backfillIndices(tx?: Transaction): Promise<number> {
    return this.runWithDefault(async (tx) => {
      // 백필할 데이터가 없거나
      if (this.pendingBackfillFields.length === 0) {
        return 0
      }

      const backfillTargets = this.pendingBackfillFields
      const metadata = await this.getDocumentInnerMetadata(tx)

      // 아직 아무런 데이터도 삽입되지 않은 데이터베이스라면 제외
      if (metadata.lastId === 0) {
        return 0
      }

      // 대상 필드당 하나의 트랜잭션 생성
      const fieldTxMap: Record<
        string,
        BPTreeAsyncTransaction<string | number, DataplyTreeValue<Primitive>>
      > = {}
      const fieldMap: Map<
        BPTreeAsyncTransaction<string | number, DataplyTreeValue<Primitive>>,
        DataplyTreeValue<Primitive>[]
      > = new Map()

      for (const field of backfillTargets) {
        const tree = this.trees.get(field)
        if (tree && field !== '_id') {
          fieldTxMap[field] = await tree.createTransaction()
        }
      }

      let backfilledCount = 0

      const idTree = this.trees.get('_id')
      if (!idTree) {
        throw new Error('ID tree not found')
      }

      const stream = idTree.whereStream({
        primaryGte: { v: 0 }
      })

      // 모든 행을 스캔하여 문서 찾기 (1번 행은 메타데이터, 2번 행 이후는 트리 헤드 또는 문서)
      for await (const [k, complexValue] of stream) {
        const doc = await this.getDocument(k as number, tx)
        if (!doc) continue
        const flatDoc = this.flattenDocument(doc)
        for (const field of backfillTargets) {
          if (
            !(field in flatDoc) || // 문서에 해당 필드가 없음
            !(field in fieldTxMap) // b+tree 트랜잭션에 해당 필드가 없음
          ) {
            continue
          }
          const v = flatDoc[field]
          const btx = fieldTxMap[field]
          const indexConfig = metadata.indices[field]?.[1]
          const isFts = typeof indexConfig === 'object' && indexConfig?.type === 'fts' && typeof v === 'string'

          let tokens = [v as string]
          if (isFts) {
            tokens = tokenize(v, indexConfig)
          }

          const batchInsertData: [number | string, DataplyTreeValue<Primitive>][] = []
          for (let i = 0, len = tokens.length; i < len; i++) {
            const token = tokens[i]
            const keyToInsert = isFts ? this.getTokenKey(k as number, token as string) : k
            const entry = { k: k as number, v: token }
            batchInsertData.push([keyToInsert, entry])
            if (!fieldMap.has(btx)) {
              fieldMap.set(btx, [])
            }
            fieldMap.get(btx)!.push({ k: keyToInsert as any, v: entry as any })
          }
          await btx.batchInsert(batchInsertData)
        }
        backfilledCount++
      }

      // 모든 트랜잭션 커밋
      const btxs = Object.values(fieldTxMap)
      const success = []
      try {
        for (const btx of btxs) {
          await btx.commit()
          success.push(btx)
        }
      } catch (err) {
        for (const btx of btxs) {
          await btx.rollback()
        }
        for (const btx of success) {
          const entries = fieldMap.get(btx)
          if (!entries) continue
          for (const entry of entries) {
            await btx.delete(entry.k, entry)
          }
        }
        throw err
      }

      // 백필 후 대기 필드 초기화
      this.pendingBackfillFields = []

      return backfilledCount
    }, tx)
  }

  createDocumentInnerMetadata(indices: DocumentDataplyInnerMetadata['indices']): DocumentDataplyInnerMetadata {
    return {
      magicString: 'document-dataply',
      version: 1,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      lastId: 0,
      indices,
    }
  }

  async initializeDocumentFile(tx: Transaction): Promise<void> {
    const metadata = await this.select(1, false, tx)
    if (metadata) {
      throw new Error('Document metadata already exists')
    }
    // 1. _id 인덱스 플레이스홀더(pk=-1)를 포함한 초기 메타데이터 생성
    // 실제 트리 헤드 행은 DocumentSerializeStrategyAsync.readHead()에서 지연 생성됨
    const metaObj = this.createDocumentInnerMetadata({
      _id: [-1, true]
    })
    // 2. 플레이스홀더로 1번 행에 저장
    await this.insertAsOverflow(JSON.stringify(metaObj), false, tx)
  }

  async verifyDocumentFile(tx: Transaction): Promise<boolean> {
    const row = await this.select(1, false, tx)
    if (!row) {
      return false
    }
    const data = JSON.parse(row)
    return data.magicString === 'document-dataply' && data.version === 1
  }

  private flatten(obj: any, parentKey: string = '', result: FlattenedDocumentJSON = {}): FlattenedDocumentJSON {
    for (const key in obj) {
      const newKey = parentKey ? `${parentKey}.${key}` : key
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        this.flatten(obj[key], newKey, result)
      }
      else {
        result[newKey] = obj[key]
      }
    }
    return result
  }

  /**
   * returns flattened document
   * @param document 
   * @returns 
   */
  flattenDocument(document: T): FlattenedDocumentJSON {
    return this.flatten(document, '', {})
  }

  async getDocumentMetadata(tx: Transaction): Promise<DocumentDataplyMetadata> {
    const metadata = await this.getMetadata(tx)
    return {
      pageSize: metadata.pageSize,
      pageCount: metadata.pageCount,
      rowCount: metadata.rowCount
    }
  }

  async getDocumentInnerMetadata(tx: Transaction): Promise<DocumentDataplyInnerMetadata> {
    const row = await this.select(1, false, tx)
    if (!row) {
      throw new Error('Document metadata not found')
    }
    return JSON.parse(row)
  }

  async updateDocumentInnerMetadata(metadata: DocumentDataplyInnerMetadata, tx: Transaction): Promise<void> {
    await this.update(1, JSON.stringify(metadata), tx)
  }

  /**
   * Transforms a query object into a verbose query object
   * @param query The query object to transform
   * @returns The verbose query object
   */
  verboseQuery<
    U extends Partial<DocumentDataplyIndexedQuery<T, IC>>,
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
            // FTS match 등 BPTree 조건이 아닌 연산자는 원본 그대로 보존
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

  /**
   * Get the selectivity candidate for the given query
   * @param query The query conditions
   * @param orderByField Optional field name for orderBy optimization
   * @returns Driver and other candidates for query execution
   */
  async getSelectivityCandidate<
    U extends Partial<DocumentDataplyIndexedQuery<T, IC>>,
    V extends DataplyTreeValue<U>
  >(
    query: Partial<DocumentDataplyQuery<V>>,
    orderByField?: string
  ): Promise<{
    driver: ({
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      isFtsMatch: false
    } | {
      tree: BPTreeAsync<string, V>
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      isFtsMatch: true,
      matchTokens: string[]
    }),
    others: ({
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      isFtsMatch: false
    } | {
      tree: BPTreeAsync<string, V>
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      isFtsMatch: true,
      matchTokens: string[]
    })[],
    rollback: () => void
  } | null> {
    const candidates: ({
      tree: BPTreeAsync<string | number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      isFtsMatch?: boolean,
      matchTokens?: string[]
    })[] = []
    const metadata = await this.getDocumentInnerMetadata(this.txContext.get()!)
    for (const field in query) {
      const tree = this.trees.get(field)
      if (!tree) continue
      const condition = query[field] as Partial<DocumentDataplyCondition<U>>
      const treeTx = await tree.createTransaction()
      const indexConfig = metadata.indices[field]?.[1]

      let isFtsMatch = false
      let matchTokens: string[] | undefined

      // Full Text Search
      if (
        typeof indexConfig === 'object' &&
        indexConfig?.type === 'fts' &&
        condition.match
      ) {
        isFtsMatch = true
        matchTokens = tokenize(condition.match as string, indexConfig)
      }

      candidates.push({
        tree: treeTx as unknown as BPTreeAsync<string | number, V>,
        condition,
        field,
        isFtsMatch,
        matchTokens
      })
    }

    const rollback = () => {
      for (const { tree } of candidates) {
        tree.rollback()
      }
    }

    // 쿼리에 조건이 있지만 해당 필드에 인덱스가 없는 경우
    if (candidates.length === 0) {
      rollback()
      return null
    }

    // orderBy 필드가 쿼리 조건에 포함된 경우만 해당 인덱스를 driver로 우선 선택
    if (orderByField) {
      const orderByCandidate = candidates.find(c => c.field === orderByField)
      if (orderByCandidate) {
        return {
          driver: orderByCandidate as any,
          others: candidates.filter(c => c.field !== orderByField) as any,
          rollback,
        }
      }
      // orderBy가 조건에 없으면 ChooseDriver로 선택도 기반 선택
    }

    // FTS match 후보 우선 처리 (match의 실질 score: 90)
    // equal/primaryEqual(100)이 다른 후보에 있을 때만 ChooseDriver에 위임
    const ftsCandidate = candidates.find(
      c => c.isFtsMatch && c.matchTokens && c.matchTokens.length > 0
    )
    if (ftsCandidate) {
      const hasHigherPriority = candidates.some(c => {
        if (c === ftsCandidate) return false
        const cond = c.condition
        return 'equal' in cond || 'primaryEqual' in cond
      })
      if (!hasHigherPriority) {
        return {
          driver: ftsCandidate as any,
          others: candidates.filter(c => c !== ftsCandidate) as any,
          rollback,
        }
      }
    }

    // 기존 로직: ChooseDriver 사용 (선택도 기반)
    let res = BPTreeAsync.ChooseDriver(candidates)
    if (!res && candidates.length > 0) {
      res = candidates[0]
    }
    if (!res) return null
    return {
      driver: res as any,
      others: candidates.filter(c => c.tree !== res!.tree) as any,
      rollback,
    }
  }

  /**
   * Get Free Memory Chunk Size
   * @returns { verySmallChunkSize, smallChunkSize }
   */
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

  private getTokenKey(pk: number, token: string): string {
    return pk + ':' + token
  }

  private async applyCandidateByFTS<V>(
    candidate: {
      tree: BPTreeAsync<string, DataplyTreeValue<V>>,
      condition: Partial<DocumentDataplyCondition<Partial<DocumentDataplyIndexedQuery<T, IC>>>>,
    },
    matchedTokens: string[],
    filterValues?: Set<number>,
    order?: 'asc' | 'desc'
  ): Promise<Set<number>> {
    const keys = new Set<number>()
    for (let i = 0, len = matchedTokens.length; i < len; i++) {
      const token = matchedTokens[i]
      const pairs = await candidate.tree.where(
        { primaryEqual: { v: token } } as any,
        {
          order,
        }
      )
      for (const pair of pairs.values()) {
        if (filterValues && !filterValues.has(pair.k)) continue
        keys.add(pair.k)
      }
    }
    return keys
  }

  /**
   * 특정 인덱스 후보를 조회하여 PK 집합을 필터링합니다.
   */
  private async applyCandidate<V>(
    candidate: {
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<Partial<DocumentDataplyIndexedQuery<T, IC>>>>,
    },
    filterValues?: Set<number>,
    order?: 'asc' | 'desc'
  ): Promise<Set<number>> {
    return await candidate.tree.keys(
      candidate.condition as any,
      {
        filterValues,
        order,
      }
    )
  }

  /**
   * 쿼리와 인덱스 선택을 기반으로 기본 키(Primary Keys)를 가져옵니다.
   * 쿼리 최적화를 통합하기 위한 내부 공통 메서드입니다.
   */
  async getKeys(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    orderBy?: keyof IC | '_id',
    sortOrder: 'asc' | 'desc' = 'asc'
  ): Promise<Float64Array> {
    // 1. 쿼리 정규화 및 최적화 후보 선택
    const isQueryEmpty = Object.keys(query).length === 0
    const normalizedQuery = isQueryEmpty ? { _id: { gte: 0 } } : query
    const selectivity = await this.getSelectivityCandidate(
      this.verboseQuery(normalizedQuery as any),
      orderBy as string
    )

    if (!selectivity) return new Float64Array(0)

    const { driver, others, rollback } = selectivity

    // 2. 실행 계획 결정
    // Driver 필드가 orderBy와 일치할 때만 인덱스 순서를 사용합니다.
    const useIndexOrder = orderBy === undefined || driver.field === orderBy
    const candidates = [driver, ...others]

    // 3. 모든 후보를 순회하며 필터링 수행
    let keys: Set<number> | undefined = undefined
    // Driver가 정렬 요건을 충족하면 전체 과정에서 정렬 순서를 유지하도록 sortOrder를 전달합니다.
    // 그렇지 않으면 트리 내부 정렬을 무시하도록(undefined) 처리합니다.
    for (let i = 0, len = candidates.length; i < len; i++) {
      const candidate = candidates[i]
      const currentOrder = useIndexOrder ? sortOrder : undefined
      if (
        candidate.isFtsMatch &&
        candidate.matchTokens &&
        candidate.matchTokens.length > 0
      ) {
        keys = await this.applyCandidateByFTS(
          candidate,
          candidate.matchTokens,
          keys,
          currentOrder
        )
      }
      else {
        keys = await this.applyCandidate(candidate as any, keys, currentOrder)
      }
    }

    rollback()
    return new Float64Array(Array.from(keys || []))
  }

  /**
   * 드라이버 인덱스만으로 PK를 가져옵니다. (교집합 없이)
   * selectDocuments에서 사용하며, 나머지 조건(others)은 스트리밍 중 tree.verify()로 검증합니다.
   * @returns 드라이버 키 배열, others 후보 목록, rollback 함수. 또는 null.
   */
  private async getDriverKeys(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    orderBy?: keyof IC | '_id',
    sortOrder: 'asc' | 'desc' = 'asc'
  ): Promise<{
    keys: Float64Array,
    others: {
      tree: BPTreeAsync<string | number, DataplyTreeValue<Primitive>>,
      condition: any,
      field: string,
      isFtsMatch: boolean,
      matchTokens?: string[]
    }[],
    rollback: () => void
  } | null> {
    const isQueryEmpty = Object.keys(query).length === 0
    const normalizedQuery = isQueryEmpty ? { _id: { gte: 0 } } : query
    const selectivity = await this.getSelectivityCandidate(
      this.verboseQuery(normalizedQuery as any),
      orderBy as string
    )

    if (!selectivity) return null

    const { driver, others, rollback } = selectivity

    // 드라이버의 정렬 순서 결정
    const useIndexOrder = orderBy === undefined || driver.field === orderBy
    const currentOrder = useIndexOrder ? sortOrder : undefined

    // 드라이버만으로 키를 가져옴
    let keys: Set<number>
    if (
      driver.isFtsMatch &&
      driver.matchTokens &&
      driver.matchTokens.length > 0
    ) {
      keys = await this.applyCandidateByFTS(
        driver as any,
        driver.matchTokens,
        undefined,
        currentOrder
      )
    }
    else {
      keys = await this.applyCandidate(driver as any, undefined, currentOrder)
    }

    return {
      keys: new Float64Array(Array.from(keys)),
      others: others as any,
      rollback,
    }
  }

  private async insertDocumentInternal(document: T, tx: Transaction): Promise<{
    pk: number
    id: number
    document: DataplyDocument<T>
  }> {
    const metadata = await this.getDocumentInnerMetadata(tx)
    const id = ++metadata.lastId
    await this.updateDocumentInnerMetadata(metadata, tx)
    const dataplyDocument: DataplyDocument<T> = Object.assign({
      _id: id,
    }, document)
    const pk = await super.insert(JSON.stringify(dataplyDocument), true, tx)
    return {
      pk,
      id,
      document: dataplyDocument
    }
  }

  /**
   * Insert a document into the database
   * @param document The document to insert
   * @param tx The transaction to use
   * @returns The primary key of the inserted document
   */
  async insertSingleDocument(document: T, tx?: Transaction): Promise<number> {
    return this.writeLock(() => this.runWithDefault(async (tx) => {
      const { pk: dpk, document: dataplyDocument } = await this.insertDocumentInternal(document, tx)
      const metadata = await this.getDocumentInnerMetadata(tx)
      const flattenDocument = this.flattenDocument(dataplyDocument)

      // Indexing
      for (const field in flattenDocument) {
        const tree = this.trees.get(field)
        if (!tree) continue
        const v = flattenDocument[field]
        const indexConfig = metadata.indices[field]?.[1]

        let tokens: string[] | Primitive[] = [v]
        const isFts = typeof indexConfig === 'object' && indexConfig?.type === 'fts' && typeof v === 'string'
        if (isFts) {
          tokens = tokenize(v, indexConfig)
        }

        for (let i = 0, len = tokens.length; i < len; i++) {
          const token = tokens[i]
          const keyToInsert = isFts ? this.getTokenKey(dpk, token as string) : dpk
          const [error] = await catchPromise(tree.insert(keyToInsert, { k: dpk, v: token }))
          if (error) {
            throw error
          }
        }
      }
      return dataplyDocument._id
    }, tx))
  }

  /**
   * Insert a batch of documents into the database
   * @param documents The documents to insert
   * @param tx The transaction to use
   * @returns The primary keys of the inserted documents
   */
  async insertBatchDocuments(documents: T[], tx?: Transaction): Promise<number[]> {
    return this.writeLock(() => this.runWithDefault(async (tx) => {
      // 1. Prepare Metadata and increment IDs in bulk
      const metadata = await this.getDocumentInnerMetadata(tx)
      const startId = metadata.lastId + 1
      metadata.lastId += documents.length
      await this.updateDocumentInnerMetadata(metadata, tx)

      const ids: number[] = []
      const dataplyDocuments: string[] = []
      const flattenedData: { pk: number, data: FlattenedDocumentJSON }[] = []

      // 2. Data Preparation Phase
      for (let i = 0, len = documents.length; i < len; i++) {
        const id = startId + i
        const dataplyDocument: DataplyDocument<T> = Object.assign({
          _id: id,
        }, documents[i])

        const stringified = JSON.stringify(dataplyDocument)
        dataplyDocuments.push(stringified)

        const flattenDocument = this.flattenDocument(dataplyDocument)
        flattenedData.push({ pk: -1, data: flattenDocument }) // PK will be filled after insertion

        ids.push(id)
      }

      // 3. Batch Data Insertion
      const pks = await super.insertBatch(dataplyDocuments, true, tx)

      // 4. Update PKs for indexing
      for (let i = 0, len = pks.length; i < len; i++) {
        flattenedData[i].pk = pks[i]
      }

      for (const [field, tree] of this.trees) {
        const treeTx = await tree.createTransaction()
        const indexConfig = metadata.indices[field]?.[1]

        const batchInsertData: [string | number, DataplyTreeValue<Primitive>][] = []
        for (let i = 0, len = flattenedData.length; i < len; i++) {
          const item = flattenedData[i]
          const v = item.data[field]
          if (v === undefined) continue

          const isFts = typeof indexConfig === 'object' && indexConfig?.type === 'fts' && typeof v === 'string'
          let tokens: string[] | Primitive[] = [v]
          if (isFts) {
            tokens = tokenize(v, indexConfig)
          }
          for (let j = 0, len = tokens.length; j < len; j++) {
            const token = tokens[j]
            const keyToInsert = isFts ? this.getTokenKey(item.pk, token as string) : item.pk
            batchInsertData.push([keyToInsert, { k: item.pk, v: token }])
          }
        }
        const [error] = await catchPromise(treeTx.batchInsert(batchInsertData))
        if (error) {
          throw error
        }
        const res = await treeTx.commit()
        if (!res.success) {
          throw (res as any).error
        }
      }
      return ids
    }, tx))
  }

  /**
   * Internal update method used by both fullUpdate and partialUpdate
   * @param query The query to use
   * @param computeUpdatedDoc Function that computes the updated document from the original
   * @param tx The transaction to use
   * @returns The number of updated documents
   */
  private async updateInternal(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    computeUpdatedDoc: (doc: DataplyDocument<T>) => DataplyDocument<T>,
    tx: Transaction
  ): Promise<number> {
    // 1. 대상 PK 목록 확보 (인덱스 전용 검색 최적화)
    const pks = await this.getKeys(query)
    let updatedCount = 0

    const treeTxs = new Map<string, BPTreeAsyncTransaction<string | number, DataplyTreeValue<any>>>()
    for (const [field, tree] of this.trees) {
      treeTxs.set(field, await tree.createTransaction())
    }
    treeTxs.delete('_id')

    for (let i = 0, len = pks.length; i < len; i++) {
      const pk = pks[i]
      // 1.1 문서 직접 로드 (PK 알고 있으므로 인덱스 재검색 불필요)
      const doc = await this.getDocument(pk, tx)
      if (!doc) continue

      // 1.2 새 문서 계산
      const updatedDoc = computeUpdatedDoc(doc)
      const oldFlatDoc = this.flattenDocument(doc)
      const newFlatDoc = this.flattenDocument(updatedDoc)

      // 1.3 변경된 인덱스 필드 동기화
      const metadata = await this.getDocumentInnerMetadata(tx)
      for (const [field, treeTx] of treeTxs) {
        const oldV = oldFlatDoc[field]
        const newV = newFlatDoc[field]

        if (oldV === newV) continue

        const indexConfig = metadata.indices[field]?.[1]
        const isFts = typeof indexConfig === 'object' && indexConfig?.type === 'fts'

        // 기존 값 토큰 삭제
        if (field in oldFlatDoc) {
          let oldTokens: string[] | Primitive[] = [oldV]
          if (isFts && typeof oldV === 'string') {
            oldTokens = tokenize(oldV, indexConfig)
          }
          for (let j = 0, len = oldTokens.length; j < len; j++) {
            const oldToken = oldTokens[j]
            const keyToDelete = isFts ? this.getTokenKey(pk, oldToken as string) : pk
            await treeTx.delete(keyToDelete, { k: pk, v: oldToken })
          }
        }

        // 새 값 토큰 삽입
        if (field in newFlatDoc) {
          let newTokens: string[] | Primitive[] = [newV]
          if (isFts && typeof newV === 'string') {
            newTokens = tokenize(newV, indexConfig)
          }

          const batchInsertData: [string | number, DataplyTreeValue<Primitive>][] = []
          for (let j = 0, len = newTokens.length; j < len; j++) {
            const newToken = newTokens[j]
            const keyToInsert = isFts ? this.getTokenKey(pk, newToken as string) : pk
            batchInsertData.push([keyToInsert, { k: pk, v: newToken }])
          }
          await treeTx.batchInsert(batchInsertData)
        }
      }

      // 1.4 실제 레코드 업데이트
      await this.update(pk, JSON.stringify(updatedDoc), tx)
      updatedCount++
    }

    for (const [field, treeTx] of treeTxs) {
      const result = await treeTx.commit()
      if (!result.success) {
        for (const rollbackTx of treeTxs.values()) {
          rollbackTx.rollback()
        }
        throw (result as any).error
      }
    }

    return updatedCount
  }

  /**
   * Fully update documents from the database that match the query
   * @param query The query to use (only indexed fields + _id allowed)
   * @param newRecord Complete document to replace with, or function that receives current document and returns new document
   * @param tx The transaction to use
   * @returns The number of updated documents
   */
  async fullUpdate(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    newRecord: T | ((document: DataplyDocument<T>) => T),
    tx?: Transaction
  ): Promise<number> {
    return await this.writeLock(() => this.runWithDefault(async (tx) => {
      return this.updateInternal(query, (doc) => {
        const newDoc = typeof newRecord === 'function'
          ? (newRecord as Function)(doc)
          : newRecord
        // _id 보존
        return { _id: doc._id, ...newDoc } as DataplyDocument<T>
      }, tx)
    }, tx))
  }

  /**
   * Partially update documents from the database that match the query
   * @param query The query to use (only indexed fields + _id allowed)
   * @param newRecord Partial document to merge, or function that receives current document and returns partial update
   * @param tx The transaction to use
   * @returns The number of updated documents
   */
  async partialUpdate(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    newRecord: Partial<DataplyDocument<T>> | ((document: DataplyDocument<T>) => Partial<DataplyDocument<T>>),
    tx?: Transaction
  ): Promise<number> {
    return this.writeLock(() => this.runWithDefault(async (tx) => {
      return this.updateInternal(query, (doc) => {
        const partialUpdateContent = typeof newRecord === 'function'
          ? (newRecord as Function)(doc)
          : newRecord
        // _id는 업데이트하지 않음
        const finalUpdate = { ...partialUpdateContent }
        delete (finalUpdate as any)._id
        // 기존 문서 + 부분 업데이트
        return { ...doc, ...finalUpdate } as DataplyDocument<T>
      }, tx)
    }, tx))
  }

  /**
   * Delete documents from the database that match the query
   * @param query The query to use (only indexed fields + _id allowed)
   * @param tx The transaction to use
   * @returns The number of deleted documents
   */
  async deleteDocuments(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    tx?: Transaction
  ): Promise<number> {
    return this.writeLock(() => this.runWithDefault(async (tx) => {
      // 1. 삭제할 대상 PK 목록 확보
      const pks = await this.getKeys(query)
      let deletedCount = 0

      for (let i = 0, len = pks.length; i < len; i++) {
        const pk = pks[i]
        // 1.1 문서 정보 확보 (인덱스 삭제를 위함)
        const doc = await this.getDocument(pk, tx)
        if (!doc) continue

        const flatDoc = this.flattenDocument(doc)

        const metadata = await this.getDocumentInnerMetadata(tx)

        // 1.2 모든 인덱스 트리에서 삭제
        for (const [field, tree] of this.trees) {
          const v = flatDoc[field]
          if (v === undefined) continue

          const indexConfig = metadata.indices[field]?.[1]
          const isFts = typeof indexConfig === 'object' && indexConfig?.type === 'fts' && typeof v === 'string'
          let tokens: string[] | Primitive[] = [v]
          if (isFts) {
            tokens = tokenize(v, indexConfig)
          }
          for (let j = 0, len = tokens.length; j < len; j++) {
            const token = tokens[j]
            const keyToDelete = isFts ? this.getTokenKey(pk, token as string) : pk
            await tree.delete(keyToDelete, { k: pk, v: token })
          }
        }

        // 1.3 실제 레코드 삭제
        await super.delete(pk, true, tx)
        deletedCount++
      }

      return deletedCount
    }, tx))
  }

  /**
   * Count documents from the database that match the query
   * @param query The query to use (only indexed fields + _id allowed)
   * @param tx The transaction to use
   * @returns The number of documents that match the query
   */
  async countDocuments(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    tx?: Transaction
  ): Promise<number> {
    return this.readLock(() => this.runWithDefault(async (tx) => {
      const pks = await this.getKeys(query)
      return pks.length
    }, tx))
  }

  /**
   * FTS 조건에 대해 문서가 유효한지 검증합니다.
   */
  private verifyFts(
    doc: DataplyDocument<T>,
    ftsConditions: { field: string, matchTokens: string[] }[]
  ): boolean {
    const flatDoc = this.flattenDocument(doc)
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

  /**
   * 메모리 기반으로 청크 크기를 동적 조절합니다.
   */
  private adjustChunkSize(currentChunkSize: number, chunkTotalSize: number): number {
    if (chunkTotalSize <= 0) return currentChunkSize
    const { verySmallChunkSize, smallChunkSize } = this.getFreeMemoryChunkSize()
    if (chunkTotalSize < verySmallChunkSize) return currentChunkSize * 2
    if (chunkTotalSize > smallChunkSize) return Math.max(Math.floor(currentChunkSize / 2), 20)
    return currentChunkSize
  }

  /**
   * Prefetch 방식으로 키 배열을 청크 단위로 조회하여 문서를 순회합니다.
   * FTS 검증 및 others 후보에 대한 tree.verify() 검증을 통과한 문서만 yield 합니다.
   * 교집합 대신 스트리밍 중 검증하여 첫 결과 반환 시간을 단축합니다.
   */
  private async *processChunkedKeysWithVerify(
    keys: Float64Array,
    startIdx: number,
    initialChunkSize: number,
    ftsConditions: { field: string, matchTokens: string[] }[],
    others: {
      tree: BPTreeAsync<string | number, DataplyTreeValue<Primitive>>,
      condition: any,
      field: string,
      isFtsMatch: boolean,
      matchTokens?: string[]
    }[],
    tx: any
  ): AsyncGenerator<DataplyDocument<T>> {
    // others 중 FTS가 아닌 일반 조건만 verify 대상으로 분리
    const verifyOthers = others.filter(o => !o.isFtsMatch)

    let i = startIdx
    const totalKeys = keys.length
    let currentChunkSize = initialChunkSize

    // 첫 번째 청크 prefetch
    let nextChunkPromise: Promise<(string | null)[]> | null = null
    if (i < totalKeys) {
      const endIdx = Math.min(i + currentChunkSize, totalKeys)
      nextChunkPromise = this.selectMany(keys.subarray(i, endIdx), false, tx)
      i = endIdx
    }

    while (nextChunkPromise) {
      const rawResults = await nextChunkPromise
      nextChunkPromise = null

      // 다음 청크 prefetch
      if (i < totalKeys) {
        const endIdx = Math.min(i + currentChunkSize, totalKeys)
        nextChunkPromise = this.selectMany(keys.subarray(i, endIdx), false, tx)
        i = endIdx
      }

      let chunkTotalSize = 0
      for (let j = 0, len = rawResults.length; j < len; j++) {
        const s = rawResults[j]
        if (!s) continue
        const doc = JSON.parse(s)
        chunkTotalSize += s.length * 2

        // FTS 검증
        if (ftsConditions.length > 0 && !this.verifyFts(doc, ftsConditions)) continue

        // others 조건 검증: 각 필드의 값을 tree.verify()로 확인
        if (verifyOthers.length > 0) {
          const flatDoc = this.flattenDocument(doc)
          let passed = true
          for (let k = 0, kLen = verifyOthers.length; k < kLen; k++) {
            const other = verifyOthers[k]
            const fieldValue = flatDoc[other.field]
            if (fieldValue === undefined) {
              passed = false
              break
            }
            // tree.verify()에 전달할 값 구성: { k: pk, v: fieldValue }
            const treeValue: DataplyTreeValue<Primitive> = { k: doc._id, v: fieldValue }
            if (!other.tree.verify(treeValue, other.condition)) {
              passed = false
              break
            }
          }
          if (!passed) continue
        }

        yield doc
      }

      currentChunkSize = this.adjustChunkSize(currentChunkSize, chunkTotalSize)
    }
  }

  /**
   * Select documents from the database
   * @param query The query to use (only indexed fields + _id allowed)
   * @param options The options to use
   * @param tx The transaction to use
   * @returns The documents that match the query
   * @throws Error if query or orderBy contains non-indexed fields
   */
  selectDocuments(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    options: DocumentDataplyQueryOptions<T, IC> = {},
    tx?: Transaction
  ): {
    stream: AsyncIterableIterator<DataplyDocument<T>>
    drain: () => Promise<DataplyDocument<T>[]>
  } {
    // 런타임 검증: 쿼리 필드가 인덱스된 필드인지 확인
    for (const field of Object.keys(query)) {
      if (!this.indexedFields.has(field)) {
        throw new Error(`Query field "${field}" is not indexed. Available indexed fields: ${Array.from(this.indexedFields).join(', ')}`)
      }
    }

    // 런타임 검증: orderBy 필드가 인덱스된 필드인지 확인
    const orderBy = options.orderBy
    if (orderBy !== undefined && !this.indexedFields.has(orderBy as string)) {
      throw new Error(`orderBy field "${orderBy}" is not indexed. Available indexed fields: ${Array.from(this.indexedFields).join(', ')}`)
    }

    // 옵션 기본값 설정
    const {
      limit = Infinity,
      offset = 0,
      sortOrder = 'asc',
      orderBy: orderByField
    } = options

    const self = this
    const stream = this.streamWithDefault(async function* (tx) {
      // FTS(전문 검색) 조건 수집: match 연산자가 있는 필드의 토큰을 추출
      const metadata = await self.getDocumentInnerMetadata(tx)
      const ftsConditions: { field: string, matchTokens: string[] }[] = []
      for (const field in query) {
        const q = query[field] as any
        if (
          q &&
          typeof q === 'object' &&
          'match' in q &&
          typeof q.match === 'string'
        ) {
          const indexConfig = metadata.indices[field]?.[1]
          if (typeof indexConfig === 'object' && indexConfig?.type === 'fts') {
            ftsConditions.push({ field, matchTokens: tokenize(q.match, indexConfig) })
          }
        }
      }

      // 드라이버 인덱스만으로 PK 목록 조회 (교집합 없이)
      const driverResult = await self.getDriverKeys(query, orderByField, sortOrder)
      if (!driverResult) return
      const { keys, others, rollback } = driverResult
      if (keys.length === 0) {
        rollback()
        return
      }

      // Driver 인덱스가 orderBy 필드와 일치하는지 판별하여 정렬 전략 결정
      const isQueryEmpty = Object.keys(query).length === 0
      const normalizedQuery = isQueryEmpty ? { _id: { gte: 0 } } : query
      const selectivity = await self.getSelectivityCandidate(
        self.verboseQuery(normalizedQuery as any),
        orderByField as string
      )
      const isDriverOrderByField = (
        orderByField === undefined ||
        (selectivity && selectivity.driver.field === orderByField)
      )
      if (selectivity) selectivity.rollback()

      try {
        // ────────────────────────────────────────────────
        // 경로 1: 메모리 내 정렬 (driver가 orderBy를 커버하지 못하는 경우)
        // 전체 문서를 수집한 후 orderBy 기준으로 정렬하여 반환합니다.
        // ────────────────────────────────────────────────
        if (!isDriverOrderByField && orderByField) {
          // offset + limit 만큼만 유지하면 되므로, 힙 크기를 topK로 제한
          const topK = limit === Infinity ? Infinity : offset + limit
          let heap: BinaryHeap<DataplyDocument<T>> | null = null

          // topK가 유한할 때만 최대 힙을 사용하여 상위 K개만 유지
          if (topK !== Infinity) {
            heap = new BinaryHeap((a: DataplyDocument<T>, b: DataplyDocument<T>) => {
              const aVal = (a as any)[orderByField] ?? (a as any)._id
              const bVal = (b as any)[orderByField] ?? (b as any)._id
              const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0
              // 힙의 루트가 가장 "나쁜" 값이 되도록 역순 비교
              return sortOrder === 'asc' ? -cmp : cmp
            })
          }

          // topK가 무한대인 경우 모든 문서를 배열에 수집
          const results: DataplyDocument<T>[] = []
          for await (const doc of self.processChunkedKeysWithVerify(
            keys,
            0,
            self.options.pageSize,
            ftsConditions,
            others,
            tx
          )) {
            if (heap) {
              // 힙이 아직 topK개 미만이면 무조건 추가
              if (heap.size < topK) heap.push(doc)
              else {
                // 현재 문서가 힙의 루트(최악)보다 나으면 교체
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

          // 최종 정렬: 힙 또는 배열의 문서를 orderBy 기준으로 안정 정렬
          const finalDocs = heap ? heap.toArray() : results
          finalDocs.sort((a, b) => {
            const aVal = (a as any)[orderByField] ?? (a as any)._id
            const bVal = (b as any)[orderByField] ?? (b as any)._id
            const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0
            return sortOrder === 'asc' ? cmp : -cmp
          })

          // offset/limit 적용 후 결과 반환
          const end = limit === Infinity ? undefined : offset + limit
          const limitedResults = finalDocs.slice(offset, end)
          for (let j = 0, len = limitedResults.length; j < len; j++) {
            yield limitedResults[j]
          }
        }
        // ────────────────────────────────────────────────
        // 경로 2: 순차 스트리밍 (driver가 orderBy를 커버하는 경우)
        // 인덱스 순서를 그대로 활용하여 offset부터 limit개를 순차 반환합니다.
        // ────────────────────────────────────────────────
        else {
          let yieldedCount = 0
          // offset부터 시작하여 limit개까지만 yield
          for await (const doc of self.processChunkedKeysWithVerify(
            keys,
            offset,
            self.options.pageSize,
            ftsConditions,
            others,
            tx
          )) {
            if (yieldedCount >= limit) break
            yield doc
            yieldedCount++
          }
        }
      }
      finally {
        // others 후보 트랜잭션 정리
        rollback()
      }
    }, tx)

    // drain: 스트림의 모든 결과를 배열로 수집하여 반환하는 편의 함수
    const drain = async () => {
      const result: DataplyDocument<T>[] = []
      for await (const document of stream) {
        result.push(document)
      }
      return result
    }
    return { stream, drain }
  }
}
