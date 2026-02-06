import type {
  DataplyTreeValue,
  DocumentDataplyInnerMetadata,
  DocumentDataplyOptions,
  DocumentJSON,
  FlattenedDocumentJSON,
  Primitive,
  DocumentDataplyQuery,
  DocumentDataplyIndexedQuery,
  FinalFlatten,
  DocumentDataplyCondition,
  DataplyDocument,
  DocumentDataplyMetadata,
  DocumentDataplyQueryOptions,
  IndexConfig
} from '../types'
import {
  type BPTreeCondition,
  DataplyAPI,
  Transaction,
  BPTreeAsync,
  BPTreeAsyncTransaction,
  Ryoiki
} from 'dataply'
import { DocumentSerializeStrategyAsync } from './bptree/documentStrategy'
import { DocumentValueComparator } from './bptree/documentComparator'
import { catchPromise } from '../utils/catchPromise'

export class DocumentDataplyAPI<T extends DocumentJSON, IC extends IndexConfig<T>> extends DataplyAPI {
  declare runWithDefault
  declare streamWithDefault

  indices: DocumentDataplyInnerMetadata['indices'] = {}
  readonly trees: Map<string, BPTreeAsync<number, DataplyTreeValue<Primitive>>> = new Map()
  readonly comparator = new DocumentValueComparator()
  private pendingBackfillFields: string[] = []
  private readonly lock: Ryoiki

  constructor(file: string, options: DocumentDataplyOptions<T, IC>) {
    super(file, options)
    this.trees = new Map()
    this.lock = new Ryoiki()
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
          metadata.indices[field] = [-1, isBackfillEnabled]
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
            metadata.indices[field][1] = true
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
      this.lock.readUnlock(lockId)
    })
  }

  async writeLock<T>(fn: () => T): Promise<T> {
    let lockId: string
    return this.lock.writeLock(async (_lockId) => {
      lockId = _lockId
      return await fn()
    }).finally(() => {
      this.lock.writeUnlock(lockId)
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
      const fieldTxMap: Record<string, BPTreeAsyncTransaction<number, DataplyTreeValue<Primitive>>> = {}
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
        const doc = await this.getDocument(k, tx)
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
          await btx.insert(k, { k, v })
        }
        backfilledCount++
      }

      // 모든 트랜잭션 커밋
      const commits = Object.values(fieldTxMap).map(btx => btx.commit())
      await Promise.all(commits).catch(async (err) => {
        const rollbacks = Object.values(fieldTxMap).map(btx => btx.rollback())
        await Promise.all(rollbacks)
        throw err
      })

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

  /**
   * returns flattened document
   * @param document 
   * @returns 
   */
  flattenDocument(document: T): FlattenedDocumentJSON {
    const result: FlattenedDocumentJSON = {}
    const flatten = (obj: any, parentKey: string = '') => {
      for (const key in obj) {
        const newKey = parentKey ? `${parentKey}.${key}` : key
        if (typeof obj[key] === 'object' && obj[key] !== null) {
          flatten(obj[key], newKey)
        } else {
          result[newKey] = obj[key]
        }
      }
    }
    flatten(document)
    return result
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
}

export class DocumentDataply<T extends DocumentJSON, IC extends IndexConfig<T>> {
  /**
   * Starts the database definition by setting the document type.
   * This is used to ensure TypeScript type inference works correctly for the document structure.
   * @template T The structure of the document to be stored.
   */
  static Define<T extends DocumentJSON>() {
    return {
      /**
       * Sets the options for the database, such as index configurations and WAL settings.
       * @template IC The configuration of indices.
       * @param options The database initialization options.
       */
      Options: <IC extends IndexConfig<T>>(
        options: DocumentDataplyOptions<T, IC>
      ) => DocumentDataply.Options<T, IC>(options)
    }
  }

  /**
   * Internal method used by the Define-chain to pass options.
   */
  private static Options<T extends DocumentJSON, IC extends IndexConfig<T>>(
    options: DocumentDataplyOptions<T, IC>
  ) {
    return {
      /**
       * Creates or opens the database instance with the specified file path.
       * @param file The path to the database file.
       */
      Open: (file: string) => DocumentDataply.Open<T, IC>(file, options)
    }
  }

  /**
   * Internal method used to finalize construction and create the instance.
   */
  private static Open<T extends DocumentJSON, IC extends IndexConfig<T>>(
    file: string,
    options: DocumentDataplyOptions<T, IC>
  ) {
    return new DocumentDataply<T, IC>(file, options)
  }

  protected readonly api: DocumentDataplyAPI<T, IC>
  private readonly indexedFields: Set<string>
  private readonly operatorConverters: Record<
    keyof DocumentDataplyCondition<FinalFlatten<T>>,
    keyof BPTreeCondition<FinalFlatten<T>>
  > = {
      equal: 'primaryEqual',
      notEqual: 'primaryNotEqual',
      lt: 'primaryLt',
      lte: 'primaryLte',
      gt: 'primaryGt',
      gte: 'primaryGte',
      or: 'primaryOr',
      like: 'like',
    }

  protected constructor(file: string, options?: DocumentDataplyOptions<T, IC>) {
    this.api = new DocumentDataplyAPI(file, options ?? {} as any)
    // indices에 지정된 필드들을 저장 (_id는 항상 포함)
    this.indexedFields = new Set(['_id'])
    if (options?.indices) {
      for (const field of Object.keys(options.indices)) {
        this.indexedFields.add(field)
      }
    }
  }

  /**
   * Initialize the document database
   */
  async init(): Promise<void> {
    await this.api.init()
    await this.api.backfillIndices()
  }

  /**
   * Get the metadata of the document database
   */
  async getMetadata(tx?: Transaction): Promise<DocumentDataplyMetadata> {
    return this.api.runWithDefault((tx) => this.api.getDocumentMetadata(tx), tx)
  }

  /**
   * Create a transaction
   */
  createTransaction(): Transaction {
    return this.api.createTransaction()
  }

  private verboseQuery<
    U extends Partial<DocumentDataplyIndexedQuery<T, IC>>,
    V extends DataplyTreeValue<U>
  >(
    query: Partial<DocumentDataplyQuery<U>>
  ): Partial<DocumentDataplyQuery<V>> {
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
          const after = this.operatorConverters[before]
          const v = conditions[before]
          if (!after) continue
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
    driver: {
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string
    },
    others: {
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string
    }[],
    rollback: () => void
  } | null> {
    const candidates: {
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string
    }[] = []
    for (const field in query) {
      const tree = this.api.trees.get(field)
      if (!tree) continue
      const condition = query[field] as Partial<DocumentDataplyCondition<U>>
      const treeTx = await tree.createTransaction()
      candidates.push({ tree: treeTx as unknown as BPTreeAsync<number, V>, condition, field })
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
          driver: orderByCandidate,
          others: candidates.filter(c => c.field !== orderByField),
          rollback,
        }
      }
      // orderBy가 조건에 없으면 ChooseDriver로 선택도 기반 선택
    }

    // 기존 로직: ChooseDriver 사용 (선택도 기반)
    let res = BPTreeAsync.ChooseDriver(candidates)
    if (!res && candidates.length > 0) {
      res = candidates[0]
    }
    if (!res) return null
    return {
      driver: res as {
        tree: BPTreeAsync<number, V>,
        condition: Partial<DocumentDataplyCondition<U>>,
        field: string
      },
      others: candidates.filter(c => c.tree !== res!.tree) as {
        tree: BPTreeAsync<number, V>,
        condition: Partial<DocumentDataplyCondition<U>>,
        field: string
      }[],
      rollback,
    }
  }

  /**
   * Get Primary Keys based on query and index selection.
   * Internal common method to unify query optimization.
   */
  private async getKeys(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    orderBy?: keyof IC | '_id',
    sortOrder: 'asc' | 'desc' = 'asc'
  ): Promise<Set<number>> {
    const isQueryEmpty = Object.keys(query).length === 0
    const normalizedQuery = isQueryEmpty
      ? { _id: { gte: 0 } } as unknown as typeof query
      : query

    const verbose = this.verboseQuery(normalizedQuery)
    const selectivity = await this.getSelectivityCandidate(
      verbose,
      orderBy as string,
    )

    if (!selectivity) return new Set<number>()

    const { driver, others, rollback } = selectivity
    const isDriverOrderByField = orderBy === undefined || driver.field === orderBy

    // Case 1: Driver matches orderBy (or no orderBy) -> Straight from B+Tree
    if (isDriverOrderByField) {
      let keys = await driver.tree.keys(driver.condition as any, undefined, sortOrder)
      for (const { tree, condition } of others) {
        keys = await tree.keys(condition as any, keys, sortOrder)
      }
      rollback()
      return keys
    }
    // Case 2: Driver is different -> No specific order guaranteed from trees
    else {
      let keys = await driver.tree.keys(driver.condition as any, undefined)
      for (const { tree, condition } of others) {
        keys = await tree.keys(condition as any, keys)
      }
      rollback()
      return keys
    }
  }

  private async insertDocument(document: T, tx: Transaction): Promise<{
    pk: number
    id: number
    document: DataplyDocument<T>
  }> {
    const metadata = await this.api.getDocumentInnerMetadata(tx)
    const id = ++metadata.lastId
    await this.api.updateDocumentInnerMetadata(metadata, tx)
    const dataplyDocument: DataplyDocument<T> = Object.assign({
      _id: id,
    }, document)
    const pk = await this.api.insert(JSON.stringify(dataplyDocument), true, tx)
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
  async insert(document: T, tx?: Transaction): Promise<number> {
    return this.api.writeLock(() => this.api.runWithDefault(async (tx) => {
      const { pk, document: dataplyDocument } = await this.insertDocument(document, tx)
      const flattenDocument = this.api.flattenDocument(dataplyDocument)
      // Indexing
      for (const field in flattenDocument) {
        const tree = this.api.trees.get(field)
        if (!tree) continue
        const v = flattenDocument[field]
        const [error] = await catchPromise(tree.insert(pk, { k: pk, v }))
        if (error) {
          console.error(`BPTree indexing failed for field: ${field}`, error)
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
  async insertBatch(documents: T[], tx?: Transaction): Promise<number[]> {
    return this.api.writeLock(() => this.api.runWithDefault(async (tx) => {
      // 1. Prepare Metadata and increment IDs in bulk
      const metadata = await this.api.getDocumentInnerMetadata(tx)
      const startId = metadata.lastId + 1
      metadata.lastId += documents.length
      await this.api.updateDocumentInnerMetadata(metadata, tx)

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

        const flattenDocument = this.api.flattenDocument(dataplyDocument)
        flattenedData.push({ pk: -1, data: flattenDocument }) // PK will be filled after insertion

        ids.push(id)
      }

      // 3. Batch Data Insertion
      const pks = await this.api.insertBatch(dataplyDocuments, true, tx)

      // 4. Update PKs for indexing
      for (let i = 0, len = pks.length; i < len; i++) {
        flattenedData[i].pk = pks[i]
      }

      // 5. Indexing Phase (Grouped by field)
      for (const [field, tree] of this.api.trees) {
        const treeTx = await tree.createTransaction()
        for (let i = 0, len = flattenedData.length; i < len; i++) {
          const item = flattenedData[i]
          const v = item.data[field]
          if (v === undefined) continue
          const [error] = await catchPromise(treeTx.insert(item.pk, { k: item.pk, v }))
          if (error) {
            console.error(`BPTree indexing failed for field: ${field}`, error)
          }
        }
        const res = await treeTx.commit()
        if (!res.success) {
          throw res.error
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

    const treeTxs = new Map<string, BPTreeAsyncTransaction<number, DataplyTreeValue<any>>>()
    for (const [field, tree] of this.api.trees) {
      treeTxs.set(field, await tree.createTransaction())
    }
    treeTxs.delete('_id')

    for (const pk of pks) {
      // 1.1 문서 직접 로드 (PK 알고 있으므로 인덱스 재검색 불필요)
      const doc = await this.api.getDocument(pk, tx)
      if (!doc) continue

      // 1.2 새 문서 계산
      const updatedDoc = computeUpdatedDoc(doc)
      const oldFlatDoc = this.api.flattenDocument(doc)
      const newFlatDoc = this.api.flattenDocument(updatedDoc)

      // 1.3 변경된 인덱스 필드 동기화
      for (const [field, treeTx] of treeTxs) {
        const oldV = oldFlatDoc[field]
        const newV = newFlatDoc[field]

        if (oldV === newV) continue

        // 기존 값 삭제
        if (field in oldFlatDoc) {
          await treeTx.delete(pk, { k: pk, v: oldV })
        }
        // 새 값 삽입
        if (field in newFlatDoc) {
          await treeTx.insert(pk, { k: pk, v: newV })
        }
      }

      // 1.4 실제 레코드 업데이트
      await this.api.update(pk, JSON.stringify(updatedDoc), tx)
      updatedCount++
    }

    for (const [field, treeTx] of treeTxs) {
      const result = await treeTx.commit()
      if (!result.success) {
        for (const rollbackTx of treeTxs.values()) {
          rollbackTx.rollback()
        }
        throw result.error
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
    return await this.api.writeLock(() => this.api.runWithDefault(async (tx) => {
      return this.updateInternal(query, (doc) => {
        const newDoc = typeof newRecord === 'function'
          ? newRecord(doc)
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
    return this.api.writeLock(() => this.api.runWithDefault(async (tx) => {
      return this.updateInternal(query, (doc) => {
        const partialUpdate = typeof newRecord === 'function'
          ? newRecord(doc)
          : newRecord
        // _id는 업데이트하지 않음
        delete (partialUpdate as any)._id
        // 기존 문서 + 부분 업데이트
        return { ...doc, ...partialUpdate } as DataplyDocument<T>
      }, tx)
    }, tx))
  }

  /**
   * Delete documents from the database that match the query
   * @param query The query to use (only indexed fields + _id allowed)
   * @param tx The transaction to use
   * @returns The number of deleted documents
   */
  async delete(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    tx?: Transaction
  ): Promise<number> {
    return this.api.writeLock(() => this.api.runWithDefault(async (tx) => {
      // 1. 삭제할 대상 PK 목록 확보
      const pks = await this.getKeys(query)
      let deletedCount = 0

      for (const pk of pks) {
        // 1.1 문서 정보 확보 (인덱스 삭제를 위함)
        const doc = await this.api.getDocument(pk, tx)
        if (!doc) continue

        const flatDoc = this.api.flattenDocument(doc)

        // 1.2 모든 인덱스 트리에서 삭제
        for (const [field, tree] of this.api.trees) {
          const v = flatDoc[field]
          if (v === undefined) continue
          await tree.delete(pk, { k: pk, v })
        }

        // 1.3 실제 레코드 삭제
        await this.api.delete(pk, true, tx)
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
  async count(
    query: Partial<DocumentDataplyIndexedQuery<T, IC>>,
    tx?: Transaction
  ): Promise<number> {
    return this.api.readLock(() => this.api.runWithDefault(async (tx) => {
      const pks = await this.getKeys(query)
      return pks.size
    }, tx))
  }

  /**
   * Select documents from the database
   * @param query The query to use (only indexed fields + _id allowed)
   * @param options The options to use
   * @param tx The transaction to use
   * @returns The documents that match the query
   * @throws Error if query or orderBy contains non-indexed fields
   */
  select(
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

    const {
      limit = Infinity,
      offset = 0,
      sortOrder = 'asc',
      orderBy: orderByField
    } = options

    const self = this
    const stream = this.api.streamWithDefault(async function* (tx) {
      const keys = await self.getKeys(query, orderByField, sortOrder)

      // Case: Needs in-memory sorting
      const selectivity = await self.getSelectivityCandidate(
        self.verboseQuery(query),
        orderByField
      )
      const isDriverOrderByField = (
        orderByField === undefined ||
        selectivity && selectivity.driver.field === orderByField
      )

      if (selectivity) {
        selectivity.rollback()
      }

      // orderBy가 주어졌거나, driver가 orderBy와 일치하지 않은 경우 인메모리 정렬 필요
      if (!isDriverOrderByField && orderByField) {
        const results: DataplyDocument<T>[] = []
        for (const key of keys) {
          const stringified = await self.api.select(key, false, tx)
          if (!stringified) continue
          results.push(JSON.parse(stringified))
        }

        // 정렬: orderBy 필드로 정렬 (인덱스 없으면 문서 필드로 직접 정렬)
        results.sort((a, b) => {
          const aVal = (a as any)[orderByField] ?? (a as any)._id
          const bVal = (b as any)[orderByField] ?? (b as any)._id
          const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0
          return sortOrder === 'asc' ? cmp : -cmp
        })

        // limit & offset 적용 후 yield
        const start = offset
        const end = limit === Infinity ? undefined : start + limit
        const limitedResults = results.slice(start, end)
        for (const doc of limitedResults) {
          yield doc
        }
      }
      // driver가 orderBy와 일치하거나, orderBy가 없는 경우 정렬 불필요
      else {
        let i = 0
        let yieldedCount = 0
        for (const key of keys) {
          if (yieldedCount >= limit) break
          if (i < offset) {
            i++
            continue
          }
          const stringified = await self.api.select(key, false, tx)
          if (!stringified) continue
          yield JSON.parse(stringified)
          yieldedCount++
          i++
        }
      }
    }, tx)
    const drain = async () => {
      const result: DataplyDocument<T>[] = []
      for await (const document of stream) {
        result.push(document)
      }
      return result
    }
    return { stream, drain }
  }

  /**
   * Close the document database
   */
  async close(): Promise<void> {
    await this.api.close()
  }
}
