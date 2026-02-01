import type { DataplyTreeValue, DocumentDataplyInnerMetadata, DocumentDataplyOptions, DocumentJSON, FlattenedDocumentJSON, Primitive, DocumentDataplyQuery, FinalFlatten, DocumentDataplyCondition, DataplyDocument, DocumentDataplyMetadata } from '../types'
import { DataplyAPI, Transaction, BPTreeAsync, type BPTreeCondition, BPTreeAsyncTransaction, Ryoiki } from 'dataply'
import { DocumentSerializeStrategyAsync } from './bptree/documentStrategy'
import { DocumentValueComparator } from './bptree/documentComparator'
import { catchPromise } from '../utils/catchPromise'

export class DocumentDataplyAPI<T extends DocumentJSON> extends DataplyAPI {
  declare runWithDefault

  indecies: DocumentDataplyInnerMetadata['indecies'] = {}
  readonly trees: Map<string, BPTreeAsync<number, DataplyTreeValue<Primitive>>> = new Map()
  readonly comparator = new DocumentValueComparator()
  private pendingBackfillFields: string[] = []
  private readonly lock: Ryoiki

  constructor(file: string, options: DocumentDataplyOptions) {
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
      const optionsIndecies = (options as DocumentDataplyOptions).indecies ?? {}
      const targetIndecies: { [key: string]: boolean } = {
        ...optionsIndecies,
        _id: true
      }

      const backfillTargets: string[] = []
      let isMetadataChanged = false

      for (const field in targetIndecies) {
        const isBackfillEnabled = targetIndecies[field]
        const existingIndex = metadata.indecies[field]

        // 새롭게 추가된 인덱스
        if (!existingIndex) {
          // 사용자 요청: readHead에서 행 생성.
          // PK를 -1로 설정하여 플레이스홀더로 사용.
          metadata.indecies[field] = [-1, isBackfillEnabled]
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
            metadata.indecies[field][1] = true
            isMetadataChanged = true
            backfillTargets.push(field)
          }
          // 활성 -> 비활성
          else if (!isBackfillEnabled && isMetaBackfillEnabled) {
            metadata.indecies[field][1] = false
            isMetadataChanged = true
          }
        }
      }

      if (isMetadataChanged) {
        await this.updateDocumentInnerMetadata(metadata, tx)
      }

      this.indecies = metadata.indecies

      // 트리 초기화
      for (const field in this.indecies) {
        if (field in targetIndecies) {
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

  async getDocument(pk: number, tx?: Transaction): Promise<DataplyDocument<T>> {
    return this.runWithDefault(async (tx) => {
      const row = await this.select(pk, false, tx)
      if (!row) {
        throw new Error(`Document not found with PK: ${pk}`)
      }
      return JSON.parse(row) as DataplyDocument<T>
    }, tx)
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

  createDocumentInnerMetadata(indecies: DocumentDataplyInnerMetadata['indecies']): DocumentDataplyInnerMetadata {
    return {
      magicString: 'document-dataply',
      version: 1,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      lastId: 0,
      indecies,
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

export class DocumentDataply<T extends DocumentJSON> {
  protected readonly api: DocumentDataplyAPI<T>
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

  constructor(file: string, options?: DocumentDataplyOptions) {
    this.api = new DocumentDataplyAPI(file, options ?? {})
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
    U extends FinalFlatten<DataplyDocument<T>>,
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
   * @param query 
   * @returns 
   */
  async getSelectivityCandidate<
    U extends FinalFlatten<DataplyDocument<T>>,
    V extends DataplyTreeValue<U>
  >(query: Partial<DocumentDataplyQuery<V>>): Promise<{
    driver: {
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>
    },
    others: {
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>
    }[]
  } | null> {
    const candidates = []
    for (const field in query) {
      const tree = this.api.trees.get(field)
      if (!tree) continue
      const condition = query[field] as Partial<DocumentDataplyCondition<U>>
      candidates.push({ tree, condition })
    }
    let res = BPTreeAsync.ChooseDriver(candidates)
    if (!res && candidates.length > 0) {
      res = candidates[0]
    }
    if (!res) return null
    return {
      driver: {
        tree: res.tree as unknown as BPTreeAsync<number, V>,
        condition: res.condition
      },
      others: candidates.filter((c) => c.tree !== res.tree) as unknown as {
        tree: BPTreeAsync<number, V>,
        condition: Partial<DocumentDataplyCondition<U>>
      }[]
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

  async insertBatch(documents: T[], tx?: Transaction): Promise<number[]> {
    return this.api.writeLock(() => this.api.runWithDefault(async (tx) => {
      const pks: number[] = []
      const treeTxs: Map<string, BPTreeAsyncTransaction<number, DataplyTreeValue<Primitive>>> = new Map()
      for (const document of documents) {
        const { pk, document: dataplyDocument } = await this.insertDocument(document, tx)
        const flattenDocument = this.api.flattenDocument(dataplyDocument)
        // Indexing
        for (const field in flattenDocument) {
          let treeTx = treeTxs.get(field)
          if (!treeTx) {
            const tree = this.api.trees.get(field)
            if (!tree) continue
            treeTx = await tree.createTransaction()
            treeTxs.set(field, treeTx)
          }
          const v = flattenDocument[field]
          const [error] = await catchPromise(treeTx.insert(pk, { k: pk, v }))
          if (error) {
            console.error(`BPTree indexing failed for field: ${field}`, error)
          }
        }
        pks.push(dataplyDocument._id)
      }
      for (const tx of treeTxs.values()) {
        await tx.commit()
      }
      return pks
    }, tx))
  }

  /**
   * Select documents from the database
   * @param query The query to use
   * @param limit The maximum number of documents to return
   * @param tx The transaction to use
   * @returns The documents that match the query
   */
  async select(query: Partial<DocumentDataplyQuery<FinalFlatten<DataplyDocument<T>>>>, limit: number = Infinity, tx?: Transaction): Promise<DataplyDocument<T>[]> {
    return this.api.runWithDefault(async (tx) => {
      const verbose = this.verboseQuery(query)
      const selectivity = await this.getSelectivityCandidate(verbose)

      if (!selectivity) return []

      const keys: Set<number> = new Set()
      const { driver, others } = selectivity
      const stream = driver.tree.whereStream(driver.condition, limit)

      for await (const [pk, val] of stream) {
        let isMatch = true
        for (const { tree, condition } of others) {
          const targetValue = await tree.get(pk)
          if (targetValue === undefined || !tree.verify(targetValue, condition)) {
            isMatch = false
            break
          }
        }
        if (isMatch) {
          keys.add(pk)
          if (keys.size >= limit) break
        }
      }

      const documents: DataplyDocument<T>[] = []
      for (const key of keys) {
        const stringify = await this.api.select(key, false, tx)
        if (!stringify) {
          continue
        }
        documents.push(JSON.parse(stringify))
      }
      return documents
    }, tx)
  }

  /**
   * Close the document database
   */
  async close(): Promise<void> {
    await this.api.close()
  }
}
