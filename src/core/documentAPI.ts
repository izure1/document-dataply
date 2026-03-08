import type {
  DataplyTreeValue,
  DocumentDataplyInnerMetadata,
  DocumentDataplyOptions,
  DocumentJSON,
  FlattenedDocumentJSON,
  Primitive,
  DataplyDocument,
  DocumentDataplyMetadata,
  DocumentDataplyQuery,
  DocumentDataplyQueryOptions,
  CreateIndexOption
} from '../types'
import {
  DataplyAPI,
  Transaction,
  BPTreeAsyncTransaction
} from 'dataply'
import { DocumentValueComparator } from './bptree/documentComparator'
import { catchPromise } from '../utils/catchPromise'
import { tokenize } from '../utils/tokenizer'
import { Optimizer } from './Optimizer'
import { QueryManager } from './QueryManager'
import { IndexManager } from './IndexManager'

export class DocumentDataplyAPI<T extends DocumentJSON> extends DataplyAPI {
  declare runWithDefault
  declare runWithDefaultWrite
  declare streamWithDefault

  readonly comparator = new DocumentValueComparator()
  private _initialized = false

  public readonly optimizer: Optimizer<T>
  public readonly queryManager: QueryManager<T>
  public readonly indexManager: IndexManager<T>

  constructor(file: string, options: DocumentDataplyOptions) {
    super(file, options)
    this.optimizer = new Optimizer(this)
    this.queryManager = new QueryManager(this, this.optimizer)
    this.indexManager = new IndexManager(this)

    this.hook.onceAfter('init', async (tx, isNewlyCreated) => {
      if (isNewlyCreated) {
        await this.initializeDocumentFile(tx)
      }
      if (!(await this.verifyDocumentFile(tx))) {
        throw new Error('Document metadata verification failed')
      }
      const metadata = await this.getDocumentInnerMetadata(tx)
      await this.indexManager.initializeIndices(metadata, isNewlyCreated, tx)
      this._initialized = true
      return tx
    })
  }

  /**
   * Whether the document database has been initialized.
   */
  get isDocInitialized(): boolean {
    return this._initialized
  }

  get indices() {
    return this.indexManager.indices
  }

  get trees() {
    return this.indexManager.trees
  }

  get indexedFields() {
    return this.indexManager.indexedFields
  }

  async registerIndex(name: string, option: CreateIndexOption<T>, tx?: Transaction): Promise<void> {
    return this.indexManager.registerIndex(name, option, tx)
  }

  /**
   * Drop (remove) a named index.
   */
  async dropIndex(name: string, tx?: Transaction): Promise<void> {
    return this.indexManager.dropIndex(name, tx)
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
   * Backfill indices for newly created indices after data was inserted.
   * Delegated to IndexManager.
   */
  async backfillIndices(tx?: Transaction): Promise<number> {
    return this.indexManager.backfillIndices(tx)
  }

  createDocumentInnerMetadata(indices: DocumentDataplyInnerMetadata['indices']): DocumentDataplyInnerMetadata {
    return {
      magicString: 'document-dataply',
      version: 1,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      lastId: 0,
      schemeVersion: 0,
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
      _id: [-1, { type: 'btree', fields: ['_id'] }]
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
    const innerMetadata = await this.getDocumentInnerMetadata(tx)
    const indices: string[] = []
    for (const name of this.indexManager.registeredIndices.keys()) {
      if (name !== '_id') {
        indices.push(name)
      }
    }
    return {
      pageSize: metadata.pageSize,
      pageCount: metadata.pageCount,
      rowCount: metadata.rowCount,
      usage: metadata.usage,
      indices,
      schemeVersion: innerMetadata.schemeVersion ?? 0,
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
   * Run a migration if the current schemeVersion is lower than the target version.
   * After the callback completes, schemeVersion is updated to the target version.
   * @param version The target scheme version
   * @param callback The migration callback
   * @param tx Optional transaction
   */
  async migration(
    version: number,
    callback: (tx: Transaction) => Promise<void>,
    tx?: Transaction
  ): Promise<void> {
    await this.runWithDefaultWrite(async (tx) => {
      const innerMetadata = await this.getDocumentInnerMetadata(tx)
      const currentVersion = innerMetadata.schemeVersion ?? 0
      if (currentVersion < version) {
        await callback(tx)
        // 콜백 내부에서 createIndex/dropIndex가 메타데이터를 변경했을 수 있으므로
        // 최신 메타데이터를 다시 읽어서 schemeVersion만 업데이트
        const freshMetadata = await this.getDocumentInnerMetadata(tx)
        freshMetadata.schemeVersion = version
        freshMetadata.updatedAt = Date.now()
        await this.updateDocumentInnerMetadata(freshMetadata, tx)
      }
    }, tx)
  }

  // Query execution methods (evaluateBTreeCandidate, getSelectivityCandidate, getKeys, getDriverKeys, etc.)
  // have been extracted into QueryManager.ts and Optimizer.ts.

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
    return this.runWithDefaultWrite(async (tx) => {
      const { pk: dpk, document: dataplyDocument } = await this.insertDocumentInternal(document, tx)
      const flattenDocument = this.flattenDocument(dataplyDocument)

      // 등록된 인덱스별로 인덱싱
      for (const [indexName, config] of this.indexManager.registeredIndices) {
        const tree = this.trees.get(indexName)
        if (!tree) continue

        if (config.type === 'fts') {
          const primaryField = this.indexManager.getPrimaryField(config)
          const v = flattenDocument[primaryField]
          if (v === undefined || typeof v !== 'string') continue
          const ftsConfig = this.indexManager.getFtsConfig(config)
          const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
          for (let i = 0, len = tokens.length; i < len; i++) {
            const token = tokens[i]
            const keyToInsert = this.indexManager.getTokenKey(dpk, token as string)
            const [error] = await catchPromise(tree.insert(keyToInsert, { k: dpk, v: token }))
            if (error) throw error
          }
        }
        else {
          const indexVal = this.indexManager.getIndexValue(config, flattenDocument)
          if (indexVal === undefined) continue
          const [error] = await catchPromise(tree.insert(dpk, { k: dpk, v: indexVal } as any))
          if (error) throw error
        }
      }
      return dataplyDocument._id
    }, tx)
  }

  /**
   * Insert a batch of documents into the database
   * @param documents The documents to insert
   * @param tx The transaction to use
   * @returns The primary keys of the inserted documents
   */
  async insertBatchDocuments(documents: T[], tx?: Transaction): Promise<number[]> {
    return this.runWithDefaultWrite(async (tx) => {
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

      // 5. 등록된 인덱스별로 인덱싱
      for (const [indexName, config] of this.indexManager.registeredIndices) {
        const tree = this.trees.get(indexName)
        if (!tree) continue

        const treeTx = await tree.createTransaction()
        const batchInsertData: [string | number, DataplyTreeValue<Primitive>][] = []

        if (config.type === 'fts') {
          const primaryField = this.indexManager.getPrimaryField(config)
          const ftsConfig = this.indexManager.getFtsConfig(config)
          for (let i = 0, len = flattenedData.length; i < len; i++) {
            const item = flattenedData[i]
            const v = item.data[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, tLen = tokens.length; j < tLen; j++) {
              const token = tokens[j]
              batchInsertData.push([this.indexManager.getTokenKey(item.pk, token as string), { k: item.pk, v: token }])
            }
          }
        }
        else {
          for (let i = 0, len = flattenedData.length; i < len; i++) {
            const item = flattenedData[i]
            const indexVal = this.indexManager.getIndexValue(config, item.data)
            if (indexVal === undefined) continue
            batchInsertData.push([item.pk, { k: item.pk, v: indexVal } as any])
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
    }, tx)
  }

  /**
   * Internal update method used by both fullUpdate and partialUpdate
   * @param query The query to use
   * @param computeUpdatedDoc Function that computes the updated document from the original
   * @param tx The transaction to use
   * @returns The number of updated documents
   */
  private async updateInternal(
    query: Partial<DocumentDataplyQuery<T>>,
    computeUpdatedDoc: (doc: DataplyDocument<T>) => DataplyDocument<T>,
    tx: Transaction
  ): Promise<number> {
    const pks = await this.queryManager.getKeys(query)
    let updatedCount = 0

    const treeTxs = new Map<string, BPTreeAsyncTransaction<string | number, DataplyTreeValue<any>>>()
    for (const [indexName, tree] of this.trees) {
      treeTxs.set(indexName, await tree.createTransaction())
    }
    treeTxs.delete('_id')

    for (let i = 0, len = pks.length; i < len; i++) {
      const pk = pks[i]
      const doc = await this.getDocument(pk, tx)
      if (!doc) continue

      const updatedDoc = computeUpdatedDoc(doc)
      const oldFlatDoc = this.flattenDocument(doc)
      const newFlatDoc = this.flattenDocument(updatedDoc)

      // 변경된 인덱스 필드 동기화
      for (const [indexName, treeTx] of treeTxs) {
        const config = this.indexManager.registeredIndices.get(indexName)
        if (!config) continue

        if (config.type === 'fts') {
          const primaryField = this.indexManager.getPrimaryField(config)
          const oldV = oldFlatDoc[primaryField]
          const newV = newFlatDoc[primaryField]
          if (oldV === newV) continue
          const ftsConfig = this.indexManager.getFtsConfig(config)

          // 기존 FTS 토큰 삭제
          if (typeof oldV === 'string') {
            const oldTokens = ftsConfig ? tokenize(oldV, ftsConfig) : [oldV]
            for (let j = 0, jLen = oldTokens.length; j < jLen; j++) {
              await treeTx.delete(this.indexManager.getTokenKey(pk, oldTokens[j] as string), { k: pk, v: oldTokens[j] })
            }
          }
          // 새 FTS 토큰 삽입
          if (typeof newV === 'string') {
            const newTokens = ftsConfig ? tokenize(newV, ftsConfig) : [newV]
            const batchInsertData: [string | number, DataplyTreeValue<Primitive>][] = []
            for (let j = 0, jLen = newTokens.length; j < jLen; j++) {
              batchInsertData.push([this.indexManager.getTokenKey(pk, newTokens[j] as string), { k: pk, v: newTokens[j] }])
            }
            await treeTx.batchInsert(batchInsertData)
          }
        }
        else {
          const oldIndexVal = this.indexManager.getIndexValue(config, oldFlatDoc)
          const newIndexVal = this.indexManager.getIndexValue(config, newFlatDoc)

          // 값이 동일하면 스킵 (배열 비교를 위해 JSON.stringify 사용)
          if (JSON.stringify(oldIndexVal) === JSON.stringify(newIndexVal)) continue

          // 기존 값 삭제
          if (oldIndexVal !== undefined) {
            await treeTx.delete(pk, { k: pk, v: oldIndexVal } as any)
          }
          // 새 값 삽입
          if (newIndexVal !== undefined) {
            await treeTx.batchInsert([[pk, { k: pk, v: newIndexVal } as any]])
          }
        }
      }

      // 실제 레코드 업데이트
      await this.update(pk, JSON.stringify(updatedDoc), tx)
      updatedCount++
    }

    for (const [indexName, treeTx] of treeTxs) {
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
   * @param query The query to use
   * @param newRecord Complete document to replace with, or function that receives current document and returns new document
   * @param tx The transaction to use
   * @returns The number of updated documents
   */
  async fullUpdate(
    query: Partial<DocumentDataplyQuery<T>>,
    newRecord: T | ((document: DataplyDocument<T>) => T),
    tx?: Transaction
  ): Promise<number> {
    return this.runWithDefaultWrite(async (tx) => {
      return this.updateInternal(query, (doc) => {
        const newDoc = typeof newRecord === 'function'
          ? (newRecord as Function)(doc)
          : newRecord
        // _id 보존
        return { _id: doc._id, ...newDoc } as DataplyDocument<T>
      }, tx)
    }, tx)
  }

  /**
   * Partially update documents from the database that match the query
   * @param query The query to use
   * @param newRecord Partial document to merge, or function that receives current document and returns partial update
   * @param tx The transaction to use
   * @returns The number of updated documents
   */
  async partialUpdate(
    query: Partial<DocumentDataplyQuery<T>>,
    newRecord: Partial<DataplyDocument<T>> | ((document: DataplyDocument<T>) => Partial<DataplyDocument<T>>),
    tx?: Transaction
  ): Promise<number> {
    return this.runWithDefaultWrite(async (tx) => {
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
    }, tx)
  }

  /**
   * Delete documents from the database that match the query
   * @param query The query to use
   * @param tx The transaction to use
   * @returns The number of deleted documents
   */
  async deleteDocuments(
    query: Partial<DocumentDataplyQuery<T>>,
    tx?: Transaction
  ): Promise<number> {
    return this.runWithDefaultWrite(async (tx) => {
      const pks = await this.queryManager.getKeys(query)
      let deletedCount = 0

      for (let i = 0, len = pks.length; i < len; i++) {
        const pk = pks[i]
        const doc = await this.getDocument(pk, tx)
        if (!doc) continue

        const flatDoc = this.flattenDocument(doc)

        // 모든 인덱스 트리에서 삭제
        for (const [indexName, tree] of this.trees) {
          const config = this.indexManager.registeredIndices.get(indexName)
          if (!config) continue

          if (config.type === 'fts') {
            const primaryField = this.indexManager.getPrimaryField(config)
            const v = flatDoc[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const ftsConfig = this.indexManager.getFtsConfig(config)
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, jLen = tokens.length; j < jLen; j++) {
              await tree.delete(this.indexManager.getTokenKey(pk, tokens[j] as string), { k: pk, v: tokens[j] })
            }
          } else {
            const indexVal = this.indexManager.getIndexValue(config, flatDoc)
            if (indexVal === undefined) continue
            await tree.delete(pk, { k: pk, v: indexVal } as any)
          }
        }

        // 실제 레코드 삭제
        await super.delete(pk, true, tx)
        deletedCount++
      }

      return deletedCount
    }, tx)
  }

  /**
   * Count documents from the database that match the query
   * @param query The query to use
   * @param tx The transaction to use
   * @returns The number of documents that match the query
   */
  async countDocuments(
    query: Partial<DocumentDataplyQuery<T>>,
    tx?: Transaction
  ): Promise<number> {
    return this.runWithDefault(async (tx) => {
      const pks = await this.queryManager.getKeys(query)
      return pks.length
    }, tx)
  }

  /**
   * Select documents from the database
   * @param query The query to use
   * @param options The options to use
   * @param tx The transaction to use
   * @returns The documents that match the query
   * @throws Error if query or orderBy contains non-indexed fields
   */
  selectDocuments(
    query: Partial<DocumentDataplyQuery<T>>,
    options: DocumentDataplyQueryOptions = {},
    tx?: Transaction
  ): {
    stream: () => AsyncIterableIterator<DataplyDocument<T>>
    drain: () => Promise<DataplyDocument<T>[]>
  } {
    return this.queryManager.selectDocuments(query, options, tx)
  }
}
