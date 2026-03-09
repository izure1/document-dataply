import type {
  DocumentDataplyInnerMetadata,
  DocumentDataplyOptions,
  DocumentJSON,
  FlattenedDocumentJSON,
  DataplyDocument,
  DocumentDataplyMetadata,
  DocumentDataplyQuery,
  DocumentDataplyQueryOptions,
  CreateIndexOption
} from '../types'
import {
  DataplyAPI,
  Transaction
} from 'dataply'
import { DocumentValueComparator } from './bptree/documentComparator'
import { Optimizer } from './Optimizer'
import { QueryManager } from './QueryManager'
import { IndexManager } from './IndexManager'
import { MutationManager } from './MutationManager'
import { MetadataManager } from './MetadataManager'
import { DocumentFormatter } from './DocumentFormatter'
import { AnalysisManager } from './AnalysisManager'

export class DocumentDataplyAPI<T extends DocumentJSON> extends DataplyAPI {
  declare runWithDefault
  declare runWithDefaultWrite
  declare streamWithDefault

  readonly comparator = new DocumentValueComparator()
  private _initialized = false

  public readonly optimizer: Optimizer<T>
  public readonly queryManager: QueryManager<T>
  public readonly indexManager: IndexManager<T>
  public readonly mutationManager: MutationManager<T>
  public readonly metadataManager: MetadataManager<T>
  public readonly documentFormatter: DocumentFormatter<T>
  public readonly analysisManager: AnalysisManager<T>

  constructor(file: string, options: DocumentDataplyOptions) {
    super(file, options)
    this.optimizer = new Optimizer(this)
    this.queryManager = new QueryManager(this, this.optimizer)
    this.indexManager = new IndexManager(this)
    this.mutationManager = new MutationManager(this)
    this.metadataManager = new MetadataManager(this)
    this.documentFormatter = new DocumentFormatter<T>()
    this.analysisManager = new AnalysisManager(this)

    this.hook.onceAfter('init', async (tx, isNewlyCreated) => {
      if (isNewlyCreated) {
        await this.initializeDocumentFile(tx)
      }
      if (!(await this.verifyDocumentFile(tx))) {
        throw new Error('Document metadata verification failed')
      }
      const metadata = await this.getDocumentInnerMetadata(tx)
      await this.indexManager.initializeIndices(metadata, isNewlyCreated, tx)
      this.analysisManager.registerBuiltinProviders()
      await this.analysisManager.initializeProviders(tx)
      await this.analysisManager.flush(tx)
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

  /**
   * Register an index.
   * @param name The name of the index
   * @param option The option of the index
   * @param tx The transaction to use
   */
  async registerIndex(name: string, option: CreateIndexOption<T>, tx?: Transaction): Promise<void> {
    return this.indexManager.registerIndex(name, option, tx)
  }

  /**
   * Drop (remove) a named index.
   * @param name The name of the index
   * @param tx The transaction to use
   */
  async dropIndex(name: string, tx?: Transaction): Promise<void> {
    return this.indexManager.dropIndex(name, tx)
  }

  /**
   * Get a document by its primary key.
   * @param pk The primary key of the document
   * @param tx The transaction to use
   * @returns The document
   */
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

  /**
   * Initialize the document database file.
   * @param tx The transaction to use
   */
  async initializeDocumentFile(tx: Transaction): Promise<void> {
    const metadata = await this.select(1, false, tx)
    if (metadata) {
      throw new Error('Document metadata already exists')
    }
    // 1. _id 인덱스 플레이스홀더(pk=-1)를 포함한 초기 메타데이터 생성
    // 실제 트리 헤드 행은 DocumentSerializeStrategyAsync.readHead()에서 지연 생성됨
    const metaObj = this.createDocumentInnerMetadata({
      _id: [-1, {
        type: 'btree',
        fields: ['_id']
      }]
    })
    // 2. 플레이스홀더로 1번 행에 저장
    await this.insertAsOverflow(JSON.stringify(metaObj), false, tx)
  }

  /**
   * Verify the document database file.
   * @param tx The transaction to use
   * @returns True if the document database file is valid, false otherwise
   */
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
    return this.documentFormatter.flattenDocument(document)
  }

  /**
   * Get the document metadata.
   * @param tx The transaction to use
   * @returns The document metadata
   */
  async getDocumentMetadata(tx: Transaction): Promise<DocumentDataplyMetadata> {
    return this.metadataManager.getDocumentMetadata(tx)
  }

  /**
   * Get the document inner metadata.
   * @param tx The transaction to use
   * @returns The document inner metadata
   */
  async getDocumentInnerMetadata(tx: Transaction): Promise<DocumentDataplyInnerMetadata> {
    return this.metadataManager.getDocumentInnerMetadata(tx)
  }

  /**
   * Update the document inner metadata.
   * @param metadata The document inner metadata
   * @param tx The transaction to use
   */
  async updateDocumentInnerMetadata(metadata: DocumentDataplyInnerMetadata, tx: Transaction): Promise<void> {
    return this.metadataManager.updateDocumentInnerMetadata(metadata, tx)
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
    return this.metadataManager.migration(version, callback, tx)
  }

  /**
   * Insert a document into the database
   * @param document The document to insert
   * @param tx The transaction to use
   * @returns The primary key of the inserted document
   */
  async insertSingleDocument(document: T, tx?: Transaction): Promise<number> {
    return this.mutationManager.insertSingleDocument(document, tx)
  }

  /**
   * Insert a batch of documents into the database
   * @param documents The documents to insert
   * @param tx The transaction to use
   * @returns The primary keys of the inserted documents
   */
  async insertBatchDocuments(documents: T[], tx?: Transaction): Promise<number[]> {
    return this.mutationManager.insertBatchDocuments(documents, tx)
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
    return this.mutationManager.fullUpdate(query, newRecord, tx)
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
    return this.mutationManager.partialUpdate(query, newRecord, tx)
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
    return this.mutationManager.deleteDocuments(query, tx)
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
    return this.queryManager.countDocuments(query, tx)
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
