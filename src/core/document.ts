import type {
  DocumentDataplyOptions,
  DocumentJSON,
  DocumentDataplyQuery,
  DataplyDocument,
  DocumentDataplyMetadata,
  DocumentDataplyQueryOptions,
  CreateIndexOption
} from '../types'
import { Transaction } from 'dataply'
import { DocumentDataplyAPI } from './documentAPI'

export class DocumentDataply<T extends DocumentJSON> {
  /**
   * Starts the database definition by setting the document type.
   * This is used to ensure TypeScript type inference works correctly for the document structure.
   * @template T The structure of the document to be stored.
   */
  static Define<T extends DocumentJSON>() {
    return {
      /**
       * Sets the options for the database, such as WAL settings.
       * @param options The database initialization options.
       */
      Options: (
        options: DocumentDataplyOptions
      ) => DocumentDataply.Options<T>(options)
    }
  }

  /**
   * Internal method used by the Define-chain to pass options.
   */
  private static Options<T extends DocumentJSON>(
    options: DocumentDataplyOptions
  ) {
    return {
      /**
       * Creates or opens the database instance with the specified file path.
       * @param file The path to the database file.
       */
      Open: (file: string) => DocumentDataply.Open<T>(file, options)
    }
  }

  /**
   * Internal method used to finalize construction and create the instance.
   */
  private static Open<T extends DocumentJSON>(
    file: string,
    options: DocumentDataplyOptions
  ) {
    return new DocumentDataply<T>(file, options)
  }

  protected readonly api: DocumentDataplyAPI<T>

  protected constructor(file: string, options?: DocumentDataplyOptions) {
    this.api = new DocumentDataplyAPI(file, options ?? {} as any)
  }

  /**
   * Create a named index on the database.
   * Can be called before or after init().
   * If called after init(), the index is immediately created and backfilled.
   * @param name The name of the index
   * @param option The index configuration (btree or fts)
   * @param tx Optional transaction
   * @returns Promise<this> for chaining
   */
  async createIndex(name: string, option: CreateIndexOption<T>, tx?: Transaction): Promise<this> {
    await this.api.registerIndex(name, option, tx)
    return this
  }

  /**
   * Drop (remove) a named index from the database.
   * The '_id' index cannot be dropped.
   * @param name The name of the index to drop
   * @param tx Optional transaction
   * @returns Promise<this> for chaining
   */
  async dropIndex(name: string, tx?: Transaction): Promise<this> {
    await this.api.dropIndex(name, tx)
    return this
  }

  /**
   * Initialize the document database
   */
  async init(): Promise<void> {
    await this.api.init()
    await this.api.backfillIndices()
  }

  /**
   * Run a migration if the current schemeVersion is lower than the target version.
   * The callback is only executed when the database's schemeVersion is below the given version.
   * After the callback completes, schemeVersion is updated to the target version.
   * @param version The target scheme version
   * @param callback The migration callback receiving a transaction
   * @param tx Optional transaction
   */
  async migration(
    version: number,
    callback: (tx: Transaction) => Promise<void>,
    tx?: Transaction
  ): Promise<void> {
    await this.api.migration(version, callback, tx)
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

  /**
   * Insert a document into the database
   * @param document The document to insert
   * @param tx The transaction to use
   * @returns The primary key of the inserted document
   */
  async insert(document: T, tx?: Transaction): Promise<number> {
    return this.api.insertSingleDocument(document, tx)
  }

  /**
   * Insert a batch of documents into the database
   * @param documents The documents to insert
   * @param tx The transaction to use
   * @returns The primary keys of the inserted documents
   */
  async insertBatch(documents: T[], tx?: Transaction): Promise<number[]> {
    return this.api.insertBatchDocuments(documents, tx)
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
    return this.api.fullUpdate(query, newRecord, tx)
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
    return this.api.partialUpdate(query, newRecord, tx)
  }

  /**
   * Delete documents from the database that match the query
   * @param query The query to use
   * @param tx The transaction to use
   * @returns The number of deleted documents
   */
  async delete(
    query: Partial<DocumentDataplyQuery<T>>,
    tx?: Transaction
  ): Promise<number> {
    return this.api.deleteDocuments(query, tx)
  }

  /**
   * Count documents from the database that match the query
   * @param query The query to use
   * @param tx The transaction to use
   * @returns The number of documents that match the query
   */
  async count(
    query: Partial<DocumentDataplyQuery<T>>,
    tx?: Transaction
  ): Promise<number> {
    return this.api.countDocuments(query, tx)
  }

  /**
   * Select documents from the database
   * @param query The query to use
   * @param options The options to use
   * @param tx The transaction to use
   * @returns The documents that match the query
   * @throws Error if query or orderBy contains non-indexed fields
   */
  select(
    query: Partial<DocumentDataplyQuery<T>>,
    options: DocumentDataplyQueryOptions = {},
    tx?: Transaction
  ): {
    stream: () => AsyncIterableIterator<DataplyDocument<T>>
    drain: () => Promise<DataplyDocument<T>[]>
  } {
    return this.api.selectDocuments(query, options, tx)
  }

  /**
   * Close the document database
   */
  async close(): Promise<void> {
    await this.api.close()
  }
}
