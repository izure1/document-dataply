import type {
  DocumentDataplyOptions,
  DocumentJSON,
  DocumentDataplyIndexedQuery,
  DataplyDocument,
  DocumentDataplyMetadata,
  DocumentDataplyQueryOptions,
  IndexConfig
} from '../types'
import { Transaction } from 'dataply'
import { DocumentDataplyAPI } from './documentAPI'

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

  protected constructor(file: string, options?: DocumentDataplyOptions<T, IC>) {
    this.api = new DocumentDataplyAPI(file, options ?? {} as any)
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
    return this.api.fullUpdate(query, newRecord, tx)
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
    return this.api.partialUpdate(query, newRecord, tx)
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
    return this.api.deleteDocuments(query, tx)
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
    return this.api.countDocuments(query, tx)
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
    return this.api.selectDocuments(query, options, tx)
  }

  /**
   * Close the document database
   */
  async close(): Promise<void> {
    await this.api.close()
  }
}
