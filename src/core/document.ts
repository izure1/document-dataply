import type { DataplyTreeValue, DocumentDataplyInnerMetadata, DocumentDataplyOptions, DocumentJSON, FlattenedDocumentJSON, Primitive, DocumentDataplyQuery, FinalFlatten, DocumentDataplyCondition, DataplyDocument, DocumentDataplyMetadata } from '../types'
import { DataplyAPI, Transaction, BPTreeAsync, type BPTreeCondition, BPTreeAsyncTransaction } from 'dataply'
import { DocumentSerializeStrategyAsync } from './bptree/documentStrategy'
import { DocumentValueComparator } from './bptree/documentComparator'



export class DocumentDataplyAPI<T extends DocumentJSON> extends DataplyAPI {
  declare runWithDefault

  indecies: DocumentDataplyInnerMetadata['indecies'] = {}
  readonly trees: Map<string, BPTreeAsync<number, DataplyTreeValue<Primitive>>> = new Map()
  readonly comparator = new DocumentValueComparator()

  constructor(file: string, options: DocumentDataplyOptions) {
    super(file, options)
    this.trees = new Map()
    this.hook.onceAfter('init', async (tx, isNewlyCreated) => {
      if (isNewlyCreated) {
        await this.initializeDocumentFile(tx)
      }
      if (!(await this.verifyDocumentFile(tx))) {
        throw new Error('Document metadata verification failed')
      }
      const metadata = await this.getDocumentInnerMetadata(tx)
      const optionsIndecies = (options as DocumentDataplyOptions).indecies ?? {}
      const targetIndecies: { [key: string]: boolean } = { ...optionsIndecies, _id: true }

      const backfillTargets: string[] = []
      let isMetadataChanged = false

      for (const field in targetIndecies) {
        const isBackfillEnabled = targetIndecies[field]
        const existingIndex = metadata.indecies[field]

        if (!existingIndex) {
          // New Index
          // User request: Create row in readHead.
          // We set PK to 0 as placeholder.
          metadata.indecies[field] = [0, isBackfillEnabled]
          isMetadataChanged = true

          if (isBackfillEnabled && !isNewlyCreated) {
            // If DB is new, no data to backfill anyway.
            backfillTargets.push(field)
          }
        }
        else {
          // Existing Index
          const [pk, isMetaBackfillEnabled] = existingIndex
          // False -> True
          if (isBackfillEnabled && !isMetaBackfillEnabled) {
            metadata.indecies[field][1] = true
            isMetadataChanged = true
            backfillTargets.push(field)
          }
          // True -> False
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

      // Initialize Trees
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

      if (backfillTargets.length) {
        const idTree = this.trees.get('_id')
        if (idTree) {
          // Iterate all documents
          const stream = idTree.whereStream({}, Infinity)
          const fields: Record<string, BPTreeAsyncTransaction<number, DataplyTreeValue<Primitive>>> = {}

          for await (const [pk, val] of stream) {
            // Fetch Document
            const docRow = await this.select(pk, false, tx)
            if (!docRow) continue
            const doc = JSON.parse(docRow) as DataplyDocument<T>
            const flatDoc = this.flattenDocument(doc)

            for (const field of backfillTargets) {
              const tree = this.trees.get(field)
              if (
                field === '_id' ||
                !tree ||
                !(field in flatDoc)
              ) {
                continue
              }

              const v = flatDoc[field]
              const btx = await tree.createTransaction()
              fields[field] = btx
              await btx.insert(pk, { k: pk, v })
            }
          }

          await Promise.all(Object.values(fields).map(btx => btx.commit()))
        }
      }

      return tx
    })
  }

  createDocumentInnerMetadata(idTreePk: number): DocumentDataplyInnerMetadata {
    return {
      magicString: 'document-dataply',
      version: 1,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      lastId: 0,
      indecies: {
        _id: [idTreePk, true]
      }
    }
  }

  async initializeDocumentFile(tx: Transaction): Promise<void> {
    const metadata = await this.select(1, false, tx)
    if (metadata) {
      throw new Error('Document metadata already exists')
    }
    // 1. Reserve Row 1 with placeholder
    await this.insertAsOverflow(JSON.stringify({ __placeholder: true }), false, tx)

    // 2. Create _id tree immediately so we have the PK
    const initialHead = {
      order: (this.rowTableEngine as any).order,
      root: null,
      data: {}
    }
    const json = JSON.stringify(initialHead)
    const pk = await this.insert(json, false, tx)

    // 3. Update metadata with _id tree PK
    const metaObj = this.createDocumentInnerMetadata(pk)
    await this.update(1, JSON.stringify(metaObj), tx)
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
    k: number
    id: number
    document: DataplyDocument<T>
  }> {
    const metadata = await this.api.getDocumentInnerMetadata(tx)
    const id = ++metadata.lastId
    await this.api.updateDocumentInnerMetadata(metadata, tx)
    const dataplyDocument: DataplyDocument<T> = Object.assign({
      _id: id,
    }, document)
    const k = await this.api.insert(JSON.stringify(dataplyDocument), true, tx)
    return {
      k,
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
    return this.api.runWithDefault(async (tx) => {
      const { k, document: dataplyDocument } = await this.insertDocument(document, tx)
      const flattenDocument = this.api.flattenDocument(dataplyDocument)

      for (const field in flattenDocument) {
        const tree = this.api.trees.get(field)
        if (!tree) continue

        const v = flattenDocument[field]
        const treeTx = await tree.createTransaction() // Create short-lived transaction
        await treeTx.insert(k, { k, v })
        const result = await treeTx.commit() // Flush to main transaction
        if (!result.success) {
          throw new Error(`BPTree indexing failed for field: ${field}`)
        }
      }
      return dataplyDocument._id
    }, tx)
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
