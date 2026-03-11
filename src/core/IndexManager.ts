import type {
  DataplyTreeValue,
  DocumentDataplyInnerMetadata,
  Primitive,
  CreateIndexOption,
  IndexMetaConfig,
  FTSConfig,
  DocumentJSON,
  FlattenedDocumentJSON
} from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import { BPTreeAsync, Transaction, BPTreeAsyncTransaction } from 'dataply'
import { tokenize } from '../utils/tokenizer'
import { DocumentSerializeStrategyAsync } from './bptree/documentStrategy'

export class IndexManager<T extends DocumentJSON> {
  indices: DocumentDataplyInnerMetadata['indices'] = {}
  readonly trees: Map<string, BPTreeAsync<string | number, DataplyTreeValue<Primitive>>> = new Map()
  readonly indexedFields: Set<string>

  /**
   * Registered indices via createIndex() (before init)
   * Key: index name, Value: index configuration
   */
  readonly pendingCreateIndices: Map<string, CreateIndexOption<T>> = new Map()

  /**
   * Resolved index configurations after init.
   * Key: index name, Value: index config (from metadata)
   */
  registeredIndices: Map<string, IndexMetaConfig> = new Map()

  /**
   * Maps field name → index names that cover this field.
   * Used for query resolution.
   */
  fieldToIndices: Map<string, string[]> = new Map()

  pendingBackfillFields: string[] = []

  constructor(
    private api: DocumentDataplyAPI<T>,
    private logger: any
  ) {
    this.trees = new Map()
    this.indexedFields = new Set(['_id'])
  }

  /**
   * Validate and apply indices from DB metadata and pending indices.
   * Called during database initialization.
   */
  async initializeIndices(
    metadata: DocumentDataplyInnerMetadata,
    isNewlyCreated: boolean,
    tx: Transaction
  ): Promise<boolean> {
    const targetIndices: Map<string, IndexMetaConfig> = new Map([
      ['_id', { type: 'btree', fields: ['_id'] }]
    ])

    // Load existing indices from metadata
    for (const [name, info] of Object.entries(metadata.indices)) {
      targetIndices.set(name, info[1])
    }

    // Apply pending options
    for (const [name, option] of this.pendingCreateIndices) {
      const config = this.toIndexMetaConfig(option)
      targetIndices.set(name, config)
    }

    const backfillTargets: string[] = []
    let isMetadataChanged = false

    for (const [indexName, config] of targetIndices) {
      const existingIndex = metadata.indices[indexName]

      if (!existingIndex) {
        metadata.indices[indexName] = [-1, config]
        isMetadataChanged = true
        if (!isNewlyCreated) {
          backfillTargets.push(indexName)
        }
      } else {
        const [_pk, existingConfig] = existingIndex
        if (JSON.stringify(existingConfig) !== JSON.stringify(config)) {
          metadata.indices[indexName] = [_pk, config]
          isMetadataChanged = true
          if (!isNewlyCreated) {
            backfillTargets.push(indexName)
          }
        }
      }
    }

    if (isMetadataChanged) {
      await this.api.updateDocumentInnerMetadata(metadata, tx)
    }

    this.indices = metadata.indices
    this.registeredIndices = new Map()
    this.fieldToIndices = new Map()

    for (const [indexName, config] of targetIndices) {
      this.registeredIndices.set(indexName, config)

      const fields = this.getFieldsFromConfig(config)
      for (const field of fields) {
        this.indexedFields.add(field)
        if (!this.fieldToIndices.has(field)) {
          this.fieldToIndices.set(field, [])
        }
        this.fieldToIndices.get(field)!.push(indexName)
      }
    }

    // Initialize trees
    const perIndexCapacity = Math.floor(this.api.options.pageCacheCapacity / Object.keys(this.api.indices).length)
    for (const indexName of targetIndices.keys()) {
      if (metadata.indices[indexName]) {
        const tree = new BPTreeAsync<number, DataplyTreeValue<Primitive>>(
          new DocumentSerializeStrategyAsync<Primitive>(
            (this.api as any).rowTableEngine.order,
            this.api,
            (this.api as any).txContext,
            indexName
          ),
          this.api.comparator as any,
          {
            capacity: perIndexCapacity
          }
        )
        await tree.init()
        this.trees.set(indexName, tree as any)
      }
    }

    this.pendingBackfillFields = backfillTargets
    return isMetadataChanged
  }

  /**
   * Register an index. If called before init(), queues it.
   */
  async registerIndex(name: string, option: CreateIndexOption<T>, tx?: Transaction): Promise<void> {
    this.logger.debug(`Registering index "${name}" (type: ${option.type})`)
    if (!this.api.isDocInitialized) {
      this.pendingCreateIndices.set(name, option)
      return
    }
    await this.registerIndexRuntime(name, option, tx)
  }

  /**
   * Register an index at runtime (after init).
   */
  private async registerIndexRuntime(name: string, option: CreateIndexOption<T>, tx?: Transaction): Promise<void> {
    const config = this.toIndexMetaConfig(option)

    if (this.registeredIndices.has(name)) {
      throw new Error(`Index "${name}" already exists.`)
    }

    await this.api.runWithDefaultWrite(async (tx) => {
      const metadata = await this.api.getDocumentInnerMetadata(tx)
      metadata.indices[name] = [-1, config]
      await this.api.updateDocumentInnerMetadata(metadata, tx)
      this.indices = metadata.indices

      this.registeredIndices.set(name, config)
      const fields = this.getFieldsFromConfig(config)
      for (let i = 0; i < fields.length; i++) {
        const field = fields[i]
        this.indexedFields.add(field)
        if (!this.fieldToIndices.has(field)) {
          this.fieldToIndices.set(field, [])
        }
        this.fieldToIndices.get(field)!.push(name)
      }

      const perIndexCapacity = Math.floor(this.api.options.pageCacheCapacity / Object.keys(this.api.indices).length)
      const tree = new BPTreeAsync<number, DataplyTreeValue<Primitive>>(
        new DocumentSerializeStrategyAsync<Primitive>(
          (this.api as any).rowTableEngine.order,
          this.api,
          (this.api as any).txContext,
          name
        ),
        this.api.comparator as any,
        {
          capacity: perIndexCapacity
        }
      )
      await tree.init()
      this.trees.set(name, tree as any)

      if (metadata.lastId > 0) {
        this.pendingBackfillFields = [name]
        await this.backfillIndices(tx)
      }
    }, tx)
  }

  /**
   * Drop (remove) a named index.
   */
  async dropIndex(name: string, tx?: Transaction): Promise<void> {
    if (name === '_id') {
      throw new Error('Cannot drop the "_id" index.')
    }
    if (!this.api.isDocInitialized) {
      this.pendingCreateIndices.delete(name)
      return
    }
    if (!this.registeredIndices.has(name)) {
      throw new Error(`Index "${name}" does not exist.`)
    }

    await this.api.runWithDefaultWrite(async (tx) => {
      const config = this.registeredIndices.get(name)!

      const metadata = await this.api.getDocumentInnerMetadata(tx)
      delete metadata.indices[name]
      await this.api.updateDocumentInnerMetadata(metadata, tx)
      this.indices = metadata.indices

      this.registeredIndices.delete(name)

      const fields = this.getFieldsFromConfig(config)
      for (let i = 0; i < fields.length; i++) {
        const field = fields[i]
        const indexNames = this.fieldToIndices.get(field)
        if (indexNames) {
          const filtered = indexNames.filter(n => n !== name)
          if (filtered.length === 0) {
            this.fieldToIndices.delete(field)
            if (field !== '_id') {
              this.indexedFields.delete(field)
            }
          }
          else {
            this.fieldToIndices.set(field, filtered)
          }
        }
      }

      this.trees.delete(name)
    }, tx)
  }

  /**
   * Backfill indices for newly created indices after data was inserted.
   */
  async backfillIndices(tx?: Transaction): Promise<number> {
    this.logger.debug(`Starting backfill for fields: ${this.pendingBackfillFields.join(', ')}`)
    return this.api.runWithDefaultWrite(async (tx) => {
      if (this.pendingBackfillFields.length === 0) {
        return 0
      }

      const backfillTargets = this.pendingBackfillFields
      const metadata = await this.api.getDocumentInnerMetadata(tx)

      if (metadata.lastId === 0) {
        return 0
      }

      let indexTxMap: Record<
        string,
        BPTreeAsyncTransaction<string | number, DataplyTreeValue<Primitive>>
      > = {}

      for (const indexName of backfillTargets) {
        const tree = this.trees.get(indexName)
        if (tree && indexName !== '_id') {
          indexTxMap[indexName] = await tree.createTransaction()
        }
      }

      let backfilledCount = 0
      let chunkCount = 0
      const CHUNK_SIZE = 1000

      const idTree = this.trees.get('_id')
      if (!idTree) {
        throw new Error('ID tree not found')
      }

      const stream = idTree.whereStream({
        primaryGte: { v: 0 }
      })

      for await (const [k, complexValue] of stream) {
        const doc = await this.api.getDocument(k as number, tx)
        if (!doc) continue
        const flatDoc = this.api.flattenDocument(doc)

        for (let i = 0, len = backfillTargets.length; i < len; i++) {
          const indexName = backfillTargets[i]
          if (!(indexName in indexTxMap)) continue

          const config = this.registeredIndices.get(indexName)
          if (!config) continue

          const btx = indexTxMap[indexName]

          if (config.type === 'fts') {
            const primaryField = this.getPrimaryField(config)
            const v = flatDoc[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const ftsConfig = this.getFtsConfig(config)
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            const batchInsertData: [number | string, DataplyTreeValue<Primitive>][] = []
            for (let i = 0, len = tokens.length; i < len; i++) {
              const token = tokens[i]
              const keyToInsert = this.getTokenKey(k as number, token as string)
              const entry = { k: k as number, v: token }
              batchInsertData.push([keyToInsert, entry])
            }
            await btx.batchInsert(batchInsertData)
          }
          else {
            const indexVal = this.getIndexValue(config, flatDoc)
            if (indexVal === undefined) continue
            const entry = { k: k as number, v: indexVal }
            const batchInsertData: [number | string, DataplyTreeValue<Primitive>][] = [[k, entry as any]]
            await btx.batchInsert(batchInsertData)
          }
        }
        backfilledCount++
        chunkCount++

        if (chunkCount >= CHUNK_SIZE) {
          try {
            for (const btx of Object.values(indexTxMap)) {
              await btx.commit()
            }
          } catch (err) {
            for (const btx of Object.values(indexTxMap)) {
              await btx.rollback()
            }
            throw err
          }

          for (const indexName of backfillTargets) {
            const tree = this.trees.get(indexName)
            if (tree && indexName !== '_id') {
              indexTxMap[indexName] = await tree.createTransaction()
            }
          }
          chunkCount = 0
        }
      }

      if (chunkCount > 0) {
        try {
          for (const btx of Object.values(indexTxMap)) {
            await btx.commit()
          }
        } catch (err) {
          for (const btx of Object.values(indexTxMap)) {
            await btx.rollback()
          }
          throw err
        }
      }

      this.pendingBackfillFields = []
      return backfilledCount
    }, tx)
  }

  /**
   * Convert CreateIndexOption to IndexMetaConfig for metadata storage.
   */
  toIndexMetaConfig(option: CreateIndexOption<T>): IndexMetaConfig {
    if (!option || typeof option !== 'object') {
      throw new Error('Index option must be a non-null object')
    }
    if (!option.type) {
      throw new Error('Index option must have a "type" property ("btree" or "fts")')
    }

    if (option.type === 'btree') {
      if (!Array.isArray(option.fields) || option.fields.length === 0) {
        throw new Error('btree index requires a non-empty "fields" array')
      }
      for (let i = 0, len = option.fields.length; i < len; i++) {
        if (
          typeof option.fields[i] !== 'string' ||
          (option.fields[i] as string).length === 0
        ) {
          throw new Error(`btree index "fields[${i}]" must be a non-empty string, got: ${JSON.stringify(option.fields[i])}`)
        }
      }
      return {
        type: 'btree',
        fields: option.fields as string[]
      }
    }

    if (option.type === 'fts') {
      if (typeof option.fields !== 'string' || option.fields.length === 0) {
        throw new Error(`fts index requires a non-empty string "fields", got: ${JSON.stringify(option.fields)}`)
      }
      if (option.tokenizer === 'ngram') {
        if (typeof option.gramSize !== 'number' || option.gramSize < 1) {
          throw new Error(`fts ngram index requires a positive "gramSize" number, got: ${JSON.stringify(option.gramSize)}`)
        }
        return {
          type: 'fts',
          fields: option.fields as string,
          tokenizer: 'ngram',
          gramSize: option.gramSize
        }
      }
      return {
        type: 'fts',
        fields: option.fields as string,
        tokenizer: 'whitespace'
      }
    }
    throw new Error(`Unknown index type: ${(option as any).type}`)
  }

  /**
   * Get all field names from an IndexMetaConfig.
   */
  getFieldsFromConfig(config: IndexMetaConfig): string[] {
    if (config.type === 'btree') {
      return config.fields
    }
    if (config.type === 'fts') {
      return [config.fields]
    }
    return []
  }

  /**
   * Get the primary field of an index.
   */
  getPrimaryField(config: IndexMetaConfig): string {
    if (config.type === 'btree') {
      return config.fields[0]
    }
    return config.fields
  }

  /**
   * Create B+Tree value string for indexing a document
   */
  getIndexValue(config: IndexMetaConfig, flatDoc: FlattenedDocumentJSON): Primitive | Primitive[] | undefined {
    if (config.type !== 'btree') return undefined
    if (config.fields.length === 1) {
      const v = flatDoc[config.fields[0]]
      return v === undefined ? undefined : v
    }
    const values: Primitive[] = []
    for (let i = 0, len = config.fields.length; i < len; i++) {
      const v = flatDoc[config.fields[i]]
      if (v === undefined) return undefined
      values.push(v)
    }
    return values
  }

  /**
   * Get FTSConfig from IndexMetaConfig
   */
  getFtsConfig(config: IndexMetaConfig): FTSConfig | null {
    if (config.type !== 'fts') return null
    if (config.tokenizer === 'ngram') {
      return { type: 'fts', tokenizer: 'ngram', gramSize: config.gramSize }
    }
    return { type: 'fts', tokenizer: 'whitespace' }
  }

  getTokenKey(pk: number, token: string): string {
    return pk + ':' + token
  }
}
