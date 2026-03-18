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
import { BPTreePureAsync, Transaction, Logger } from 'dataply'
import { DocumentSerializeStrategyAsync } from './bptree/documentStrategy'
import { tokenize } from '../utils/tokenizer'
import { yieldEventLoop } from '../utils/eventLoopManager'

export class IndexManager<T extends DocumentJSON> {
  indices: DocumentDataplyInnerMetadata['indices'] = {}
  readonly trees: Map<string, BPTreePureAsync<string | number, DataplyTreeValue<Primitive>>> = new Map()
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
    private logger: Logger
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
      }
      else {
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
        const tree = new BPTreePureAsync<number, DataplyTreeValue<Primitive>>(
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

    await this.api.withWriteTransaction(async (tx) => {
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
      const tree = new BPTreePureAsync<number, DataplyTreeValue<Primitive>>(
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

    await this.api.withWriteTransaction(async (tx) => {
      const config = this.registeredIndices.get(name)!

      const metadata = await this.api.getDocumentInnerMetadata(tx)

      // 인덱스에 사용된 모든 B+Tree 노드 행을 삭제하여 저장소 행을 회수
      const indexInfo = metadata.indices[name]
      if (indexInfo) {
        const headPk = indexInfo[0]
        if (headPk !== -1) {
          const tree = this.trees.get(name)
          if (tree) {
            const strategy = (tree as any).strategy as import('./bptree/documentStrategy').DocumentSerializeStrategyAsync<Primitive>
            await strategy.clearAllNodes(headPk)
          }
        }
      }

      delete metadata.indices[name]
      await this.api.updateDocumentInnerMetadata(metadata, tx)
      this.indices = metadata.indices

      this.registeredIndices.delete(name)

      const fields = this.getFieldsFromConfig(config)
      for (let i = 0, len = fields.length; i < len; i++) {
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
    return this.api.withWriteTransaction(async (tx) => {
      if (this.pendingBackfillFields.length === 0) {
        return 0
      }

      const backfillTargets = this.pendingBackfillFields
      const metadata = await this.api.getDocumentInnerMetadata(tx)

      if (metadata.lastId === 0) {
        return 0
      }

      // Collect all entries per index for bulkLoad (empty tree optimization)
      const bulkData: Record<
        string,
        [number | string, DataplyTreeValue<Primitive>][]
      > = {}

      for (const indexName of backfillTargets) {
        const tree = this.trees.get(indexName)
        if (tree && indexName !== '_id') {
          bulkData[indexName] = []
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

      for await (const [k, complexValue] of stream) {
        const doc = await this.api.getDocument(k as number, tx)
        if (!doc) continue
        const flatDoc = this.api.flattenDocument(doc)

        for (let i = 0, len = backfillTargets.length; i < len; i++) {
          const indexName = backfillTargets[i]
          if (!(indexName in bulkData)) continue

          const config = this.registeredIndices.get(indexName)
          if (!config) continue

          if (config.type === 'fts') {
            const primaryField = this.getPrimaryField(config)
            const v = flatDoc[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const ftsConfig = this.getFtsConfig(config)
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, tLen = tokens.length; j < tLen; j++) {
              const token = tokens[j]
              const keyToInsert = this.getTokenKey(k as number, token as string)
              const entry = { k: k as number, v: token }
              bulkData[indexName].push([keyToInsert, entry])
            }
          }
          else {
            const indexVal = this.getIndexValue(config, flatDoc)
            if (indexVal === undefined) continue
            const entry = { k: k as number, v: indexVal }
            bulkData[indexName].push([k, entry as any])
          }
        }
        backfilledCount++
      }

      // Use bulkLoad for each new index (tree is guaranteed empty)
      for (const indexName of backfillTargets) {
        const tree = this.trees.get(indexName)
        if (!tree || indexName === '_id') continue
        const entries = bulkData[indexName]
        if (!entries || entries.length === 0) continue

        try {
          await tree.bulkLoad(entries)
        } catch (err) {
          this.logger.error(`Failed to bulk load index ${indexName}`, err)
          throw err
        }
        await yieldEventLoop()
      }

      this.pendingBackfillFields = []
      return backfilledCount
    }, tx)
  }

  /**
   * Rebuild specified indices by clearing existing tree data and rebuilding via bulkLoad.
   * If no index names are provided, all indices (except _id) are rebuilt.
   */
  async rebuildIndices(indexNames?: string[], tx?: Transaction): Promise<number> {
    const targets = indexNames ?? [...this.registeredIndices.keys()].filter(n => n !== '_id')

    for (const name of targets) {
      if (name === '_id') {
        throw new Error('Cannot rebuild the "_id" index.')
      }
      if (!this.registeredIndices.has(name)) {
        throw new Error(`Index "${name}" does not exist.`)
      }
    }

    if (targets.length === 0) return 0

    this.logger.debug(`Starting rebuild for indices: ${targets.join(', ')}`)

    return this.api.withWriteTransaction(async (tx) => {
      const metadata = await this.api.getDocumentInnerMetadata(tx)
      if (metadata.lastId === 0) return 0

      // 1. Collect entries from documents
      const bulkData: Record<
        string,
        [number | string, DataplyTreeValue<Primitive>][]
      > = {}
      for (const indexName of targets) {
        bulkData[indexName] = []
      }

      const idTree = this.trees.get('_id')
      if (!idTree) throw new Error('ID tree not found')

      let docCount = 0
      const stream = idTree.whereStream({ primaryGte: { v: 0 } })
      for await (const [k] of stream) {
        const doc = await this.api.getDocument(k as number, tx)
        if (!doc) continue
        const flatDoc = this.api.flattenDocument(doc)

        for (let i = 0, len = targets.length; i < len; i++) {
          const indexName = targets[i]
          const config = this.registeredIndices.get(indexName)
          if (!config) continue

          if (config.type === 'fts') {
            const primaryField = this.getPrimaryField(config)
            const v = flatDoc[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const ftsConfig = this.getFtsConfig(config)
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, tLen = tokens.length; j < tLen; j++) {
              const token = tokens[j]
              const keyToInsert = this.getTokenKey(k as number, token as string)
              bulkData[indexName].push([keyToInsert, { k: k as number, v: token }])
            }
          }
          else {
            const indexVal = this.getIndexValue(config, flatDoc)
            if (indexVal === undefined) continue
            bulkData[indexName].push([k, { k: k as number, v: indexVal } as any])
          }
        }
        docCount++
      }

      // 2. Clear existing trees and rebuild with bulkLoad
      const perIndexCapacity = Math.floor(this.api.options.pageCacheCapacity / Object.keys(this.api.indices).length)

      for (const indexName of targets) {
        // Reset head PK to -1 so readHead returns null → init creates fresh root
        metadata.indices[indexName][0] = -1
        await this.api.updateDocumentInnerMetadata(metadata, tx)

        // Create a new tree instance (old tree nodes become orphans in storage)
        const tree = new BPTreePureAsync<number, DataplyTreeValue<Primitive>>(
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

        // BulkLoad the collected entries
        const entries = bulkData[indexName]
        if (entries.length > 0) {
          try {
            await tree.bulkLoad(entries as any)
          } catch (err) {
            this.logger.error(`Failed to bulk load index ${indexName}`, err)
            throw err
          }
          await yieldEventLoop()
        }
      }

      return docCount
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
