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
  DocumentDataplyQuery,
  DocumentDataplyCondition,
  DocumentDataplyQueryOptions,
  FTSConfig,
  CreateIndexOption,
  IndexMetaConfig,
  FinalFlatten
} from '../types'
import {
  type BPTreeCondition,
  DataplyAPI,
  Transaction,
  BPTreeAsync,
  BPTreeAsyncTransaction
} from 'dataply'
import { DocumentSerializeStrategyAsync } from './bptree/documentStrategy'
import { DocumentValueComparator } from './bptree/documentComparator'
import { catchPromise } from '../utils/catchPromise'
import { BinaryHeap } from '../utils/heap'
import { tokenize } from '../utils/tokenizer'

export class DocumentDataplyAPI<T extends DocumentJSON> extends DataplyAPI {
  declare runWithDefault
  declare runWithDefaultWrite
  declare streamWithDefault

  indices: DocumentDataplyInnerMetadata['indices'] = {}
  readonly trees: Map<string, BPTreeAsync<string | number, DataplyTreeValue<Primitive>>> = new Map()
  readonly comparator = new DocumentValueComparator()
  private pendingBackfillFields: string[] = []
  private _initialized = false

  readonly indexedFields: Set<string>

  /**
   * Registered indices via createIndex() (before init)
   * Key: index name, Value: index configuration
   */
  private readonly pendingCreateIndices: Map<string, CreateIndexOption<T>> = new Map()

  /**
   * Resolved index configurations after init.
   * Key: index name, Value: index config (from metadata)
   */
  private registeredIndices: Map<string, IndexMetaConfig> = new Map()

  /**
   * Maps field name → index names that cover this field.
   * Used for query resolution.
   */
  private fieldToIndices: Map<string, string[]> = new Map()

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

  constructor(file: string, options: DocumentDataplyOptions) {
    super(file, options)
    this.trees = new Map()

    // _id는 항상 포함
    this.indexedFields = new Set(['_id'])

    this.hook.onceAfter('init', async (tx, isNewlyCreated) => {
      if (isNewlyCreated) {
        await this.initializeDocumentFile(tx)
      }
      if (!(await this.verifyDocumentFile(tx))) {
        throw new Error('Document metadata verification failed')
      }
      const metadata = await this.getDocumentInnerMetadata(tx)

      // _id 인덱스는 항상 자동 등록
      const targetIndices: Map<string, IndexMetaConfig> = new Map([
        ['_id', { type: 'btree', fields: ['_id'] }]
      ])

      // 1. 기존 메타데이터에 있는 인덱스들 로드
      for (const [name, info] of Object.entries(metadata.indices)) {
        targetIndices.set(name, info[1])
      }

      // 2. pendingCreateIndices에서 추가/업데이트된 설정 적용
      for (const [name, option] of this.pendingCreateIndices) {
        const config = this.toIndexMetaConfig(option)
        targetIndices.set(name, config)
      }

      const backfillTargets: string[] = []
      let isMetadataChanged = false

      for (const [indexName, config] of targetIndices) {
        const existingIndex = metadata.indices[indexName]

        // 새롭게 추가된 인덱스
        if (!existingIndex) {
          metadata.indices[indexName] = [-1, config]
          isMetadataChanged = true

          if (!isNewlyCreated) {
            backfillTargets.push(indexName)
          }
        }
        // 기존 인덱스 - 설정 갱신
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
        await this.updateDocumentInnerMetadata(metadata, tx)
      }

      this.indices = metadata.indices

      // registeredIndices 및 fieldToIndices 구축
      this.registeredIndices = new Map()
      this.fieldToIndices = new Map()

      for (const [indexName, config] of targetIndices) {
        this.registeredIndices.set(indexName, config)

        // indexedFields와 fieldToIndices 갱신
        const fields = this.getFieldsFromConfig(config)
        for (const field of fields) {
          this.indexedFields.add(field)
          if (!this.fieldToIndices.has(field)) {
            this.fieldToIndices.set(field, [])
          }
          this.fieldToIndices.get(field)!.push(indexName)
        }
      }

      // 트리 초기화
      for (const indexName of targetIndices.keys()) {
        if (metadata.indices[indexName]) {
          const tree = new BPTreeAsync<number, DataplyTreeValue<Primitive>>(
            new DocumentSerializeStrategyAsync<Primitive>(
              (this.rowTableEngine as any).order,
              this,
              this.txContext,
              indexName
            ),
            this.comparator as any
          )
          await tree.init()
          this.trees.set(indexName, tree as any)
        }
      }

      // 백필 대기 필드 저장
      this.pendingBackfillFields = backfillTargets
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

  /**
   * Register an index. If called before init(), queues it for processing during init.
   * If called after init(), immediately creates the tree, updates metadata, and backfills.
   */
  async registerIndex(name: string, option: CreateIndexOption<T>, tx?: Transaction): Promise<void> {
    if (!this._initialized) {
      // Pre-init: just queue it
      this.pendingCreateIndices.set(name, option)
      return
    }
    // Post-init: register immediately
    await this.registerIndexRuntime(name, option, tx)
  }

  /**
   * Register an index at runtime (after init).
   * Creates the tree, updates metadata, and backfills existing data.
   */
  private async registerIndexRuntime(name: string, option: CreateIndexOption<T>, tx?: Transaction): Promise<void> {
    const config = this.toIndexMetaConfig(option)

    // 이미 동일한 이름의 인덱스가 존재하면 스킵
    if (this.registeredIndices.has(name)) {
      throw new Error(`Index "${name}" already exists.`)
    }

    await this.runWithDefaultWrite(async (tx) => {
      // 1. 메타데이터 갱신
      const metadata = await this.getDocumentInnerMetadata(tx)
      metadata.indices[name] = [-1, config]
      await this.updateDocumentInnerMetadata(metadata, tx)
      this.indices = metadata.indices

      // 2. registeredIndices / fieldToIndices / indexedFields 갱신
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

      // 3. B+tree 생성
      const tree = new BPTreeAsync<number, DataplyTreeValue<Primitive>>(
        new DocumentSerializeStrategyAsync<Primitive>(
          (this.rowTableEngine as any).order,
          this,
          this.txContext,
          name
        ),
        this.comparator as any
      )
      await tree.init()
      this.trees.set(name, tree as any)

      // 4. 기존 데이터 백필
      if (metadata.lastId > 0) {
        this.pendingBackfillFields = [name]
        await this.backfillIndices(tx)
      }
    }, tx)
  }

  /**
   * Drop (remove) a named index.
   * Removes the index from metadata, in-memory maps, and trees.
   * The '_id' index cannot be dropped.
   * @param name The name of the index to drop
   */
  async dropIndex(name: string, tx?: Transaction): Promise<void> {
    if (name === '_id') {
      throw new Error('Cannot drop the "_id" index.')
    }
    if (!this._initialized) {
      // Pre-init: just remove from pending
      this.pendingCreateIndices.delete(name)
      return
    }
    if (!this.registeredIndices.has(name)) {
      throw new Error(`Index "${name}" does not exist.`)
    }

    await this.runWithDefaultWrite(async (tx) => {
      const config = this.registeredIndices.get(name)!

      // 1. 메타데이터에서 제거
      const metadata = await this.getDocumentInnerMetadata(tx)
      delete metadata.indices[name]
      await this.updateDocumentInnerMetadata(metadata, tx)
      this.indices = metadata.indices

      // 2. registeredIndices에서 제거
      this.registeredIndices.delete(name)

      // 3. fieldToIndices / indexedFields 갱신
      const fields = this.getFieldsFromConfig(config)
      for (let i = 0; i < fields.length; i++) {
        const field = fields[i]
        const indexNames = this.fieldToIndices.get(field)
        if (indexNames) {
          const filtered = indexNames.filter(n => n !== name)
          if (filtered.length === 0) {
            this.fieldToIndices.delete(field)
            // 다른 인덱스가 이 필드를 커버하지 않으면 indexedFields에서도 제거
            if (field !== '_id') {
              this.indexedFields.delete(field)
            }
          }
          else {
            this.fieldToIndices.set(field, filtered)
          }
        }
      }

      // 4. 트리 제거
      this.trees.delete(name)
    }, tx)
  }

  /**
   * Convert CreateIndexOption to IndexMetaConfig for metadata storage.
   */
  private toIndexMetaConfig(option: CreateIndexOption<T>): IndexMetaConfig {
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
  private getFieldsFromConfig(config: IndexMetaConfig): string[] {
    if (config.type === 'btree') {
      return config.fields
    }
    if (config.type === 'fts') {
      return [config.fields]
    }
    return []
  }

  /**
   * Get the primary field of an index (the field used as tree key).
   * For btree: first field in fields array.
   * For fts: the single field.
   */
  private getPrimaryField(config: IndexMetaConfig): string {
    if (config.type === 'btree') {
      return config.fields[0]
    }
    return config.fields
  }

  /**
   * 인덱스 config에 따라 B+tree에 저장할 v 값을 생성합니다.
   * - 단일 필드 btree: Primitive (단일 값)
   * - 복합 필드 btree: Primitive[] (필드 순서대로 배열)
   * - fts: 별도 처리 (이 메서드 사용 안 함)
   * @returns undefined면 해당 문서에 필수 필드가 없으므로 인덱싱 스킵
   */
  private getIndexValue(config: IndexMetaConfig, flatDoc: FlattenedDocumentJSON): Primitive | Primitive[] | undefined {
    if (config.type !== 'btree') return undefined
    if (config.fields.length === 1) {
      const v = flatDoc[config.fields[0]]
      return v === undefined ? undefined : v
    }
    // 복합 인덱스: 모든 필드 값을 배열로 구성
    const values: Primitive[] = []
    for (let i = 0, len = config.fields.length; i < len; i++) {
      const v = flatDoc[config.fields[i]]
      if (v === undefined) return undefined
      values.push(v)
    }
    return values
  }

  /**
   * Get FTSConfig from IndexMetaConfig (for tokenizer compatibility).
   */
  private getFtsConfig(config: IndexMetaConfig): FTSConfig | null {
    if (config.type !== 'fts') return null
    if (config.tokenizer === 'ngram') {
      return { type: 'fts', tokenizer: 'ngram', gramSize: config.gramSize }
    }
    return { type: 'fts', tokenizer: 'whitespace' }
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
   * This method should be called after `init()`.
   *
   * @returns Number of documents that were backfilled
   */
  async backfillIndices(tx?: Transaction): Promise<number> {
    return this.runWithDefaultWrite(async (tx) => {
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

      // 대상 인덱스당 하나의 트랜잭션 생성
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

      // 모든 행을 스캔하여 문서 찾기 (1번 행은 메타데이터, 2번 행 이후는 트리 헤드 또는 문서)
      for await (const [k, complexValue] of stream) {
        const doc = await this.getDocument(k as number, tx)
        if (!doc) continue
        const flatDoc = this.flattenDocument(doc)

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

        // 성능 및 메모리 부하 방지를 위해 B+ Tree 트랜잭션을 일정 단위(CHUNK)로 커밋
        // Engine의 외부 tx가 유지되므로 부분 커밋 시 에러가 나더라도 Database 전체 롤백은 안전함
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

          // 커밋 후 다음 청크 데이터를 받을 새로운 B+ Tree 트랜잭션 재생성
          for (const indexName of backfillTargets) {
            const tree = this.trees.get(indexName)
            if (tree && indexName !== '_id') {
              indexTxMap[indexName] = await tree.createTransaction()
            }
          }
          chunkCount = 0
        }
      }

      // 남아있는 트랜잭션 커밋
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
    for (const name of this.registeredIndices.keys()) {
      if (name !== '_id') {
        indices.push(name)
      }
    }
    return {
      pageSize: metadata.pageSize,
      pageCount: metadata.pageCount,
      rowCount: metadata.rowCount,
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

  /**
   * Transforms a query object into a verbose query object
   * @param query The query object to transform
   * @returns The verbose query object
   */
  verboseQuery<
    U extends Partial<DocumentDataplyQuery<T>>,
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
   * B-Tree 타입 인덱스의 선택도를 평가하고 트리에 부여할 조건을 산출합니다.
   * 필드 매칭 여부를 검사하고, 연속된(Prefix) 조건에 대해 점수를 부여하며 Start/End 바운드를 구성합니다.
   * 
   * @param indexName 평가할 인덱스의 이름 (예: idx_nickname_createdat)
   * @param config 등록된 인덱스의 설정 객체
   * @param query 쿼리 객체
   * @param queryFields 쿼리에 포함된 필드 목록 집합
   * @param treeTx 조회를 수행할 B-Tree 트랜잭션 객체
   * @param orderByField 정렬에 사용할 필드명 (옵션)
   * @returns B-Tree 인덱스 후보 정보 (조건, 점수, 커버된 필드 등), 적합하지 않으면 null
   */
  private evaluateBTreeCandidate<
    U extends Partial<DocumentDataplyQuery<T>>,
    V extends DataplyTreeValue<U>
  >(
    indexName: string,
    config: any,
    query: Partial<DocumentDataplyQuery<V>>,
    queryFields: Set<string>,
    treeTx: BPTreeAsync<string | number, V>,
    orderByField?: string
  ) {
    const primaryField = config.fields[0]
    // 인덱스의 첫 번째 필드가 쿼리에 포함되어 있지 않으면 이 인덱스로 트리 탐색 불가
    if (!queryFields.has(primaryField)) return null

    const builtCondition: Record<string, any> = {}
    let score = 0
    let isConsecutive = true // 복합 인덱스 필드들이 연속적으로 매칭되는지 추적
    const coveredFields: string[] = []

    // B-Tree 트래버스를 위한 탐색 구간(Bound) 설정용 배열
    const compositeVerifyFields: string[] = []
    const startValues: any[] = []
    const endValues: any[] = []
    let startOperator: string | null = null
    let endOperator: string | null = null

    // 인덱스에 정의된 필드 순서대로 쿼리 조건을 확인하여 점수 산출
    for (let i = 0, len = config.fields.length; i < len; i++) {
      const field = config.fields[i]

      // 해당 필드가 쿼리에 없다면, 이후 필드들은 Prefix 규칙에 의해 트리를 좁히는 데 사용할 수 없음
      if (!queryFields.has(field)) {
        isConsecutive = false
        continue
      }

      coveredFields.push(field)
      score += 1 // 기본적으로 조건 매칭 하나당 1점 부여

      if (isConsecutive) {
        const cond = query[field as keyof typeof query] as any
        if (cond !== undefined) {
          let isBounded = false // Tree Bound 연산으로 완벽히 커버되었는지를 나타내는 플래그

          // 조건 값이 객체가 아니거나 (직접 매칭) equal 연산인 경우 (동일값 검색)
          if (typeof cond !== 'object' || cond === null) {
            score += 100
            startValues.push(cond)
            endValues.push(cond)
            startOperator = 'primaryGte'
            endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryEqual' in cond || 'equal' in cond) {
            const val = cond.primaryEqual?.v ?? cond.equal?.v ?? cond.primaryEqual ?? cond.equal
            score += 100
            startValues.push(val)
            endValues.push(val)
            startOperator = 'primaryGte'
            endOperator = 'primaryLte'
            isBounded = true
          }
          // 부등호 연산(범위 검색)이 등장하면, 그 이후의 필드들은 Prefix 조건으로 묶일 수 없음
          else if ('primaryGte' in cond || 'gte' in cond) {
            const val = cond.primaryGte?.v ?? cond.gte?.v ?? cond.primaryGte ?? cond.gte
            score += 50
            isConsecutive = false // 연속성 단절점
            startValues.push(val)
            startOperator = 'primaryGte'
            if (endValues.length > 0) endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryGt' in cond || 'gt' in cond) {
            const val = cond.primaryGt?.v ?? cond.gt?.v ?? cond.primaryGt ?? cond.gt
            score += 50
            isConsecutive = false
            startValues.push(val)
            startOperator = 'primaryGt'
            if (endValues.length > 0) endOperator = 'primaryLte'
            isBounded = true
          }
          else if ('primaryLte' in cond || 'lte' in cond) {
            const val = cond.primaryLte?.v ?? cond.lte?.v ?? cond.primaryLte ?? cond.lte
            score += 50
            isConsecutive = false
            endValues.push(val)
            endOperator = 'primaryLte'
            if (startValues.length > 0) startOperator = 'primaryGte'
            isBounded = true
          }
          else if ('primaryLt' in cond || 'lt' in cond) {
            const val = cond.primaryLt?.v ?? cond.lt?.v ?? cond.primaryLt ?? cond.lt
            score += 50
            isConsecutive = false
            endValues.push(val)
            endOperator = 'primaryLt'
            if (startValues.length > 0) startOperator = 'primaryGte'
            isBounded = true
          }
          // OR, LIKE 등 Tree Bound로 변환할 수 없는 복잡한 조건들
          else if ('primaryOr' in cond || 'or' in cond) {
            score += 20
            isConsecutive = false
          }
          else if ('like' in cond) {
            score += 15
            isConsecutive = false
          }
          else {
            score += 10
            isConsecutive = false
          }

          // Tree Bound로 완전히 걸러내지 못하는 조건이라면(예: like), 메모리상 재검증 과정에 추가함
          if (!isBounded && field !== primaryField) {
            compositeVerifyFields.push(field)
          }
        }
      } else {
        // 프리픽스 연속성이 끊긴 뒤의 쿼리 필드는 모두 메모리 재검증 대상
        if (field !== primaryField) {
          compositeVerifyFields.push(field)
        }
      }
    }

    // 분석을 바탕으로 하여 B-Tree 트리 탐색을 위한 최종 Bound Condition 객체를 생성
    if (coveredFields.length === 1 && config.fields.length === 1) {
      // 단일 인덱스인 경우 쿼리의 포맷 구조체 그대로를 바인딩
      Object.assign(builtCondition, query[primaryField as keyof typeof query])
    }
    else {
      // 복합 인덱스 바운드 결합
      if (startOperator && startValues.length > 0) {
        builtCondition[startOperator] = { v: startValues.length === 1 ? startValues[0] : startValues }
      }
      if (endOperator && endValues.length > 0) {
        // start와 end가 배열 전체에 걸쳐 완벽히 일치하면 primaryEqual로 단일화 (최적화)
        if (startOperator && startValues.length === endValues.length && startValues.every((val: any, i: any) => val === endValues[i])) {
          delete builtCondition[startOperator]
          builtCondition['primaryEqual'] = { v: startValues.length === 1 ? startValues[0] : startValues }
        }
        else {
          builtCondition[endOperator] = { v: endValues.length === 1 ? endValues[0] : endValues }
        }
      }
      // 특수한 연산자만 있어서 바운드가 잡히지 않았을 경우의 Fallback
      if (Object.keys(builtCondition).length === 0) {
        Object.assign(builtCondition, query[primaryField as keyof typeof query] || {})
      }
    }

    // 정렬(orderBy) 요청을 해당 인덱스가 커버할 수 있는지 검사 (추가 비용 없이 메모리 정렬 없는 빠른 검색 가능 여부 확인)
    let isIndexOrderSupported = false
    if (orderByField) {
      for (let i = 0, len = config.fields.length; i < len; i++) {
        const field = config.fields[i]
        if (field === orderByField) {
          isIndexOrderSupported = true
          break
        }
        const cond = query[field as keyof typeof query] as any
        let isExactMatch = false
        if (cond !== undefined) {
          if (typeof cond !== 'object' || cond === null) isExactMatch = true
          else if ('primaryEqual' in cond || 'equal' in cond) isExactMatch = true
        }
        if (!isExactMatch) break
      }
      if (isIndexOrderSupported) {
        score += 200
      }
    }

    return {
      tree: treeTx,
      condition: builtCondition as any,
      field: primaryField,
      indexName,
      isFtsMatch: false,
      score,
      compositeVerifyFields,
      coveredFields,
      isIndexOrderSupported
    } as const
  }

  /**
   * FTS (Full Text Search) 타입 인덱스의 선택도를 평가합니다.
   * 'match' 연산자가 쿼리에 존재하는지 확인하고, 검색용 토큰으로 분해(tokenize)하여 점수를 매깁니다.
   * 
   * @param indexName 평가할 인덱스의 이름
   * @param config 등록된 인덱스의 설정 객체
   * @param query 쿼리 객체
   * @param queryFields 쿼리에 포함된 필드 목록 집합
   * @param treeTx 조회를 수행할 B-Tree 트랜잭션 객체
   * @returns FTS 인덱스 후보 정보 (조건, 점수, 분석된 토큰 등), 적합하지 않으면 null
   */
  private evaluateFTSCandidate<
    U extends Partial<DocumentDataplyQuery<T>>,
    V extends DataplyTreeValue<U>
  >(
    indexName: string,
    config: any,
    query: Partial<DocumentDataplyQuery<V>>,
    queryFields: Set<string>,
    treeTx: BPTreeAsync<string | number, V>
  ) {
    const field = config.fields
    // FTS 인덱스 대상 필드가 쿼리에 포함되지 않았으면 검색에 사용할 수 없음
    if (!queryFields.has(field)) return null

    const condition = query[field as keyof typeof query] as Partial<DocumentDataplyCondition<U>>
    // FTS는 반드시 'match' 연산자를 사용해야만 동작함
    if (!condition || typeof condition !== 'object' || !('match' in condition)) return null

    // 형태소 분석(토크나이징) 설정 가져오기
    const ftsConfig = this.getFtsConfig(config as any)
    const matchTokens = ftsConfig ? tokenize((condition as any).match as string, ftsConfig) : []

    return {
      tree: treeTx,
      condition: condition as any,
      field,
      indexName,
      isFtsMatch: true,
      matchTokens,
      score: 90, // FTS 쿼리는 기본적인 B-Tree 단일 검색(대략 101점)보다는 우선순위를 조금 낮게 가져가도록 90점 부여
      compositeVerifyFields: [],
      coveredFields: [field],
      isIndexOrderSupported: false
    } as const
  }

  /**
   * Choose the best index (driver) for the given query.
   * Scores each index based on field coverage and condition type.
   *
   * @param query The verbose query conditions
   * @param orderByField Optional field name for orderBy optimization
   * @returns Driver and other candidates for query execution
   */
  async getSelectivityCandidate<
    U extends Partial<DocumentDataplyQuery<T>>,
    V extends DataplyTreeValue<U>
  >(
    query: Partial<DocumentDataplyQuery<V>>,
    orderByField?: string
  ): Promise<{
    driver: ({
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: false,
      isIndexOrderSupported: boolean
    } | {
      tree: BPTreeAsync<string, V>
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: true,
      matchTokens: string[],
      isIndexOrderSupported: boolean
    }),
    others: ({
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: false,
      isIndexOrderSupported: boolean
    } | {
      tree: BPTreeAsync<string, V>
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: true,
      matchTokens: string[],
      isIndexOrderSupported: boolean
    })[],
    // 복합 인덱스의 non-primary 필드 검증 조건
    compositeVerifyConditions: {
      field: string,
      condition: any
    }[],
    rollback: () => void
  } | null> {
    const queryFields = new Set(Object.keys(query))
    const candidates: {
      tree: BPTreeAsync<string | number, V>,
      condition: Partial<DocumentDataplyCondition<U>>,
      field: string,
      indexName: string,
      isFtsMatch: boolean,
      matchTokens?: string[],
      score: number,
      compositeVerifyFields: string[],
      coveredFields: string[],
      isIndexOrderSupported: boolean
    }[] = []

    for (const [indexName, config] of this.registeredIndices) {
      const tree = this.trees.get(indexName)
      if (!tree) continue

      if (config.type === 'btree') {
        const treeTx = await tree.createTransaction()
        const candidate = this.evaluateBTreeCandidate(
          indexName,
          config as any,
          query,
          queryFields,
          treeTx as unknown as BPTreeAsync<string | number, V>,
          orderByField
        )
        if (candidate) candidates.push(candidate as any)
      }
      else if (config.type === 'fts') {
        const treeTx = await tree.createTransaction()
        const candidate = this.evaluateFTSCandidate(
          indexName,
          config as any,
          query,
          queryFields,
          treeTx as unknown as BPTreeAsync<string | number, V>
        )
        if (candidate) candidates.push(candidate as any)
      }
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

    // 점수 기준 내림차순 정렬, 동점일 경우 필드 개수가 적은 인덱스를 선호
    candidates.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score

      const aConfig = this.registeredIndices.get(a.indexName)
      const bConfig = this.registeredIndices.get(b.indexName)
      const aFieldCount = aConfig ? (Array.isArray(aConfig.fields) ? aConfig.fields.length : 1) : 0
      const bFieldCount = bConfig ? (Array.isArray(bConfig.fields) ? bConfig.fields.length : 1) : 0

      return aFieldCount - bFieldCount
    })

    const driver = candidates[0]
    const driverCoveredFields = new Set(driver.coveredFields)
    const others = candidates.slice(1).filter(c => !driverCoveredFields.has(c.field))

    // 드라이버의 복합 인덱스 non-primary 필드 조건
    const compositeVerifyConditions: { field: string, condition: any }[] = []
    for (let i = 0, len = driver.compositeVerifyFields.length; i < len; i++) {
      const field = driver.compositeVerifyFields[i]
      if (query[field]) {
        compositeVerifyConditions.push({ field, condition: query[field] })
      }
    }

    return {
      driver: driver as any,
      others: others as any,
      compositeVerifyConditions,
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

  private async *applyCandidateByFTSStream<V>(
    candidate: {
      tree: BPTreeAsync<string, DataplyTreeValue<V>>,
      condition: Partial<DocumentDataplyCondition<Partial<DocumentDataplyQuery<T>>>>,
    },
    matchedTokens: string[],
    filterValues?: Set<number>,
    order?: 'asc' | 'desc'
  ): AsyncIterableIterator<number> {
    const keys = new Set<number>()
    for (let i = 0, len = matchedTokens.length; i < len; i++) {
      const token = matchedTokens[i]
      for await (const pair of candidate.tree.whereStream(
        { primaryEqual: { v: token } } as any,
        { order }
      )) {
        const pk = (pair[1] as any).k as number
        if (filterValues && !filterValues.has(pk)) continue
        if (!keys.has(pk)) {
          keys.add(pk)
          yield pk
        }
      }
    }
  }

  /**
   * 특정 인덱스 후보를 조회하여 PK 집합을 필터링합니다.
   */
  private applyCandidateStream<V>(
    candidate: {
      tree: BPTreeAsync<number, V>,
      condition: Partial<DocumentDataplyCondition<Partial<DocumentDataplyQuery<T>>>>,
    },
    filterValues?: Set<number>,
    order?: 'asc' | 'desc'
  ): AsyncIterableIterator<number> {
    return candidate.tree.keysStream(
      candidate.condition as any,
      { filterValues, order }
    ) as AsyncIterableIterator<number>
  }

  /**
   * 쿼리와 인덱스 선택을 기반으로 기본 키(Primary Keys)를 가져옵니다.
   * 쿼리 최적화를 통합하기 위한 내부 공통 메서드입니다.
   */
  async getKeys(
    query: Partial<DocumentDataplyQuery<T>>,
    orderBy?: string,
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
    // Driver가 지정된 orderBy 순서를 지원(보장)하는지 확인합니다.
    const useIndexOrder = orderBy === undefined || driver.isIndexOrderSupported
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
        const stream = this.applyCandidateByFTSStream(
          candidate as any,
          candidate.matchTokens,
          keys,
          currentOrder
        )
        keys = new Set()
        for await (const pk of stream) keys.add(pk)
      }
      else {
        const stream = this.applyCandidateStream(candidate as any, keys, currentOrder)
        keys = new Set()
        for await (const pk of stream) keys.add(pk)
      }
    }

    rollback()
    return new Float64Array(Array.from(keys || []))
  }

  /**
   * 드라이버 인덱스만으로 PK 스트림을 가져옵니다. (교집합 없이)
   * selectDocuments에서 사용하며, 나머지 조건(others)은 스트리밍 중 tree.verify()로 검증합니다.
   * @returns 드라이버 키 스트림, others 후보 목록, rollback 함수. 또는 null.
   */
  private async getDriverKeys(
    query: Partial<DocumentDataplyQuery<T>>,
    orderBy?: string,
    sortOrder: 'asc' | 'desc' = 'asc'
  ): Promise<{
    keysStream: AsyncIterableIterator<number>,
    others: {
      tree: BPTreeAsync<string | number, DataplyTreeValue<Primitive>>,
      condition: any,
      field: string,
      indexName: string,
      isFtsMatch: boolean,
      matchTokens?: string[]
    }[],
    compositeVerifyConditions: {
      field: string,
      condition: any
    }[],
    isDriverOrderByField: boolean,
    rollback: () => void,
  } | null> {
    const isQueryEmpty = Object.keys(query).length === 0
    const normalizedQuery = isQueryEmpty ? { _id: { gte: 0 } } : query
    const selectivity = await this.getSelectivityCandidate(
      this.verboseQuery(normalizedQuery as any),
      orderBy as string
    )

    if (!selectivity) return null

    const { driver, others, compositeVerifyConditions, rollback } = selectivity

    // 드라이버의 정렬 순서 결정
    const useIndexOrder = orderBy === undefined || driver.isIndexOrderSupported
    const currentOrder = useIndexOrder ? sortOrder : undefined

    // 드라이버만으로 키 스트림을 가져옴
    let keysStream: AsyncIterableIterator<number>
    if (
      driver.isFtsMatch &&
      driver.matchTokens &&
      driver.matchTokens.length > 0
    ) {
      keysStream = this.applyCandidateByFTSStream(
        driver as any,
        driver.matchTokens,
        undefined,
        currentOrder
      )
    }
    else {
      keysStream = this.applyCandidateStream(driver as any, undefined, currentOrder)
    }

    return {
      keysStream,
      others: others as any,
      compositeVerifyConditions,
      isDriverOrderByField: useIndexOrder,
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
    return this.runWithDefaultWrite(async (tx) => {
      const { pk: dpk, document: dataplyDocument } = await this.insertDocumentInternal(document, tx)
      const flattenDocument = this.flattenDocument(dataplyDocument)

      // 등록된 인덱스별로 인덱싱
      for (const [indexName, config] of this.registeredIndices) {
        const tree = this.trees.get(indexName)
        if (!tree) continue

        if (config.type === 'fts') {
          const primaryField = this.getPrimaryField(config)
          const v = flattenDocument[primaryField]
          if (v === undefined || typeof v !== 'string') continue
          const ftsConfig = this.getFtsConfig(config)
          const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
          for (let i = 0, len = tokens.length; i < len; i++) {
            const token = tokens[i]
            const keyToInsert = this.getTokenKey(dpk, token as string)
            const [error] = await catchPromise(tree.insert(keyToInsert, { k: dpk, v: token }))
            if (error) throw error
          }
        }
        else {
          const indexVal = this.getIndexValue(config, flattenDocument)
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
      for (const [indexName, config] of this.registeredIndices) {
        const tree = this.trees.get(indexName)
        if (!tree) continue

        const treeTx = await tree.createTransaction()
        const batchInsertData: [string | number, DataplyTreeValue<Primitive>][] = []

        if (config.type === 'fts') {
          const primaryField = this.getPrimaryField(config)
          const ftsConfig = this.getFtsConfig(config)
          for (let i = 0, len = flattenedData.length; i < len; i++) {
            const item = flattenedData[i]
            const v = item.data[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, tLen = tokens.length; j < tLen; j++) {
              const token = tokens[j]
              batchInsertData.push([this.getTokenKey(item.pk, token as string), { k: item.pk, v: token }])
            }
          }
        }
        else {
          for (let i = 0, len = flattenedData.length; i < len; i++) {
            const item = flattenedData[i]
            const indexVal = this.getIndexValue(config, item.data)
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
    const pks = await this.getKeys(query)
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
        const config = this.registeredIndices.get(indexName)
        if (!config) continue

        if (config.type === 'fts') {
          const primaryField = this.getPrimaryField(config)
          const oldV = oldFlatDoc[primaryField]
          const newV = newFlatDoc[primaryField]
          if (oldV === newV) continue
          const ftsConfig = this.getFtsConfig(config)

          // 기존 FTS 토큰 삭제
          if (typeof oldV === 'string') {
            const oldTokens = ftsConfig ? tokenize(oldV, ftsConfig) : [oldV]
            for (let j = 0, jLen = oldTokens.length; j < jLen; j++) {
              await treeTx.delete(this.getTokenKey(pk, oldTokens[j] as string), { k: pk, v: oldTokens[j] })
            }
          }
          // 새 FTS 토큰 삽입
          if (typeof newV === 'string') {
            const newTokens = ftsConfig ? tokenize(newV, ftsConfig) : [newV]
            const batchInsertData: [string | number, DataplyTreeValue<Primitive>][] = []
            for (let j = 0, jLen = newTokens.length; j < jLen; j++) {
              batchInsertData.push([this.getTokenKey(pk, newTokens[j] as string), { k: pk, v: newTokens[j] }])
            }
            await treeTx.batchInsert(batchInsertData)
          }
        }
        else {
          const oldIndexVal = this.getIndexValue(config, oldFlatDoc)
          const newIndexVal = this.getIndexValue(config, newFlatDoc)

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
      const pks = await this.getKeys(query)
      let deletedCount = 0

      for (let i = 0, len = pks.length; i < len; i++) {
        const pk = pks[i]
        const doc = await this.getDocument(pk, tx)
        if (!doc) continue

        const flatDoc = this.flattenDocument(doc)

        // 모든 인덱스 트리에서 삭제
        for (const [indexName, tree] of this.trees) {
          const config = this.registeredIndices.get(indexName)
          if (!config) continue

          if (config.type === 'fts') {
            const primaryField = this.getPrimaryField(config)
            const v = flatDoc[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const ftsConfig = this.getFtsConfig(config)
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, jLen = tokens.length; j < jLen; j++) {
              await tree.delete(this.getTokenKey(pk, tokens[j] as string), { k: pk, v: tokens[j] })
            }
          } else {
            const indexVal = this.getIndexValue(config, flatDoc)
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
      const pks = await this.getKeys(query)
      return pks.length
    }, tx)
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
   * 복합 인덱스의 non-primary 필드에 대해 문서가 유효한지 검증합니다.
   */
  private verifyCompositeConditions(
    doc: DataplyDocument<T>,
    conditions: { field: string, condition: any }[]
  ): boolean {
    if (conditions.length === 0) return true
    const flatDoc = this.flattenDocument(doc)
    for (let i = 0, len = conditions.length; i < len; i++) {
      const { field, condition } = conditions[i]
      const docValue = flatDoc[field]
      if (docValue === undefined) return false

      // verbose 조건 형태를 다시 역변환하여 비교
      const treeValue: DataplyTreeValue<Primitive> = { k: doc._id, v: docValue }
      // tree.verify() 대신 직접 비교
      if (!this.verifyValue(docValue, condition)) return false
    }
    return true
  }

  /**
   * 단일 값에 대해 verbose 조건을 검증합니다.
   */
  private verifyValue(value: Primitive, condition: any): boolean {
    if (typeof condition !== 'object' || condition === null) {
      // 직접 값 비교 (equal)
      return value === condition
    }
    // verbose 형태의 조건 검증
    if ('primaryEqual' in condition) {
      return value === condition.primaryEqual?.v
    }
    if ('primaryNotEqual' in condition) {
      return value !== condition.primaryNotEqual?.v
    }
    if ('primaryLt' in condition) {
      return value !== null && condition.primaryLt?.v !== undefined && value < condition.primaryLt.v
    }
    if ('primaryLte' in condition) {
      return value !== null && condition.primaryLte?.v !== undefined && value <= condition.primaryLte.v
    }
    if ('primaryGt' in condition) {
      return value !== null && condition.primaryGt?.v !== undefined && value > condition.primaryGt.v
    }
    if ('primaryGte' in condition) {
      return value !== null && condition.primaryGte?.v !== undefined && value >= condition.primaryGte.v
    }
    if ('primaryOr' in condition && Array.isArray(condition.primaryOr)) {
      return condition.primaryOr.some((c: any) => value === c?.v)
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
   * Prefetch 방식으로 키 스트림을 청크 단위로 조회하여 문서를 순회합니다.
   * FTS 검증, 복합 인덱스 검증, others 후보에 대한 tree.verify() 검증을 통과한 문서만 yield 합니다.
   */
  private async *processChunkedKeysWithVerify(
    keysStream: AsyncIterableIterator<number>,
    startIdx: number,
    initialChunkSize: number,
    limit: number,
    ftsConditions: { field: string, matchTokens: string[] }[],
    compositeVerifyConditions: { field: string, condition: any }[],
    others: {
      tree: BPTreeAsync<string | number, DataplyTreeValue<Primitive>>,
      condition: any,
      field: string,
      indexName: string,
      isFtsMatch: boolean,
      matchTokens?: string[]
    }[],
    tx: any
  ): AsyncGenerator<DataplyDocument<T>> {
    // others 중 FTS가 아닌 일반 조건만 verify 대상으로 분리
    const verifyOthers = others.filter(o => !o.isFtsMatch)
    const isFts = ftsConditions.length > 0
    const isCompositeVerify = compositeVerifyConditions.length > 0
    const isVerifyOthers = verifyOthers.length > 0
    const isInfiniteLimit = isFinite(limit)

    let currentChunkSize = isInfiniteLimit ? initialChunkSize : limit
    let chunk: number[] = []
    let chunkSize = 0
    let dropped = 0

    const processChunk = async (pks: number[]) => {
      const docs: DataplyDocument<T>[] = []
      const rawResults = await this.selectMany(new Float64Array(pks), false, tx)
      let chunkTotalSize = 0

      for (let j = 0, len = rawResults.length; j < len; j++) {
        const s = rawResults[j]
        if (!s) continue
        const doc = JSON.parse(s)
        chunkTotalSize += s.length * 2

        // FTS 검증
        if (isFts && !this.verifyFts(doc, ftsConditions)) continue

        // 복합 인덱스 non-primary 필드 검증
        if (
          isCompositeVerify &&
          this.verifyCompositeConditions(doc, compositeVerifyConditions) === false
        ) continue

        // others 조건 검증: 각 필드의 값을 tree.verify()로 확인
        if (isVerifyOthers) {
          const flatDoc = this.flattenDocument(doc)
          let passed = true
          for (let k = 0, kLen = verifyOthers.length; k < kLen; k++) {
            const other = verifyOthers[k]
            const fieldValue = flatDoc[other.field]
            if (fieldValue === undefined) {
              passed = false
              break
            }
            const treeValue: DataplyTreeValue<Primitive> = { k: doc._id, v: fieldValue }
            if (!other.tree.verify(treeValue, other.condition)) {
              passed = false
              break
            }
          }
          if (!passed) continue
        }

        docs.push(doc)
      }

      if (isInfiniteLimit) {
        currentChunkSize = this.adjustChunkSize(currentChunkSize, chunkTotalSize)
      }
      return docs
    }

    for await (const pk of keysStream) {
      if (dropped < startIdx) {
        dropped++
        continue
      }
      chunk.push(pk)
      chunkSize++
      if (chunkSize >= currentChunkSize) {
        const docs = await processChunk(chunk)
        for (let j = 0, dLen = docs.length; j < dLen; j++) yield docs[j]
        chunk = []
        chunkSize = 0
      }
    }

    if (chunkSize > 0) {
      const docs = await processChunk(chunk)
      for (let j = 0, dLen = docs.length; j < dLen; j++) yield docs[j]
    }
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
      const ftsConditions: { field: string, matchTokens: string[] }[] = []
      for (const field in query) {
        const q = query[field] as any
        if (
          q &&
          typeof q === 'object' &&
          'match' in q &&
          typeof q.match === 'string'
        ) {
          // 해당 필드를 커버하는 FTS 인덱스 찾기
          const indexNames = self.fieldToIndices.get(field) || []
          for (const indexName of indexNames) {
            const config = self.registeredIndices.get(indexName)
            if (config && config.type === 'fts') {
              const ftsConfig = self.getFtsConfig(config)
              if (ftsConfig) {
                ftsConditions.push({ field, matchTokens: tokenize(q.match, ftsConfig) })
              }
              break
            }
          }
        }
      }

      // 드라이버 인덱스만으로 PK 목록 조회
      const driverResult = await self.getDriverKeys(query, orderByField, sortOrder)
      if (!driverResult) return
      const { keysStream, others, compositeVerifyConditions, isDriverOrderByField, rollback } = driverResult
      const initialChunkSize = self.options.pageSize

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
            keysStream,
            0,
            initialChunkSize,
            Infinity,
            ftsConditions,
            compositeVerifyConditions,
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
          const hasFilters = ftsConditions.length > 0 || compositeVerifyConditions.length > 0 || others.length > 0
          const startIdx = hasFilters ? 0 : offset

          let yieldedCount = 0
          let skippedCount = hasFilters ? 0 : offset

          // offset부터 시작하여 limit개까지만 yield
          for await (const doc of self.processChunkedKeysWithVerify(
            keysStream,
            startIdx,
            initialChunkSize,
            limit,
            ftsConditions,
            compositeVerifyConditions,
            others,
            tx
          )) {
            if (skippedCount < offset) {
              skippedCount++
              continue
            }
            if (yieldedCount >= limit) break
            yield doc
            yieldedCount++
          }
        }
      }
      finally {
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
