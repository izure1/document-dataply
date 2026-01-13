import type { DataplyTreeValue, DocumentDataplyMetadata, DocumentDataplyOptions, DocumentJSON, FlattenedDocumentJSON, Primitive, DocumentDataplyQuery, FinalFlatten, DocumentDataplyCondition, DataplyDocument } from '../types'
import { DataplyAPI, Transaction, BPTreeAsync, type BPTreeCondition } from 'dataply'
import { DocumentSerializeStrategyAsync } from './bptree/documentStrategy'
import { DocumentValueComparator } from './bptree/documentComparator'

export class DocumentDataplyAPI<T extends DocumentJSON> extends DataplyAPI {
  declare runWithDefault

  treeHeads: DocumentDataplyMetadata['treeHeads'] = {}
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
      const metadata = await this.getDocumentMetadata(tx)
      this.treeHeads = metadata.treeHeads
      return tx
    })
  }

  async ensureTree<T extends Primitive>(field: string): Promise<BPTreeAsync<number, DataplyTreeValue<T>>> {
    if (!this.trees.has(field)) {
      const comparator = this.comparator
      const tree = new BPTreeAsync<number, DataplyTreeValue<T>>(
        new DocumentSerializeStrategyAsync<T>(
          (this.rowTableEngine as any).order,
          this,
          this.txContext,
          field
        ),
        comparator as any
      )
      await tree.init()
      this.trees.set(field, tree as any)
    }
    const tree = this.trees.get(field) as unknown as BPTreeAsync<number, DataplyTreeValue<T>>
    return tree
  }

  createDocumentMetadata(): DocumentDataplyMetadata {
    return {
      magicString: 'document-dataply',
      version: 1,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      lastId: 0,
      treeHeads: {}
    }
  }

  async initializeDocumentFile(tx: Transaction): Promise<void> {
    const metadata = await this.select(1, false, tx)
    if (metadata) {
      throw new Error('Document metadata already exists')
    }
    await this.insertAsOverflow(JSON.stringify(this.createDocumentMetadata()), false, tx)
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
    const row = await this.select(1, false, tx)
    if (!row) {
      throw new Error('Document metadata not found')
    }
    return JSON.parse(row)
  }

  async updateDocumentMetadata(metadata: DocumentDataplyMetadata, tx: Transaction): Promise<void> {
    await this.update(1, JSON.stringify(metadata), tx)
  }

  async updateTreeHead(tx: Transaction): Promise<void> {
    const metadata = await this.getDocumentMetadata(tx)
    metadata.treeHeads = this.treeHeads
    await this.updateDocumentMetadata(metadata, tx)
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
      like: 'like'
    }

  constructor(file: string, options?: DocumentDataplyOptions) {
    this.api = new DocumentDataplyAPI(file, options ?? {})
  }

  async init(): Promise<void> {
    await this.api.init()
  }

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
      if (typeof conditions !== 'object') {
        newConditions = { primaryEqual: { v: conditions } as unknown as V }
      }
      else {
        newConditions = {}
        for (const condition in conditions) {
          const before = condition as keyof typeof conditions
          const after = this.operatorConverters[before]
          const v = conditions[before]
          if (!after) continue
          newConditions[after] = { v } as any
        }
      }
      (result as any)[field] = newConditions
    }
    return result
  }

  /**
   * Return optimized tree for query
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
      const tree = await this.api.ensureTree(field)
      if (!tree) continue
      const condition = query[field] as Partial<DocumentDataplyCondition<U>>
      candidates.push({ tree, condition })
    }
    const res = BPTreeAsync.ChooseDriver(candidates)
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
    const metadata = await this.api.getDocumentMetadata(tx)
    const id = metadata.lastId++
    await this.api.updateDocumentMetadata(metadata, tx)
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

  async insert(document: T, tx?: Transaction): Promise<boolean> {
    return this.api.runWithDefault(async (tx) => {
      const { k, document: dataplyDocument } = await this.insertDocument(document, tx)
      const flattenDocument = this.api.flattenDocument(dataplyDocument)
      for (const field in flattenDocument) {
        const v = flattenDocument[field]
        const tree = await this.api.ensureTree(field)
        await tree.insert(k, { k, v })
      }
      return true
    }, tx)
  }

  async select(query: Partial<DocumentDataplyQuery<FinalFlatten<DataplyDocument<T>>>>, limit: number = Infinity, tx?: Transaction): Promise<DataplyDocument<T>[]> {
    return this.api.runWithDefault(async (tx) => {
      const verbose = this.verboseQuery(query)
      const selectivity = await this.getSelectivityCandidate(verbose)

      if (!selectivity) return []

      const keys: Set<number> = new Set()
      const { driver, others } = selectivity
      const stream = driver.tree.whereStream(driver.condition)

      // 2. 드라이버 스트림 시작
      for await (const [pk, val] of stream) {
        let isMatch = true
        // 3. 나머지 조건들 검증
        for (const { tree, condition } of others) {
          const targetValue = await tree.get(pk) // PK로 해당 인덱스의 값을 조회
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

  async close(): Promise<void> {
    await this.api.close()
  }
}
