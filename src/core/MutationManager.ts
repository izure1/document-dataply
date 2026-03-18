import type {
  DocumentJSON,
  DataplyDocument,
  FlattenedDocumentJSON,
  DocumentDataplyQuery,
  DataplyTreeValue,
  Primitive
} from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import { Transaction, Logger, BPTreePureAsync } from 'dataply'
import { tokenize } from '../utils/tokenizer'
import { yieldEventLoop } from '../utils/eventLoopManager'

export class MutationManager<T extends DocumentJSON> {
  constructor(
    private api: DocumentDataplyAPI<T>,
    private logger: Logger
  ) { }

  private async isTreeEmpty(tree: BPTreePureAsync<string | number, DataplyTreeValue<Primitive>>): Promise<boolean> {
    try {
      const root = await tree.getRootNode()
      return root.leaf && root.values.length === 0
    } catch {
      return true
    }
  }

  private async insertDocumentInternal(document: T, tx: Transaction): Promise<{
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

  async insertSingleDocument(document: T, tx?: Transaction): Promise<number> {
    return this.api.withWriteTransaction(async (tx: Transaction) => {
      const { pk: dpk, document: dataplyDocument } = await this.insertDocumentInternal(document, tx)
      const flattenDocument = this.api.flattenDocument(dataplyDocument)

      // 등록된 인덱스별로 인덱싱
      for (const [indexName, config] of this.api.indexManager.registeredIndices) {
        const tree = this.api.trees.get(indexName)
        if (!tree) continue

        if (config.type === 'fts') {
          const primaryField = this.api.indexManager.getPrimaryField(config)
          const v = flattenDocument[primaryField]
          if (v === undefined || typeof v !== 'string') continue
          const ftsConfig = this.api.indexManager.getFtsConfig(config)
          const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
          for (let i = 0, len = tokens.length; i < len; i++) {
            const token = tokens[i]
            const keyToInsert = this.api.indexManager.getTokenKey(dpk, token as string)
            await tree.insert(keyToInsert, { k: dpk, v: token })
          }
        }
        else {
          const indexVal = this.api.indexManager.getIndexValue(config, flattenDocument)
          if (indexVal === undefined) continue
          await tree.insert(dpk, { k: dpk, v: indexVal } as any)
        }
      }

      // 통계 provider에 insert 이벤트 전파
      await this.api.analysisManager.notifyInsert([flattenDocument], tx)

      this.logger.debug(`Inserted single document with ID: ${dataplyDocument._id}`)

      return dataplyDocument._id
    }, tx)
  }

  async insertBatchDocuments(documents: T[], tx?: Transaction): Promise<number[]> {
    return this.api.withWriteTransaction(async (tx: Transaction) => {
      this.logger.debug(`Batch inserting ${documents.length} documents`)

      // 1. Prepare Metadata and increment IDs in bulk
      const metadata = await this.api.getDocumentInnerMetadata(tx)
      let startId = metadata.lastId + 1
      metadata.lastId += documents.length
      await this.api.updateDocumentInnerMetadata(metadata, tx)

      const ids: Float64Array = new Float64Array(documents.length)
      const pks: Float64Array = new Float64Array(documents.length)
      const dataplyDocuments: string[] = []
      const flattenedData: { pk: number, data: FlattenedDocumentJSON }[] = []

      // 2. 데이터 준비 단계
      for (let i = 0, len = documents.length; i < len; i++) {
        const id = startId + i
        const dataplyDocument: DataplyDocument<T> = Object.assign({
          _id: id,
        }, documents[i])

        const stringified = JSON.stringify(dataplyDocument)
        dataplyDocuments.push(stringified)

        const flattenDocument = this.api.flattenDocument(dataplyDocument)
        flattenedData.push({ pk: -1, data: flattenDocument }) // PK will be filled after insertion

        ids[i] = id
      }

      // 3. 실제 문서 행 삽입
      const res = await this.api.insertBatch(dataplyDocuments, true, tx)
      for (let i = 0, len = res.length; i < len; i++) {
        const index = i
        pks[index] = res[i]
        flattenedData[index].pk = res[i]
      }
      await yieldEventLoop()

      // 4. 등록된 인덱스별로 인덱싱
      for (const [indexName, config] of this.api.indexManager.registeredIndices) {
        const tree = this.api.trees.get(indexName)
        if (!tree) continue

        const batchInsertData: [string | number, DataplyTreeValue<Primitive>][] = []

        if (config.type === 'fts') {
          const primaryField = this.api.indexManager.getPrimaryField(config)
          const ftsConfig = this.api.indexManager.getFtsConfig(config)
          for (let i = 0, len = flattenedData.length; i < len; i++) {
            const item = flattenedData[i]
            const v = item.data[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, tLen = tokens.length; j < tLen; j++) {
              const token = tokens[j]
              batchInsertData.push([this.api.indexManager.getTokenKey(item.pk, token as string), { k: item.pk, v: token }])
            }
          }
        }
        else {
          for (let i = 0, len = flattenedData.length; i < len; i++) {
            const item = flattenedData[i]
            const indexVal = this.api.indexManager.getIndexValue(config, item.data)
            if (indexVal === undefined) continue
            batchInsertData.push([item.pk, { k: item.pk, v: indexVal } as any])
          }
        }

        // 5. 인덱스에 데이터 삽입
        const isEmptyTree = await this.isTreeEmpty(tree)
        if (isEmptyTree) {
          this.logger.info(`Bulk loading ${batchInsertData.length} items into index ${indexName}`)
          await tree.bulkLoad(batchInsertData)
        }
        else {
          this.logger.info(`Batch inserting ${batchInsertData.length} items into index ${indexName}`)
          await tree.batchInsert(batchInsertData)
        }

        await yieldEventLoop()
      }

      // 6. 통계 provider에 insert 이벤트 전파
      const flatDocs: FlattenedDocumentJSON[] = []
      for (let i = 0, len = flattenedData.length; i < len; i++) {
        flatDocs.push(flattenedData[i].data)
      }
      await this.api.analysisManager.notifyInsert(flatDocs, tx)
      await yieldEventLoop()

      this.logger.debug(`Successfully batch inserted ${documents.length} documents`)

      return Array.from(ids)
    }, tx)
  }

  private async updateInternal(
    query: Partial<DocumentDataplyQuery<T>>,
    computeUpdatedDoc: (doc: DataplyDocument<T>) => DataplyDocument<T>,
    tx: Transaction
  ): Promise<number> {
    const pks = await this.api.queryManager.getKeys(query)
    this.logger.debug(`Found ${pks.length} documents to update`)
    let updatedCount = 0
    const updatePairs: { oldDocument: FlattenedDocumentJSON, newDocument: FlattenedDocumentJSON }[] = []

    // 인덱스별 삭제/삽입 항목 수집용 맵
    const deleteMap: Map<string, [string | number, any][]> = new Map()
    const insertMap: Map<string, [string | number, DataplyTreeValue<Primitive>][]> = new Map()
    for (const indexName of this.api.trees.keys()) {
      deleteMap.set(indexName, [])
      insertMap.set(indexName, [])
    }

    for (let i = 0, len = pks.length; i < len; i++) {
      const pk = pks[i]
      const doc = await this.api.getDocument(pk, tx)
      if (!doc) continue

      const updatedDoc = computeUpdatedDoc(doc)
      const oldFlatDoc = this.api.flattenDocument(doc)
      const newFlatDoc = this.api.flattenDocument(updatedDoc)

      // 변경된 인덱스 필드 동기화 항목 수집
      for (const [indexName, tree] of this.api.trees) {
        const config = this.api.indexManager.registeredIndices.get(indexName)
        if (!config) continue
        const delEntries = deleteMap.get(indexName)!
        const insEntries = insertMap.get(indexName)!
        if (config.type === 'fts') {
          const primaryField = this.api.indexManager.getPrimaryField(config)
          const oldV = oldFlatDoc[primaryField]
          const newV = newFlatDoc[primaryField]
          if (oldV === newV) continue
          const ftsConfig = this.api.indexManager.getFtsConfig(config)
          // 기존 FTS 토큰 삭제 항목 수집
          if (typeof oldV === 'string') {
            const oldTokens = ftsConfig ? tokenize(oldV, ftsConfig) : [oldV]
            for (let j = 0, jLen = oldTokens.length; j < jLen; j++) {
              delEntries.push([this.api.indexManager.getTokenKey(pk, oldTokens[j] as string), { k: pk, v: oldTokens[j] }])
            }
          }
          // 새 FTS 토큰 삽입 항목 수집
          if (typeof newV === 'string') {
            const newTokens = ftsConfig ? tokenize(newV, ftsConfig) : [newV]
            for (let j = 0, jLen = newTokens.length; j < jLen; j++) {
              insEntries.push([this.api.indexManager.getTokenKey(pk, newTokens[j] as string), { k: pk, v: newTokens[j] }])
            }
          }
        }
        else {
          const oldIndexVal = this.api.indexManager.getIndexValue(config, oldFlatDoc)
          const newIndexVal = this.api.indexManager.getIndexValue(config, newFlatDoc)

          // 값이 동일하면 스킵 (배열 비교를 위해 JSON.stringify 사용)
          if (JSON.stringify(oldIndexVal) === JSON.stringify(newIndexVal)) continue

          // 기존 값 삭제 항목 수집
          if (oldIndexVal !== undefined) {
            delEntries.push([pk, { k: pk, v: oldIndexVal }])
          }
          // 새 값 삽입 항목 수집
          if (newIndexVal !== undefined) {
            insEntries.push([pk, { k: pk, v: newIndexVal } as any])
          }
        }
      }

      // update pair 축적
      updatePairs.push({ oldDocument: oldFlatDoc, newDocument: newFlatDoc })

      // 실제 레코드 업데이트
      await this.api.update(pk, JSON.stringify(updatedDoc), tx)
      await yieldEventLoop()
      updatedCount++
    }

    // 인덱스별 batchDelete → batchInsert 호출
    for (const [indexName, tree] of this.api.trees) {
      const delEntries = deleteMap.get(indexName)!
      if (delEntries.length > 0) {
        await tree.batchDelete(delEntries)
      }
      const insEntries = insertMap.get(indexName)!
      if (insEntries.length > 0) {
        await tree.batchInsert(insEntries)
      }
      await yieldEventLoop()
    }

    // 통계 provider에 update 이벤트 일괄 전파
    await this.api.analysisManager.notifyUpdate(updatePairs, tx)
    await yieldEventLoop()

    this.logger.debug(`Successfully updated ${updatedCount} documents`)

    return updatedCount
  }

  async fullUpdate(
    query: Partial<DocumentDataplyQuery<T>>,
    newRecord: T | ((document: DataplyDocument<T>) => T),
    tx?: Transaction
  ): Promise<number> {
    return this.api.withWriteTransaction(async (tx: Transaction) => {
      return this.updateInternal(query, (doc) => {
        const newDoc = typeof newRecord === 'function'
          ? (newRecord as Function)(doc)
          : newRecord
        // _id 보존
        return { _id: doc._id, ...newDoc } as DataplyDocument<T>
      }, tx)
    }, tx)
  }

  async partialUpdate(
    query: Partial<DocumentDataplyQuery<T>>,
    newRecord: Partial<DataplyDocument<T>> | ((document: DataplyDocument<T>) => Partial<DataplyDocument<T>>),
    tx?: Transaction
  ): Promise<number> {
    return this.api.withWriteTransaction(async (tx: Transaction) => {
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

  async deleteDocuments(
    query: Partial<DocumentDataplyQuery<T>>,
    tx?: Transaction
  ): Promise<number> {
    return this.api.withWriteTransaction(async (tx: Transaction) => {
      const pks = await this.api.queryManager.getKeys(query)
      this.logger.debug(`Found ${pks.length} documents to delete`)
      const deletedFlatDocs: FlattenedDocumentJSON[] = []
      const deletedPks: number[] = []

      // 인덱스별 삭제 항목 수집용 맵
      const batchDeleteMap: Map<string, [string | number, any][]> = new Map()
      for (const indexName of this.api.trees.keys()) {
        batchDeleteMap.set(indexName, [])
      }

      // 1단계: 모든 삭제 대상 문서 조회 및 인덱스 삭제 항목 수집
      for (let i = 0, len = pks.length; i < len; i++) {
        const pk = pks[i]
        const doc = await this.api.getDocument(pk, tx)
        if (!doc) continue

        const flatDoc = this.api.flattenDocument(doc)

        for (const [indexName, tree] of this.api.trees) {
          const config = this.api.indexManager.registeredIndices.get(indexName)
          if (!config) continue
          const entries = batchDeleteMap.get(indexName)!
          if (config.type === 'fts') {
            const primaryField = this.api.indexManager.getPrimaryField(config)
            const v = flatDoc[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const ftsConfig = this.api.indexManager.getFtsConfig(config)
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, jLen = tokens.length; j < jLen; j++) {
              entries.push([this.api.indexManager.getTokenKey(pk, tokens[j] as string), { k: pk, v: tokens[j] }])
            }
          }
          else {
            const indexVal = this.api.indexManager.getIndexValue(config, flatDoc)
            if (indexVal === undefined) continue
            entries.push([pk, { k: pk, v: indexVal }])
          }
        }

        deletedFlatDocs.push(flatDoc)
        deletedPks.push(pk)
        await yieldEventLoop()
      }

      // 2단계: 인덱스별 batchDelete 호출
      for (const [indexName, tree] of this.api.trees) {
        const entries = batchDeleteMap.get(indexName)!
        if (entries.length === 0) continue
        await tree.batchDelete(entries)
        await yieldEventLoop()
      }

      // 3단계: 행 일괄 삭제
      if (deletedPks.length > 0) {
        await this.api.deleteBatch(deletedPks, true, tx)
        await yieldEventLoop()
      }

      // 통계 provider에 delete 이벤트 일괄 전파
      await this.api.analysisManager.notifyDelete(deletedFlatDocs, tx)
      await yieldEventLoop()

      this.logger.debug(`Successfully deleted ${deletedPks.length} documents`)

      return deletedPks.length
    }, tx)
  }
}
