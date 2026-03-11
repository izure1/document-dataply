import type {
  DocumentJSON,
  DataplyDocument,
  FlattenedDocumentJSON,
  DocumentDataplyQuery,
  DataplyTreeValue,
  Primitive
} from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import { Transaction, BPTreeAsyncTransaction } from 'dataply'
import { tokenize } from '../utils/tokenizer'
import { catchPromise } from '../utils/catchPromise'
import { DeadlineChunker } from '../utils/DeadlineChunker'

export class MutationManager<T extends DocumentJSON> {
  constructor(
    private api: DocumentDataplyAPI<T>,
    private logger: any // Fallback until logger is exported from dataply
  ) { }

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
    return this.api.runWithDefaultWrite(async (tx: Transaction) => {
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
            const [error] = await catchPromise(tree.insert(keyToInsert, { k: dpk, v: token }))
            if (error) throw error
          }
        }
        else {
          const indexVal = this.api.indexManager.getIndexValue(config, flattenDocument)
          if (indexVal === undefined) continue
          const [error] = await catchPromise(tree.insert(dpk, { k: dpk, v: indexVal } as any))
          if (error) throw error
        }
      }

      // 통계 provider에 insert 이벤트 전파
      await this.api.analysisManager.notifyInsert([flattenDocument], tx)

      return dataplyDocument._id
    }, tx)
  }

  async insertBatchDocuments(documents: T[], tx?: Transaction): Promise<number[]> {
    return this.api.runWithDefaultWrite(async (tx: Transaction) => {
      // 1. Prepare Metadata and increment IDs in bulk
      const metadata = await this.api.getDocumentInnerMetadata(tx)
      const startId = metadata.lastId + 1
      metadata.lastId += documents.length
      await this.api.updateDocumentInnerMetadata(metadata, tx)

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

        const flattenDocument = this.api.flattenDocument(dataplyDocument)
        flattenedData.push({ pk: -1, data: flattenDocument }) // PK will be filled after insertion

        ids.push(id)
      }

      // 3. Batch Data Insertion
      const pks: number[] = []
      const documentChunker = new DeadlineChunker(10000)
      await documentChunker.processInChunks(dataplyDocuments, async (chunk) => {
        const res = await this.api.insertBatch(chunk, true, tx)
        pks.push(...res)
      })

      // 4. Update PKs for indexing
      for (let i = 0, len = pks.length; i < len; i++) {
        flattenedData[i].pk = pks[i]
      }

      // 5. 등록된 인덱스별로 인덱싱
      for (const [indexName, config] of this.api.indexManager.registeredIndices) {
        const tree = this.api.trees.get(indexName)
        if (!tree) continue

        const treeTx = await tree.createTransaction()
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

        // Chunk batchInsertData to prevent Node.js event loop starvation
        const initMaxSize = 50000
        const initChunkSize = Math.min(
          initMaxSize,
          Math.max(initMaxSize, Math.floor(batchInsertData.length / 100 * 5))
        )
        const chunker = new DeadlineChunker(initChunkSize)
        await chunker.processInChunks(batchInsertData, async (chunk) => {
          const [error] = await catchPromise(treeTx.batchInsert(chunk))
          if (error) {
            throw error
          }
        })

        const res = await treeTx.commit()
        if (!res.success) {
          await treeTx.rollback()
          throw (res as any).error
        }
      }

      // 6. 통계 provider에 insert 이벤트 전파
      const flatDocs: FlattenedDocumentJSON[] = []
      for (let i = 0, len = flattenedData.length; i < len; i++) {
        flatDocs.push(flattenedData[i].data)
      }
      await this.api.analysisManager.notifyInsert(flatDocs, tx)

      return ids
    }, tx)
  }

  private async updateInternal(
    query: Partial<DocumentDataplyQuery<T>>,
    computeUpdatedDoc: (doc: DataplyDocument<T>) => DataplyDocument<T>,
    tx: Transaction
  ): Promise<number> {
    const pks = await this.api.queryManager.getKeys(query)
    let updatedCount = 0
    const updatePairs: { oldDocument: FlattenedDocumentJSON, newDocument: FlattenedDocumentJSON }[] = []

    const treeTxs = new Map<string, BPTreeAsyncTransaction<string | number, DataplyTreeValue<any>>>()
    for (const [indexName, tree] of this.api.trees) {
      treeTxs.set(indexName, await tree.createTransaction())
    }
    treeTxs.delete('_id')

    for (let i = 0, len = pks.length; i < len; i++) {
      const pk = pks[i]
      const doc = await this.api.getDocument(pk, tx)
      if (!doc) continue

      const updatedDoc = computeUpdatedDoc(doc)
      const oldFlatDoc = this.api.flattenDocument(doc)
      const newFlatDoc = this.api.flattenDocument(updatedDoc)

      // 변경된 인덱스 필드 동기화
      for (const [indexName, treeTx] of treeTxs) {
        const config = this.api.indexManager.registeredIndices.get(indexName)
        if (!config) continue

        if (config.type === 'fts') {
          const primaryField = this.api.indexManager.getPrimaryField(config)
          const oldV = oldFlatDoc[primaryField]
          const newV = newFlatDoc[primaryField]
          if (oldV === newV) continue
          const ftsConfig = this.api.indexManager.getFtsConfig(config)

          // 기존 FTS 토큰 삭제
          if (typeof oldV === 'string') {
            const oldTokens = ftsConfig ? tokenize(oldV, ftsConfig) : [oldV]
            for (let j = 0, jLen = oldTokens.length; j < jLen; j++) {
              await treeTx.delete(this.api.indexManager.getTokenKey(pk, oldTokens[j] as string), { k: pk, v: oldTokens[j] })
            }
          }
          // 새 FTS 토큰 삽입
          if (typeof newV === 'string') {
            const newTokens = ftsConfig ? tokenize(newV, ftsConfig) : [newV]
            const batchInsertData: [string | number, DataplyTreeValue<Primitive>][] = []
            for (let j = 0, jLen = newTokens.length; j < jLen; j++) {
              batchInsertData.push([this.api.indexManager.getTokenKey(pk, newTokens[j] as string), { k: pk, v: newTokens[j] }])
            }
            await treeTx.batchInsert(batchInsertData)
          }
        }
        else {
          const oldIndexVal = this.api.indexManager.getIndexValue(config, oldFlatDoc)
          const newIndexVal = this.api.indexManager.getIndexValue(config, newFlatDoc)

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

      // update pair 축적
      updatePairs.push({ oldDocument: oldFlatDoc, newDocument: newFlatDoc })

      // 실제 레코드 업데이트
      await this.api.update(pk, JSON.stringify(updatedDoc), tx)
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

    // 통계 provider에 update 이벤트 일괄 전파
    await this.api.analysisManager.notifyUpdate(updatePairs, tx)

    return updatedCount
  }

  async fullUpdate(
    query: Partial<DocumentDataplyQuery<T>>,
    newRecord: T | ((document: DataplyDocument<T>) => T),
    tx?: Transaction
  ): Promise<number> {
    return this.api.runWithDefaultWrite(async (tx: Transaction) => {
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
    return this.api.runWithDefaultWrite(async (tx: Transaction) => {
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
    return this.api.runWithDefaultWrite(async (tx: Transaction) => {
      const pks = await this.api.queryManager.getKeys(query)
      let deletedCount = 0
      const deletedFlatDocs: FlattenedDocumentJSON[] = []

      for (let i = 0, len = pks.length; i < len; i++) {
        const pk = pks[i]
        const doc = await this.api.getDocument(pk, tx)
        if (!doc) continue

        const flatDoc = this.api.flattenDocument(doc)

        // 모든 인덱스 트리에서 삭제
        for (const [indexName, tree] of this.api.trees) {
          const config = this.api.indexManager.registeredIndices.get(indexName)
          if (!config) continue

          if (config.type === 'fts') {
            const primaryField = this.api.indexManager.getPrimaryField(config)
            const v = flatDoc[primaryField]
            if (v === undefined || typeof v !== 'string') continue
            const ftsConfig = this.api.indexManager.getFtsConfig(config)
            const tokens = ftsConfig ? tokenize(v, ftsConfig) : [v]
            for (let j = 0, jLen = tokens.length; j < jLen; j++) {
              await tree.delete(this.api.indexManager.getTokenKey(pk, tokens[j] as string), { k: pk, v: tokens[j] })
            }
          } else {
            const indexVal = this.api.indexManager.getIndexValue(config, flatDoc)
            if (indexVal === undefined) continue
            await tree.delete(pk, { k: pk, v: indexVal } as any)
          }
        }

        // 삭제된 문서 축적
        deletedFlatDocs.push(flatDoc)

        // 실제 레코드 삭제
        await this.api.delete(pk, true, tx)
        deletedCount++
      }

      // 통계 provider에 delete 이벤트 일괄 전파
      await this.api.analysisManager.notifyDelete(deletedFlatDocs, tx)

      return deletedCount
    }, tx)
  }
}
