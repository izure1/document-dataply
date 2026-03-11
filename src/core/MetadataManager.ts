import type {
  DocumentDataplyMetadata,
  DocumentDataplyInnerMetadata,
  DocumentJSON
} from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import { Transaction } from 'dataply'

export class MetadataManager<T extends DocumentJSON> {
  constructor(
    private api: DocumentDataplyAPI<T>,
    private logger: any
  ) { }

  async getDocumentMetadata(tx: Transaction): Promise<DocumentDataplyMetadata> {
    const metadata = await this.api.getMetadata(tx)
    const innerMetadata = await this.getDocumentInnerMetadata(tx)
    const indices: string[] = []
    for (const name of this.api.indexManager.registeredIndices.keys()) {
      if (name !== '_id') {
        indices.push(name)
      }
    }
    return {
      pageSize: metadata.pageSize,
      pageCount: metadata.pageCount,
      rowCount: metadata.rowCount,
      usage: metadata.usage,
      indices,
      schemeVersion: innerMetadata.schemeVersion ?? 0,
    }
  }

  async getDocumentInnerMetadata(tx: Transaction): Promise<DocumentDataplyInnerMetadata> {
    const row = await this.api.select(1, false, tx)
    if (!row) {
      throw new Error('Document metadata not found')
    }
    return JSON.parse(row)
  }

  async updateDocumentInnerMetadata(metadata: DocumentDataplyInnerMetadata, tx: Transaction): Promise<void> {
    this.logger.debug(`Updating document inner metadata: version ${metadata.version}`)
    await this.api.update(1, JSON.stringify(metadata), tx)
  }

  async migration(
    version: number,
    callback: (tx: Transaction) => Promise<void>,
    tx?: Transaction
  ): Promise<void> {
    await this.api.runWithDefaultWrite(async (tx: Transaction) => {
      const innerMetadata = await this.getDocumentInnerMetadata(tx)
      const currentVersion = innerMetadata.schemeVersion ?? 0
      if (currentVersion < version) {
        await callback(tx)
        // 콜백 내부에서 createIndex/dropIndex가 메타데이터를 변경했을 수 있으므로
        // 최신 메타데이터를 다시 읽어서 schemeVersion만 업데이트
        const freshMetadata = await this.getDocumentInnerMetadata(tx)
        this.logger.info(`Migration applied successfully to schemeVersion ${version}`)
        freshMetadata.schemeVersion = version
        freshMetadata.updatedAt = Date.now()
        await this.updateDocumentInnerMetadata(freshMetadata, tx)
      }
    }, tx)
  }
}
