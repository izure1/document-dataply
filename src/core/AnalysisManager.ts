import type {
  AnalysisHeader,
  DocumentJSON,
  FlattenedDocumentJSON
} from '../types'
import type { DocumentDataplyAPI } from './documentAPI'
import type { AnalysisProvider } from './AnalysisProvider'
import { Transaction } from 'dataply'

export class AnalysisManager<T extends DocumentJSON> {
  private providers: Map<string, AnalysisProvider<T>> = new Map()

  constructor(private api: DocumentDataplyAPI<T>) { }

  /**
   * Register an analysis provider.
   * @param provider The provider instance to register
   */
  registerProvider(provider: AnalysisProvider<T>): void {
    if (this.providers.has(provider.name)) {
      throw new Error(`Analysis provider "${provider.name}" is already registered.`)
    }
    this.providers.set(provider.name, provider)
  }

  /**
   * Initialize all registered providers by loading existing data from disk.
   * Should be called after database initialization.
   * @param tx The transaction to use
   */
  async initializeProviders(tx: Transaction): Promise<void> {
    for (const [name, provider] of this.providers) {
      const raw = await this.getAnalysisData(name, tx)
      await provider.load(raw)
    }
  }

  /**
   * Notify all registered providers that documents were inserted.
   * For realtime providers, data is persisted immediately.
   * @param documents The flattened documents that were inserted
   * @param tx The transaction to use
   */
  async notifyInsert(documents: FlattenedDocumentJSON[], tx: Transaction): Promise<void> {
    if (documents.length === 0) return
    for (const [name, provider] of this.providers) {
      await provider.onInsert(documents)
      if (provider.triggerMode === 'realtime') {
        await this.setAnalysisData(name, await provider.serialize(), tx)
      }
    }
  }

  /**
   * Notify all registered providers that documents were deleted.
   * For realtime providers, data is persisted immediately.
   * @param documents The flattened documents that were deleted
   * @param tx The transaction to use
   */
  async notifyDelete(documents: FlattenedDocumentJSON[], tx: Transaction): Promise<void> {
    if (documents.length === 0) return
    for (const [name, provider] of this.providers) {
      await provider.onDelete(documents)
      if (provider.triggerMode === 'realtime') {
        await this.setAnalysisData(name, await provider.serialize(), tx)
      }
    }
  }

  /**
   * Notify all registered providers that documents were updated.
   * For realtime providers, data is persisted immediately.
   * @param pairs Array of { oldDocument, newDocument } pairs
   * @param tx The transaction to use
   */
  async notifyUpdate(pairs: { oldDocument: FlattenedDocumentJSON, newDocument: FlattenedDocumentJSON }[], tx: Transaction): Promise<void> {
    if (pairs.length === 0) return
    for (const [name, provider] of this.providers) {
      await provider.onUpdate(pairs)
      if (provider.triggerMode === 'realtime') {
        await this.setAnalysisData(name, await provider.serialize(), tx)
      }
    }
  }

  /**
   * Flush all interval providers' data to disk.
   * @param tx The transaction to use (must be a write transaction)
   */
  async flush(tx: Transaction): Promise<void> {
    for (const [name, provider] of this.providers) {
      if (provider.triggerMode === 'interval') {
        await this.setAnalysisData(name, await provider.serialize(), tx)
      }
    }
  }

  /**
   * Get the analysis header row.
   * Returns null if no analysis header exists yet.
   * @param tx The transaction to use
   */
  async getAnalysisHeader(tx: Transaction): Promise<AnalysisHeader | null> {
    const metadata = await this.api.getDocumentInnerMetadata(tx)
    if (metadata.analysis == null) {
      return null
    }
    const row = await this.api.select(metadata.analysis, false, tx)
    if (!row) {
      return null
    }
    return JSON.parse(row) as AnalysisHeader
  }

  /**
   * Get the analysis header row, creating it if it doesn't exist.
   * @param tx The transaction to use (must be a write transaction)
   */
  async getOrCreateAnalysisHeader(tx: Transaction): Promise<AnalysisHeader> {
    const metadata = await this.api.getDocumentInnerMetadata(tx)
    if (metadata.analysis != null) {
      const row = await this.api.select(metadata.analysis, false, tx)
      if (row) {
        return JSON.parse(row) as AnalysisHeader
      }
    }
    // Create a new empty analysis header row
    const header: AnalysisHeader = {}
    const pk = await this.api.insertAsOverflow(JSON.stringify(header), false, tx)
    metadata.analysis = pk
    await this.api.updateDocumentInnerMetadata(metadata, tx)
    return header
  }

  /**
   * Get analysis data for a specific type as a raw string.
   * Returns null if the type doesn't exist in the analysis header.
   * @param type The analysis type name
   * @param tx The transaction to use
   */
  async getAnalysisData(type: string, tx: Transaction): Promise<string | null> {
    const header = await this.getAnalysisHeader(tx)
    if (!header || header[type] == null) {
      return null
    }
    const row = await this.api.select(header[type], false, tx)
    if (!row) {
      return null
    }
    return row
  }

  /**
   * Set analysis data for a specific type.
   * Creates a new overflow row if the type doesn't exist yet,
   * or updates the existing row if it does.
   * @param type The analysis type name
   * @param data The raw string data to store
   * @param tx The transaction to use (must be a write transaction)
   */
  async setAnalysisData(type: string, data: string, tx: Transaction): Promise<void> {
    const header = await this.getOrCreateAnalysisHeader(tx)
    const metadata = await this.api.getDocumentInnerMetadata(tx)

    if (header[type] != null) {
      // Update existing data row
      await this.api.update(header[type], data, tx)
    } else {
      // Create new data row
      const pk = await this.api.insertAsOverflow(data, false, tx)
      header[type] = pk
      // Update the analysis header row
      await this.api.update(metadata.analysis!, JSON.stringify(header), tx)
    }
  }

  /**
   * Delete analysis data for a specific type.
   * Removes the type entry from the analysis header.
   * @param type The analysis type name
   * @param tx The transaction to use (must be a write transaction)
   */
  async deleteAnalysisData(type: string, tx: Transaction): Promise<boolean> {
    const metadata = await this.api.getDocumentInnerMetadata(tx)
    if (metadata.analysis == null) {
      return false
    }
    const header = await this.getAnalysisHeader(tx)
    if (!header || header[type] == null) {
      return false
    }
    // Delete the data row
    await this.api.delete(header[type], false, tx)
    delete header[type]
    // Update the analysis header row
    await this.api.update(metadata.analysis, JSON.stringify(header), tx)
    return true
  }

  /**
   * Check if analysis data exists for a specific type.
   * @param type The analysis type name
   * @param tx The transaction to use
   */
  async hasAnalysisData(type: string, tx: Transaction): Promise<boolean> {
    const header = await this.getAnalysisHeader(tx)
    return header != null && header[type] != null
  }

  /**
   * Get all registered analysis type names.
   * @param tx The transaction to use
   */
  async getAnalysisTypes(tx: Transaction): Promise<string[]> {
    const header = await this.getAnalysisHeader(tx)
    if (!header) {
      return []
    }
    return Object.keys(header)
  }
}
