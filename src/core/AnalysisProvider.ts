import type { FlattenedDocumentJSON, DocumentJSON } from '../types'
import type { DocumentDataplyAPI } from './documentAPI'

/**
 * Abstract base class for analysis providers.
 * Each statistics type should extend this class and implement its methods.
 */
export abstract class AnalysisProvider<T extends DocumentJSON = DocumentJSON> {
  constructor(protected api: DocumentDataplyAPI<T>) { }
  /**
   * Unique name of this analysis type (e.g. 'fts_term_count').
   * Used as the key in the AnalysisHeader.
   */
  abstract readonly name: string

  /**
   * Trigger mode for this provider.
   * - 'realtime': onInsert/onDelete/onUpdate are called on every mutation and persisted immediately
   * - 'interval': mutations are accumulated in memory and persisted only when flush() is called
   */
  abstract readonly triggerMode: 'realtime' | 'interval'

  /**
   * Load existing statistics data from a raw string.
   * Called during initialization to restore state from disk.
   * @param raw The raw string from the overflow row, or null if no data exists yet
   */
  abstract load(raw: string | null): Promise<void>

  /**
   * Serialize the current statistics data to a raw string for storage.
   */
  abstract serialize(): Promise<string>

  /**
   * Called when documents are inserted.
   * @param documents The flattened documents that were inserted
   */
  abstract onInsert(documents: FlattenedDocumentJSON[]): Promise<void>

  /**
   * Called when documents are deleted.
   * @param documents The flattened documents that were deleted
   */
  abstract onDelete(documents: FlattenedDocumentJSON[]): Promise<void>

  /**
   * Called when documents are updated.
   * @param pairs Array of { oldDocument, newDocument } pairs
   */
  abstract onUpdate(pairs: { oldDocument: FlattenedDocumentJSON, newDocument: FlattenedDocumentJSON }[]): Promise<void>
}
